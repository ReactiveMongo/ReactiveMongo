package org.asyncmongo.actors

import akka.actor.{Actor, ActorRef}
import akka.actor.ActorSystem
import akka.actor.Props
import org.asyncmongo.protocol._
import akka.actor.ActorContext
import akka.routing.RoundRobinRouter
import java.net.InetSocketAddress
import akka.routing.Broadcast

import java.nio.ByteOrder
import org.jboss.netty.bootstrap._
import org.jboss.netty.channel._
import org.jboss.netty.buffer._
import org.jboss.netty.channel.socket.nio._

class ChannelActor(val channel: Channel) extends Actor {
  def receive = {
    case (message :WritableMessage[WritableOp], writeConcern :WritableMessage[WritableOp]) => {
      log("will send WritableMessage " + message + " followed by writeConcern " + writeConcern)
      channel.write(message)
      channel.write(writeConcern)
    }
    case message :WritableMessage[WritableOp] => {
      log("will send WritableMessage " + message)
      log(channel.getLocalAddress.asInstanceOf[InetSocketAddress].toString)
      channel.write(message)
    }
    case "toto" => log("toto")
    case s:String => log("received string=" + s)
    case t => log("something else " + t)
  }
  def log(s: String) = println("ChannelActor [" + self.path + "] : " + s)
}

private object NodeRouterMaker {
  def apply(creator: ActorRef, context: ActorContext, name: String, n: Int = 3, host: String = "localhost", port: Int = 27017) :ActorRef = {
    context.actorOf(Props.empty.withRouter(RoundRobinRouter(routees = 
      for(i <- 0 to n) yield {
        println("creating actor on " + host + ":" + port + " with parent " + context.parent + ", dispatcher is " + context.dispatcher)
        val channel = ChannelFactory.create(host, port, creator)
        context.actorOf(Props(new ChannelActor(channel)), name = channel.getId.toString)
      }
    )), name)
  }
}

object ChannelFactory {
  import java.net.InetSocketAddress
  import java.util.concurrent.Executors

  val factory = new NioClientSocketChannelFactory(
    Executors.newCachedThreadPool,
    Executors.newCachedThreadPool
  )

  def create(host: String = "localhost", port: Int = 27017, receiver: ActorRef) = {
    val bootstrap = new ClientBootstrap(factory)

    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      override def getPipeline :ChannelPipeline = {
        Channels.pipeline(new WritableMessageEncoder(), new ReplyFrameDecoder(), new ReplyDecoder(), new MongoHandler(receiver))
      }
    })

    bootstrap.setOption("tcpNoDelay", true)
    bootstrap.setOption("bufferFactory", new HeapChannelBufferFactory(java.nio.ByteOrder.LITTLE_ENDIAN))
    bootstrap.connect(new InetSocketAddress(host, port)).await.getChannel
  }
}

private[asyncmongo] class NodeResizer(init: Option[IndexedSeq[ActorRef]] = None) extends akka.routing.Resizer {
  private var toAdd :IndexedSeq[ActorRef] = init.getOrElse(Vector())
  private var toRemove :IndexedSeq[ActorRef] = Vector()

  def isTimeForResize(messageCounter: Long) :Boolean = {
    println("resizer = " + messageCounter)
    messageCounter == 0 || !toRemove.isEmpty || !toAdd.isEmpty
  }

  def resize(props: Props, routeeProvider: akka.routing.RouteeProvider) {
    if(!toRemove.isEmpty)
      routeeProvider.unregisterRoutees(toRemove)
    if(!toAdd.isEmpty)
      routeeProvider.registerRoutees(toAdd)
    toRemove = Vector()
    toAdd = Vector()
    println("called for resize, now actors = " + routeeProvider.routees)
  }

  def addActor(a :ActorRef) {
    toAdd = toAdd :+ a
  }

  def removeActor(a :ActorRef) {
    toRemove = toRemove :+ a
    println("remove " + a)
  }
}

class MongoDBSystem(val nodes: List[(String, Int)]) extends Actor {
  val secondaries = context.actorOf(
    Props.empty.withRouter(
      RoundRobinRouter(resizer = Some(
        new NodeResizer(Some(
          (for(node <- nodes)
            yield NodeRouterMaker(self, context, "yop", 0, node._1, node._2)
          ).toIndexedSeq
        ))
      ))
    ), "toto"
  )

  println("secondaries router is " + secondaries)

  private val awaitingResponses = scala.collection.mutable.ListMap[Int, ActorRef]()

  override def receive = {
    case message: WritableMessage[WritableOp] => {
      if(message.op.expectsResponse) {
        awaitingResponses += ((message.requestID, sender))
        log("registering awaiting response for requestID " + message.requestID + ", awaitingResponses: " + awaitingResponses)
      } else log("NOT registering awaiting response for requestID " + message.requestID)
      resolveReceiver(message) forward message
    }
    case (message: WritableMessage[WritableOp], writeConcern: WritableMessage[WritableOp]) => {
      awaitingResponses += ((message.requestID, sender))
      log("registering writeConcern-awaiting response for requestID " + message.requestID + ", awaitingResponses: " + awaitingResponses)
      resolveReceiver(message) forward (message, writeConcern)
    }
    case message: ReadReply => {
      awaitingResponses.get(message.header.responseTo) match {
        case Some(_sender) => {
          // akka://mongodb/user/$a/631320101
          log("Got a response from " + sender + "! Will give back message="+message + " to sender " + _sender)
          val sel = "akka://mongodb/*/" + message.info.channelID
          log("will send back a message to the connection that gave us this reply (actorpath = " + sel + ")")
          context.actorFor(message.info.channelID.toString) ! "hey!!!!"
          awaitingResponses -= message.header.responseTo
          _sender ! message
        }
        case None => log("oups. " + message.header.responseTo + " not found! complete message is " + message)
      }
    }
    case _ => log("not supported")
  }
  def log(s: String) = println("MongoDBSystem [" + self.path + "] : " + s)

  def resolveReceiver(message: WritableMessage[WritableOp]) :ActorRef = message.connectionId.map{ id => context.actorFor(id.toString) }.getOrElse(secondaries)
}

class MongoConnection(
  mongosystem: ActorRef
) {
  import org.asyncmongo.protocol.messages._
  import akka.util.Timeout
  import akka.dispatch.Future

  /** write an op and wait for db response */
  def ask(message: WritableMessage[WritableOp])(implicit timeout: Timeout) :Future[ReadReply] = {
    import akka.pattern.ask
    (mongosystem ? message).mapTo[ReadReply]
  }

  /** write a no-response op followed by a GetLastError command and wait for its response */
  def ask(message: WritableMessage[WritableOp], writeConcern: GetLastError = GetLastError())(implicit timeout: Timeout) = {
    import akka.pattern.ask
    (mongosystem ? ((message, writeConcern.makeWritableMessage("plugin", message.header.requestID)))).mapTo[ReadReply]
  }

  /** write a no-response op without getting a future */
  def send(message: WritableMessage[WritableOp]) = mongosystem ! message

  def stop = MongoConnection.system.stop(mongosystem)
}

object MongoConnection {
  import com.typesafe.config.ConfigFactory
  val config = ConfigFactory.load()

  val system = ActorSystem("mongodb", config.getConfig("mongo-async-driver"))

  def apply(nodes: List[(String, Int)], name: Option[String]= None) = {
    val props = Props(new MongoDBSystem(nodes))
    new MongoConnection(if(name.isDefined) system.actorOf(props, name = name.get) else system.actorOf(props))
  }
}