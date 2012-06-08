package org.asyncmongo.actors

import akka.actor._
import akka.dispatch.Future
import akka.pattern.ask
import akka.routing.{Broadcast, RoundRobinRouter}
import akka.util.Timeout

import org.asyncmongo.bson._
import org.asyncmongo.handlers.DefaultBSONHandlers._
import org.asyncmongo.protocol._
import org.asyncmongo.protocol.messages._

import java.net.InetSocketAddress
import java.nio.ByteOrder

import org.jboss.netty.bootstrap._
import org.jboss.netty.buffer._
import org.jboss.netty.channel._
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

  override def postStop = {
    log("stopping !")
    channel.close
  }

  def log(s: String) = println("ChannelActor [" + self.path + "] : " + s)
}

private object NodeRouterMaker {
  def apply(creator: ActorRef, context: ActorContext, host: String = "localhost", port: Int = 27017, n: Int = 3) :ActorRef = {
    context.actorOf(Props.empty.withRouter(RoundRobinRouter(routees = 
      for(i <- 0 to n) yield {
        println("creating actor on " + host + ":" + port + " with parent " + context.parent + ", dispatcher is " + context.dispatcher)
        val channel = ChannelFactory.create(host, port, creator)
        context.actorOf(Props(new ChannelActor(channel)), name = channel.getId.toString)
      }
    )), makeName(host, port))
  }
  def makeName(host: String, port: Int) :String = {
    host + "-" + port
  }
}

private case class Node(
  host: String,
  port: Int,
  actorRef: ActorRef
) {
  val name :String = NodeRouterMaker.makeName(host, port)
}

private[asyncmongo] class NodeManager(creator: ActorRef, context: ActorContext, numberOfConnections: Int, init: Option[List[(String, Int)]]) {
  private var nodes = List[Node]()

  private var index = -1

  def pick :ActorRef = {
    index = if(index >= nodes.length - 1) 0 else index + 1
    nodes(index).actorRef
  }

  def all :List[ActorRef] = nodes.map(_.actorRef)

  def addNode(host: String, port: Int) :ActorRef = {
    nodes.find(_.name == NodeRouterMaker.makeName(host, port)).getOrElse({
      val node = Node(host, port, {
        context.actorOf(Props.empty.withRouter(RoundRobinRouter(routees =
          for(i <- 0 until numberOfConnections) yield {
            println("creating actor on " + host + ":" + port + " with parent " + context.parent + ", dispatcher is " + context.dispatcher)
            val channel = ChannelFactory.create(host, port, creator)
            context.actorOf(Props(new ChannelActor(channel)), name = channel.getId.toString)
          }
        )), NodeRouterMaker.makeName(host, port))
      })
      nodes = nodes :+ node
      node
    }).actorRef
  }

  override def toString = "NodeManager[nodes = " + nodes + "]"

  init.map( _.foreach { node =>
    addNode(node._1, node._2)
  })
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

class MongoDBSystem(val nodes: List[(String, Int)]) extends Actor {
  val nodeManager = new NodeManager(self, context, 3, Some(nodes))

  var primary :Option[ActorRef] = None
  var isElecting = false

  log("starting with " + nodeManager)

  // todo: if primary changes again while sending queued messages ?
  private val queuedMessages = scala.collection.mutable.Queue[(ActorRef, Either[(WritableMessage[WritableOp], WritableMessage[WritableOp]), WritableMessage[WritableOp]])]()
  private val awaitingResponses = scala.collection.mutable.ListMap[Int, ActorRef]()

  override def receive = {
    // todo: refactoring
    case message: WritableMessage[WritableOp] => {
      log("received a message!")
      if(message.op.expectsResponse) {
        awaitingResponses += ((message.requestID, sender))
        log("registering awaiting response for requestID " + message.requestID + ", awaitingResponses: " + awaitingResponses)
      } else log("NOT registering awaiting response for requestID " + message.requestID)
      if(secondaryOK(message)) {
        resolveReceiver(message) forward message
      } else if(!primary.isDefined) {
        log("delaying send because primary is not available")
        queuedMessages += (sender -> Right(message))
        if(!isElecting) elect
      } else {
        primary.get forward message
      }
    }
    case (message: WritableMessage[WritableOp], writeConcern: WritableMessage[WritableOp]) => {
      log("received a message!")
      //if(message.op.expectsResponse) {
        awaitingResponses += ((message.requestID, sender))
        log("registering writeConcern-awaiting response for requestID " + message.requestID + ", awaitingResponses: " + awaitingResponses)
      //} else log("NOT registering awaiting response for requestID " + message.requestID)
      if(secondaryOK(message)) {
        resolveReceiver(message) forward (message, writeConcern)
      } else if(!primary.isDefined) {
        log("delaying send because primary is not available")
        queuedMessages += (sender -> Left(message -> writeConcern))
        if(!isElecting) elect
      } else {
        primary.get forward ((message, writeConcern))
      }
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
        case None => {
          log("oups. " + message.header.responseTo + " not found! complete message is " + message)
          log("maybe it's for myself?") // check if the message is a replset msg. And replace replstatus by ismaster
          val toAdd = parseForRSNodes(DefaultBSONReaderHandler.handle(message.reply, message.documents).next).get
          for(node <- toAdd) {
            node._3 match {
              case PRIMARY => {
                isElecting = false
                primary = Some(nodeManager.addNode(node._1, node._2))
                log("ok, found primary, sending all the queued messages...")
                queuedMessages.dequeueAll( _ => true).foreach {
                  case (actorRef, Left( (message, writeConcern) )) => primary.get tell ( (message, writeConcern), actorRef )
                  case (actorRef, Right(message)) => primary.get tell (message, actorRef)
                }
              }
              case SECONDARY => nodeManager.addNode(node._1, node._2)
              case _ =>
            }
          }
          log("received " + toAdd)
          log("added nodes, now nodes are : " + nodeManager)
        }
      }
    }
    case _ => log("not supported")
  }

  def log(s: String) = println("MongoDBSystem [" + self.path + "] : " + s)

  def secondaryOK(message: WritableMessage[WritableOp]) = !message.op.requiresPrimary && (message.op match {
    case query: Query => (query.flags & QueryFlags.SlaveOk) != 0
    case _ :KillCursors => true
    case _ => false
  })

  def elect = {
    log("starting electing process...")
    isElecting = true
    for(node <- nodeManager.all) {
      node ! ReplStatus.makeWritableMessage
    }
  }

  sealed trait NodeState
  object PRIMARY extends NodeState
  object SECONDARY extends NodeState
  object ANY_OTHER_STATE extends NodeState // well, we dont care for other states right now

  def parseForRSNodes(document: BSONIterator) :Option[List[(String, Int, NodeState)]]= {
    val map = document.mapped
    println(map)
    map.get("members").flatMap {
      case BSONArray(_, b) => {
        val it: Iterator[(String, Int, NodeState)] = for(o <- DefaultBSONIterator(b)) yield {
          val member = DefaultBSONIterator(o.asInstanceOf[BSONDocument].value).mapped
          val (name, port) = member.get("name").get.asInstanceOf[BSONString].value.span(_ != ':')
          val p2 = port.drop(1)
          (name, port.drop(1).toInt, member.get("state").get.asInstanceOf[BSONInteger].value match {
            case 1 => PRIMARY
            case 2 => SECONDARY
            case _ => ANY_OTHER_STATE
          })
        }
        Some(it.toList)
      }
      case _ => None
    }
  }

  def resolveReceiver(message: WritableMessage[WritableOp]) :ActorRef = message.connectionId.map{ id => context.actorFor(id.toString) }.getOrElse(nodeManager.pick)
}

class MongoConnection(
  val mongosystem: ActorRef
) {
  /** write an op and wait for db response */
  def ask(message: WritableMessage[WritableOp])(implicit timeout: Timeout) :Future[ReadReply] = {
    (mongosystem ? message).mapTo[ReadReply]
  }

  /** write a no-response op followed by a GetLastError command and wait for its response */
  def ask(message: WritableMessage[WritableOp], writeConcern: GetLastError = GetLastError())(implicit timeout: Timeout) = {
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