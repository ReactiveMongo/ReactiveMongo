package org.asyncmongo

import akka.actor.{Actor, ActorRef}
import akka.actor.ActorSystem
import akka.actor.Props
import protocol._
import akka.actor.ActorContext
import akka.routing.RoundRobinRouter
import java.net.InetSocketAddress
import akka.routing.Broadcast

package actors {
  import java.nio.ByteOrder
  import org.jboss.netty.bootstrap._
  import org.jboss.netty.channel._
  import org.jboss.netty.buffer._
  import org.jboss.netty.channel.socket.nio._

  class ChannelActor(val host: String = "localhost", val port: Int = 27017) extends Actor {
    println("creating actor on " + host + ":" + port + " with parent " + context.parent + ", dispatcher is " + context.dispatcher)
    val channel = ChannelFactory.create(host, port, context.parent)
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
      case _ => log("something else")
    }
    def log(s: String) = println("ChannelActor [" + self.path + "] : " + s)
  }

  private object ActorMaker {
    def makeRouter(context: ActorContext, name: String, n: Int = 3, host: String = "localhost", port: Int = 27017) :ActorRef = {
      context.actorOf(Props(new ChannelActor(host, port)).withRouter(RoundRobinRouter(routees = 
        for(i <- 0 to n) yield context.actorOf(Props(new ChannelActor(host, port)), name = name + "connection" + i)
      )))
    }
  }
  
  class MongoActor(nodes: List[(String, Int)]) extends Actor {
    import scala.collection.mutable.ListMap
    //private val channelActor = context.actorOf(Props[ChannelActor], name = "mongoconnection1")
    private val channelActor = ActorMaker.makeRouter(context, "primary")

    private val awaitingResponses = ListMap[Int, ActorRef]()

    override def receive = {
      case message: WritableMessage[AwaitingResponse] => {
        awaitingResponses += ((message.requestID, sender))
        log("registering awaiting response for requestID " + message.requestID + ", awaitingResponses: " + awaitingResponses)
        channelActor forward message
      }
      case (message: WritableMessage[WritableOp], writeConcern: WritableMessage[WritableOp]) => {
        awaitingResponses += ((message.requestID, sender))
        log("registering writeConcern-awaiting response for requestID " + message.requestID + ", awaitingResponses: " + awaitingResponses)
        channelActor forward (message, writeConcern)
        //channelActor forward writeConcern
      }
      case message: WritableMessage[WritableOp] => {
        log("NOT registering awaiting response for requestID " + message.requestID)
        channelActor forward message
      }
      case message: ReadReply => {
        awaitingResponses.get(message.header.responseTo) match {
          case Some(_sender) => {
            log("Got a response! Will give back message="+message + " to sender " + _sender)
            _sender ! message
          }
          case None => log("oups. " + message.header.responseTo + " not found! complete message is " + message)
        }
      }
      case _ => log("not supported")
    }
    def log(s: String) = println("MongoActor [" + self.path + "] : " + s)
  }

  object ChannelFactory {
    import java.net.InetSocketAddress
    import java.util.concurrent.Executors

    val factory = new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool,
      Executors.newCachedThreadPool
    )

    def create(host: String = "localhost", port: Int = 27017, receiver: ActorRef = MongoSystem.actor) = {
      val bootstrap = new ClientBootstrap(factory)

      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
        override def getPipeline :ChannelPipeline = {
          //Channels.pipeline(new MongoDecoder(), new MongoEncoder(), new MongoHandler())
          println("getting a new pipeline")
          Channels.pipeline(new WritableMessageEncoder(), new ReplyFrameDecoder(), new ReplyDecoder(), new MongoHandler(receiver))
        }
      })

      bootstrap.setOption("tcpNoDelay", true)
      bootstrap.setOption("bufferFactory", new HeapChannelBufferFactory(java.nio.ByteOrder.LITTLE_ENDIAN))
      bootstrap.connect(new InetSocketAddress(host, port)).await.getChannel
    }
  }
}

class AnotherActor(val name: String) extends Actor {
  def receive = {
    case msg => {
      println(self + " says '" + name + "', received " + msg + ", my parent is " + context.parent)
    }
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
    if(toRemove.isEmpty && toAdd.isEmpty)
      routeeProvider.registerRoutees(routeeProvider.routees)
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

class MasterActor extends Actor {
  val resizer = /*new akka.routing.Resizer {
    var toAdd :IndexedSeq[ActorRef] = (for(i <- 0 to 1) yield context.actorOf(Props(new AnotherActor(""+i)),  ""+i))
    var toRemove :IndexedSeq[ActorRef] = Vector()
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
  }*/
  new NodeResizer(Some(for(i <- 0 to 1) yield context.actorOf(Props(new AnotherActor(""+i)),  ""+i)))

  val router = context.actorOf(Props.empty.withRouter(RoundRobinRouter(resizer = Some(resizer))))

  var count = 0

  def receive = {
    case msg => {
      //println("MasterActor received " + msg + ", dispatching...")
      if(count == 4) {
        resizer.addActor(context.actorOf(Props(new AnotherActor("new"))))
        println("")
      }
      if(count == 5) {
        val _1 = context.actorFor("1")
        println(_1)
        resizer.removeActor(_1)
      }
      count += 1
      router forward msg
    }
  }
}

object MasterActor {
  val system = ActorSystem("mongosystem")
  lazy val actor = system.actorOf(Props[MasterActor])
  def test = {
    actor ! "message 1"
    actor ! "message 2"
    actor ! "message 3"
    actor ! "message 4"
    actor ! "message 5"
    actor ! "message 6"
    actor ! "message 7"
    actor ! "message 8"
    actor ! "message 9"
    actor ! "message 10"
    actor ! "message 11"
    actor ! "message 12"
    actor ! "message 13"
    actor ! "message 14"
    actor ! "message 15"
  }
}

class ReplicaSetElector extends Actor {
  import akka.util.Timeout
  import akka.util.duration._
  import akka.pattern.ask
  
  implicit val timeout = Timeout(5 seconds)
  
  println("building ReplicaSetElector: " + self)
  
  def receive = {
    case LookForMaster => {
      log("sending broadcast...")
      context.parent ! Broadcast(protocol.messages.IsMaster.makeWritableMessage("admin", 0))
    }
    case response: ReadReply => {
      log("got response " + response)
    }
    case _ => log("not understandable response")
  }
  def log(s: String) = println("ReplicaSetElector [" + self.path + "] : " + s)
}

case object LookForMaster

/** Each actor here is a router to one or many channels (a router actor routes for a mongodb node) */
case class Nodes(
  secondaries: Set[ActorRef],
  primary: ActorRef
) {
  lazy val secondaryRouter = null
}

object Nodes {
  def apply(nodes: List[(String, Int)]) = {
null
  }
}

class MongoDBSystem(val nodes: List[(String, Int)]) extends Actor {
  //val _actors = 
  //val secondaries = context.actorOf(Props.empty.withRouter(RoundRobinRouter(resizer = Some(new NodeResizer(for(node <- nodes) yield context.actorOf(Props(new actors.ChannelActor(node._1, node._2))))))))
  val secondaries = context.actorOf(
    Props(new actors.ChannelActor("192.168.0.1", 27018)).withRouter(
      RoundRobinRouter/*(routees =
        for(node <- nodes) yield context.actorOf(Props(new actors.ChannelActor(node._1, node._2)))
      ).copy*/(resizer = Some(new NodeResizer(Some((for(node <- nodes) yield context.actorOf(Props(new actors.ChannelActor(node._1, node._2)).
        withDispatcher("secondaries-dispatcher"))).toIndexedSeq))))
    ).withDispatcher("secondaries-dispatcher")
  )

  val s = context.actorOf(Props(new actors.ChannelActor("localhost", 27017)))

  private val awaitingResponses = scala.collection.mutable.ListMap[Int, ActorRef]()

  override def receive = {
    case message: WritableMessage[AwaitingResponse] => {
      awaitingResponses += ((message.requestID, sender))
      log("registering awaiting response for requestID " + message.requestID + ", awaitingResponses: " + awaitingResponses)
      secondaries forward message
    }
    case (message: WritableMessage[WritableOp], writeConcern: WritableMessage[WritableOp]) => {
      awaitingResponses += ((message.requestID, sender))
      log("registering writeConcern-awaiting response for requestID " + message.requestID + ", awaitingResponses: " + awaitingResponses)
      secondaries forward (message, writeConcern)
    }
    case message: WritableMessage[WritableOp] => {
      log("NOT registering awaiting response for requestID " + message.requestID)
      secondaries forward message
    }
    case message: ReadReply => {
      awaitingResponses.get(message.header.responseTo) match {
        case Some(_sender) => {
          log("Got a response from " + sender + "! Will give back message="+message + " to sender " + _sender)
          _sender ! message
        }
        case None => log("oups. " + message.header.responseTo + " not found! complete message is " + message)
      }
    }
    /*case Broadcast(message) => message match {
      case message: WritableMessage[AwaitingResponse] => {
        for(a <- channelActors) {
          val requestID = (new java.util.Random()).nextInt(Integer.MAX_VALUE)
          val toSend = message.copy(requestID = requestID)
          awaitingResponses += ((requestID, sender))
          log("[BROADCAST] registering awaiting response for requestID " + message.requestID + ", awaitingResponses: " + awaitingResponses)
          a ! (message, sender)
        }
      }
      case _ => log("[BROADCAST] not supported")
    }*/
    case _ => log("not supported")
  }
  def log(s: String) = println("MongoDBSystem [" + self.path + "] : " + s)
}

class MongoConnection(
  mongosystem: ActorRef
) {
  import protocol.messages._
  import akka.util.Timeout
  import akka.dispatch.Future

  /** write a response op and get a future for its response */
  def ask(message: WritableMessage[AwaitingResponse])(implicit timeout: Timeout) :Future[ReadReply] = {
    import akka.pattern.ask
    (mongosystem ? message).mapTo[ReadReply]
  }

  /** write a no-response op and wait for db response */
  def ask(message: WritableMessage[WritableOp])(implicit timeout: Timeout) = None

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

object MongoDBSystem {
  import protocol.messages._
  import akka.util.Timeout
  import akka.dispatch.Future
  import akka.util.duration._

  implicit val timeout = Timeout(5 seconds)

  def test = {
    val connection = MongoConnection( List( "localhost" -> 27017 ) )
    connection send list
    connection send insert
    connection ask(insert, GetLastError())
    val future = connection ask list
    println(future)
    future.onComplete({
      case Right(reply) => {
        println("future is complete! got this response: " + reply)
        //connection.stop
      }
      case Left(error) => throw error
    })
  }
  /*val system = ActorSystem("mongodb")
  def makeDBSystem(nodes: List[(String, Int)]) = system.actorOf(Props(new MongoDBSystem(nodes)), name="db1")
  
  def test = {
    val act = makeDBSystem(List("localhost" -> 27017))
    act ! "toto"
    val elector = system.actorFor(act.path.child("elector"))
    println("elector is " + elector)
    act ! "toto"
    elector ! LookForMaster
    elector ! "hello"
  }*/
  import java.io._
  import de.undercouch.bson4jackson._
  import de.undercouch.bson4jackson.io._

  def insert = {
    val factory = new BsonFactory()
    //serialize data
    val baos = new ByteArrayOutputStream();
    val gen = factory.createJsonGenerator(baos);
    gen.writeStartObject();
    gen.writeStringField("name", "Jack")
    gen.writeEndObject()
    gen.close()
    
    val random = (new java.util.Random()).nextInt(Integer.MAX_VALUE)
    println("generated list request #" + random)

    WritableMessage(random, 0, Insert(0, "plugin.acoll"), baos.toByteArray)
  }

  def list = {
    val factory = new BsonFactory()
 
    //serialize data
    val baos = new ByteArrayOutputStream();
    val gen = factory.createJsonGenerator(baos);
    gen.writeStartObject();
    gen.writeEndObject()
    gen.close()

    val random = (new java.util.Random()).nextInt(Integer.MAX_VALUE)
    println("generated list request #" + random)

    WritableMessage(random, 0, Query(0, "plugin.acoll", 0, 0), baos.toByteArray)
  }
}

object MongoSystem {
  import protocol.messages._
  import akka.util.Timeout
  import akka.dispatch.Future

  val system = ActorSystem("mongosystem")
  val actor = system.actorOf(Props(new actors.MongoActor(Nil)), name = "router")

  /** write a response op and get a future for its response */
  def ask(message: WritableMessage[AwaitingResponse])(implicit timeout: Timeout) :Future[ReadReply] = {
    import akka.pattern.ask
    (actor ? message).mapTo[ReadReply]
  }

  /** write a no-response op and wait for db response */
  def ask(message: WritableMessage[WritableOp])(implicit timeout: Timeout) = None

  /** write a no-response op followed by a GetLastError command and wait for its response */
  def ask(message: WritableMessage[WritableOp], writeConcern: GetLastError = GetLastError())(implicit timeout: Timeout) = {
    import akka.pattern.ask
    (actor ? ((message, writeConcern.makeWritableMessage("plugin", message.header.requestID)))).mapTo[ReadReply]
  }

  /** write a no-response op without getting a future */
  def send(message: WritableMessage[WritableOp]) = actor ! message
}


object Client {
  import java.io._
  import de.undercouch.bson4jackson._
  import de.undercouch.bson4jackson.io._

  import protocol.messages._
  import akka.util.Timeout
  import akka.util.duration._

  implicit val timeout = Timeout(5 seconds)

  def testList {
    import akka.dispatch.Await
    val future = MongoSystem ask list
    println("in test: future is " + future)
    val response = Await.result(future, timeout.duration)
    println("hey, got response! " + response)
    println("response embeds " + response.reply.numberReturned + " documents")
      //println("find conn " + actor + " for responseTo " + readReply.header.responseTo)
    future
    //MongoSystem send insert
  }

  def testNonWaitingList {
    MongoSystem send list
    MongoSystem send insert
    MongoSystem ask(insert, GetLastError())
    val future = MongoSystem ask list
    println(future)
    future.onComplete({
      case Right(reply) => {
        println("future is complete! got this response: " + reply)
      }
      case Left(error) => throw error
    })
    //MongoSystem send insert
  }

  def test {
    MongoSystem send list
    //testNonWaitingList
  }

  def insert = {
    val factory = new BsonFactory()
    //serialize data
    val baos = new ByteArrayOutputStream();
    val gen = factory.createJsonGenerator(baos);
    gen.writeStartObject();
    gen.writeStringField("name", "Jack")
    gen.writeEndObject()
    gen.close()
    
    val random = (new java.util.Random()).nextInt(Integer.MAX_VALUE)
    println("generated list request #" + random)

    WritableMessage(random, 0, Insert(0, "plugin.acoll"), baos.toByteArray)
  }

  def list = {
    val factory = new BsonFactory()
 
    //serialize data
    val baos = new ByteArrayOutputStream();
    val gen = factory.createJsonGenerator(baos);
    gen.writeStartObject();
    gen.writeEndObject()
    gen.close()

    val random = (new java.util.Random()).nextInt(Integer.MAX_VALUE)
    println("generated list request #" + random)

    WritableMessage(random, 0, Query(0, "plugin.acoll", 0, 0), baos.toByteArray)
  }
}
