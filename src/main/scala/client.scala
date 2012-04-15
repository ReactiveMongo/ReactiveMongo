package org.asyncmongo

import akka.actor.{Actor, ActorRef}
import akka.actor.ActorSystem
import akka.actor.Props
import protocol._
import akka.actor.ActorContext
import akka.routing.RoundRobinRouter
import java.net.InetSocketAddress

package actors {
  import java.nio.ByteOrder
  import org.jboss.netty.bootstrap._
  import org.jboss.netty.channel._
  import org.jboss.netty.buffer._
  import org.jboss.netty.channel.socket.nio._

  class ChannelActor extends Actor {
    val channel = ChannelFactory.create()
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
      context.actorOf(Props[ChannelActor].withRouter(RoundRobinRouter(routees = 
        for(i <- 0 to n) yield context.actorOf(Props[ChannelActor], name = name + "connection" + i)
      )))
    }
  }
  
  class MongoActor extends Actor {
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

    def create(host: String = "localhost", port: Int = 27017) = {
      val bootstrap = new ClientBootstrap(factory)

      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
        override def getPipeline :ChannelPipeline = {
          //Channels.pipeline(new MongoDecoder(), new MongoEncoder(), new MongoHandler())
          println("getting a new pipeline")
          Channels.pipeline(new WritableMessageEncoder(), new ReplyFrameDecoder(), new ReplyDecoder(), new MongoHandler())
        }
      })

      bootstrap.setOption("tcpNoDelay", true)
      bootstrap.setOption("bufferFactory", new HeapChannelBufferFactory(java.nio.ByteOrder.LITTLE_ENDIAN))
      bootstrap.connect(new InetSocketAddress(host, port)).await.getChannel
    }
  }
}

object MongoSystem {
  import protocol.messages._
  import akka.util.Timeout
  import akka.dispatch.Future

  val system = ActorSystem("mongosystem")
  val actor = system.actorOf(Props[actors.MongoActor], name = "router")

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

