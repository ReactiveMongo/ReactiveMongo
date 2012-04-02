package org.asyncmongo

import java.net.InetSocketAddress
import java.util.concurrent.Executors

import java.nio.ByteOrder
import org.jboss.netty.bootstrap._
import org.jboss.netty.channel._
import org.jboss.netty.buffer._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.handler.codec.oneone._

import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.logging.InternalLogLevel

import org.codehaus.jackson.map.ObjectMapper

import akka.actor.{Actor, ActorRef}
import akka.actor.ActorSystem
import akka.actor.Props


package protocol {
  // implicits
  object `package` {
    case class ExtendedBuffer(buffer: ChannelBuffer) {
      def writeUTF8(s: String) {
        buffer writeBytes (s.getBytes("UTF-8"))
      }
      def write(writable: ChannelBufferWritable) {
        writable writeTo buffer
      }
    }
    implicit def channelBufferToExtendedBuffer(buffer: ChannelBuffer) = ExtendedBuffer(buffer)
  }

  // traits
  trait ChannelBufferWritable extends SizeMeasurable {
    def writeTo(buffer: ChannelBuffer) :Unit
  }
  trait ChannelBufferReadable[T] {
    def readFrom(buffer: ChannelBuffer) :T
    def apply(buffer: ChannelBuffer) :T = readFrom(buffer)
  }
  trait SizeMeasurable {
    def size :Int
  }

  sealed trait Op {
    val code :Int
  }
  sealed trait WritableOp extends Op with ChannelBufferWritable

  trait BSONReader[DocumentType] {
    val count: Int
    def next: Option[DocumentType]
  }
  trait BSONReaderHandler[DocumentType] {
    def handle(reply: Reply, buffer: ChannelBuffer) :BSONReader[DocumentType]
  }

  // concrete classes
  case class MessageHeader(
    messageLength: Int,
    requestID: Int,
    responseTo: Int,
    opCode: Int
  ) extends ChannelBufferWritable {
    def writeTo(buffer: ChannelBuffer) {
      buffer writeInt messageLength
      buffer writeInt requestID
      buffer writeInt responseTo
      buffer writeInt opCode
    }
    override def size = 4 + 4 + 4 + 4
  }

  object MessageHeader extends ChannelBufferReadable[MessageHeader] {
    override def readFrom(buffer: ChannelBuffer) = MessageHeader(
      buffer.readInt,
      buffer.readInt,
      buffer.readInt,
      buffer.readInt
    )
  }

  case class Query(
    flags: Int,
    fullCollectionName: String,
    numberToSkip: Int,
    numberToReturn: Int
  ) extends WritableOp {
    override val code = 2004
    override def writeTo(buffer: ChannelBuffer) {
      buffer writeInt flags
      buffer writeUTF8 fullCollectionName
      buffer writeByte 0
      buffer writeInt numberToSkip
      buffer writeInt numberToReturn
    }
    override def size = 4 + fullCollectionName.length + 1 + 4 + 4
  }

  case class Reply(
    flags: Int,
    cursorID: Long,
    startingFrom: Int,
    numberReturned: Int
  ) extends Op {
    override val code = 1
  }

  object Reply extends ChannelBufferReadable[Reply] {
    def readFrom(buffer: ChannelBuffer) = Reply(
      buffer.readInt,
      buffer.readLong,
      buffer.readInt,
      buffer.readInt
    )
  }

  case class WritableMessage(
    requestID: Int,
    responseTo: Int,
    op: WritableOp,
    documents: Array[Byte]
  ) extends ChannelBufferWritable {
    override def writeTo(buffer: ChannelBuffer) {
      println("write into buffer, header=" + header + ", op=" + op)
      buffer write header
      buffer write op
      buffer writeBytes documents
    }
    override def size = 16 + op.size + documents.size
    lazy val header = MessageHeader(size, requestID, responseTo, op.code)
  }

  case class ReadReply(
    header: MessageHeader,
    reply: Reply,
    documents: Array[Byte]
  )

  class WritableMessageEncoder extends OneToOneEncoder {
    def encode(ctx: ChannelHandlerContext, channel: Channel, obj: Object) = {
      obj match {
        case message: WritableMessage => {
          println("WritableMessageEncoder: " + message)
          val buffer :ChannelBuffer = ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 1000)
          message writeTo buffer
          buffer
        }
        case _ => {
           println("WritableMessageEncoder: weird... " + obj)
           obj
        }
      }
    }
  }

  class ReplyDecoder extends OneToOneDecoder {
    def decode(ctx: ChannelHandlerContext, channel: Channel, obj: Object) = {
      println("ReplyDecoder: " + obj.asInstanceOf[ChannelBuffer].factory().getDefaultOrder)
      val buffer = obj.asInstanceOf[ChannelBuffer]
      val header = MessageHeader(buffer)
      val reply = Reply(buffer)
      val json = MapReaderHandler.handle(reply, buffer).next
      println(header)
      println(reply)
      println(json)
      ReadReply(header, reply, Array())
    }
  }

  class MongoHandler extends SimpleChannelHandler {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      import akka.dispatch.Await
      import akka.pattern.ask
      import akka.util.Timeout
      import akka.util.duration._
      /*val buf = e.getMessage().asInstanceOf[ChannelBuffer]
      val response = MongoResponse.apply(buf)*/
      println("handler: message received " + e.getMessage)
      MongoSystem.actor ! e.getMessage.asInstanceOf[ReadReply]
      /*implicit val timeout = Timeout(5 seconds)
      val readReply = e.getMessage.asInstanceOf[ReadReply]
      val actor = Await.result( (MongoSystem.actor ? readReply.header.responseTo), Timeout(5 seconds).duration).asInstanceOf[Option[String]]*/
      //println("find conn " + actor + " for responseTo " + readReply.header.responseTo)
    }
    override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      println("connected")
    }
  }


  // json stuff
  import org.codehaus.jackson.JsonNode

  object JacksonNodeReaderHandler extends BSONReaderHandler[JsonNode] {
    override def handle(reply: Reply, buffer: ChannelBuffer) :BSONReader[JsonNode] = JacksonNodeReader(reply.numberReturned, buffer)
  }

  case class JacksonNodeReader(count: Int, buffer: ChannelBuffer) extends BSONReader[JsonNode] {
    import de.undercouch.bson4jackson._
    import de.undercouch.bson4jackson.io._
    import de.undercouch.bson4jackson.uuid._

    private val mapper = {
      val fac = new BsonFactory()
      fac.enable(BsonParser.Feature.HONOR_DOCUMENT_LENGTH)
      val om = new ObjectMapper(fac)
      om.registerModule(new BsonUuidModule())
      om
    }
    private val is = new ChannelBufferInputStream(buffer)

    override def next :Option[JsonNode] = {
      if(is.available > 0)
        Some(mapper.readValue(new ChannelBufferInputStream(buffer), classOf[JsonNode]))
      else None
    }
  }

  object MapReaderHandler extends BSONReaderHandler[java.util.HashMap[Object, Object]] {
    override def handle(reply: Reply, buffer: ChannelBuffer) :BSONReader[java.util.HashMap[Object, Object]] = MapReader(reply.numberReturned, buffer)
  }

  case class MapReader(count: Int, buffer: ChannelBuffer) extends BSONReader[java.util.HashMap[Object, Object]] {
    import de.undercouch.bson4jackson._
    import de.undercouch.bson4jackson.io._
    import de.undercouch.bson4jackson.uuid._

    private val mapper = {
      val fac = new BsonFactory()
      fac.enable(BsonParser.Feature.HONOR_DOCUMENT_LENGTH)
      val om = new ObjectMapper(fac)
      om.registerModule(new BsonUuidModule())
      om
    }
    private val is = new ChannelBufferInputStream(buffer)

    override def next :Option[java.util.HashMap[Object, Object]] = {
      if(is.available > 0)
        Some(mapper.readValue(new ChannelBufferInputStream(buffer), classOf[java.util.HashMap[Object, Object]]))
      else None
    }
  }
}


class ChannelActor extends Actor {
  println("creating channelActor")
  val channel = ChannelFactory.create()
  def receive = {
    case message :protocol.WritableMessage => {
      println("will send WritableMessage " + message)
      val f = channel.write(message)
      f.addListener(new ChannelFutureListener() {
        override def operationComplete(fut: ChannelFuture) {
          println("actor: operation complete with fut="+fut)
        }
      })
      f.await
      println("sent")
    }
    case "toto" => println("toto")
    case s:String => println("received string=" + s)
    case _ => println("something else")
  }
}

class MongoActor extends Actor {
  import scala.collection.mutable.ListMap
  private val channelActor = context.actorOf(Props[ChannelActor], name = "mongoconnection1")
  private val map = ListMap[Int, String]()

  private val m = ListMap[Int, ActorRef]()

  override def receive = {
    case message: protocol.WritableMessage => {
      map += ((message.requestID, "mongoconnection1"))
      m += ((message.requestID, sender))
      println("now map is " + map)
      channelActor forward message
    }
    case requestID: Int => {
      println("in charge from map " + map)
      sender ! inCharge(requestID)
    }
    case message: protocol.ReadReply => {
      m.get(message.header.responseTo) match {
        case Some(_sender) => {
          println("ok, will send message="+message + " to sender " + _sender)
          _sender ! message
        }
        case None => println("oups. " + message.header.responseTo + " not found!")
      }
    }
    case _ => println("not supported")
  }
  def inCharge(requestID: Int) = map.get(requestID) map { s => println("found " + s + " for requestID=" + requestID);context.actorFor(s) }
}

object MongoSystem {
  val system = ActorSystem("mongosystem")
  val actor = system.actorOf(Props[MongoActor], name = "router")

  def send(message: protocol.WritableMessage) = {
    import akka.dispatch.Await
    import akka.pattern.ask
    import akka.util.Timeout
    import akka.util.duration._

    implicit val timeout = Timeout(5 seconds)

    val future = actor ? message
    println("FUTURE is "+ future)
    val response = Await.result(future, timeout.duration).asInstanceOf[protocol.ReadReply]
    println("hey, got response! " + response)
      //println("find conn " + actor + " for responseTo " + readReply.header.responseTo)
    future
  }
}

object ChannelFactory {
  val factory = new NioClientSocketChannelFactory(
    Executors.newCachedThreadPool,
    Executors.newCachedThreadPool
  )

  def create(host: String = "localhost", port: Int = 27017) = {
    val bootstrap = new ClientBootstrap(factory)

    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      override def getPipeline :ChannelPipeline = {
        //Channels.pipeline(new MongoDecoder(), new MongoEncoder(), new MongoHandler())
        Channels.pipeline(new protocol.WritableMessageEncoder(), new protocol.ReplyDecoder(), new protocol.MongoHandler())
      }
    })

    bootstrap.setOption("tcpNoDelay", true)
    bootstrap.setOption("bufferFactory", new HeapChannelBufferFactory(java.nio.ByteOrder.LITTLE_ENDIAN))
    bootstrap.connect(new InetSocketAddress(host, port)).await.getChannel
  }
}





class MongoHandler extends SimpleChannelHandler {
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    /*val buf = e.getMessage().asInstanceOf[ChannelBuffer]
    val response = MongoResponse.apply(buf)*/
    println("handler: message received " + e.getMessage)
  }
  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    println("connected")
  }
}

// TODO que le mongo encoder serve à quelque chose ici...
class MongoEncoder extends OneToOneEncoder {
  def encode(ctx: ChannelHandlerContext, channel: Channel, obj: Object) = {
    println("sending " + obj)
    obj
  }
}

class MongoDecoder/*(callback: () => Unit)*/ extends OneToOneDecoder {
  def decode(ctx: ChannelHandlerContext, channel: Channel, obj: Object) = {
    println("decoding...")
    //MongoResponse.apply(obj.asInstanceOf[ChannelBuffer])
    obj
  }
}

object Client {
  def test {
    //val system = ActorSystem("MySystem")
    //val myActor = system.actorOf(Props[ChannelActor], name = "myactor")
    /*val buffer :ChannelBuffer = ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 1000)
    writeMessage(buffer)*/
    MongoSystem send message
  }

  def main(args: Array[String]) {
    val factory = new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool,
      Executors.newCachedThreadPool
    )

    val bootstrap = new ClientBootstrap(factory)

    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      override def getPipeline :ChannelPipeline = {
        //Channels.pipeline(new protocol.ReplyDecoder(), new MongoDecoder(), new MongoEncoder(), new MongoHandler())
        //Channels.pipeline(new protocol.ReplyDecoder(), new MongoEncoder(), new MongoHandler())
        Channels.pipeline(new protocol.WritableMessageEncoder(), new protocol.ReplyDecoder(), new MongoHandler())
      }
    })

    bootstrap.setOption("tcpNoDelay", true)
    bootstrap.setOption("bufferFactory", new HeapChannelBufferFactory(java.nio.ByteOrder.LITTLE_ENDIAN))
    val channelFuture = bootstrap.connect(new InetSocketAddress("localhost", 27017))

    /*val buffer :ChannelBuffer = ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 1000)
    writeMessage(buffer)

    println("buffer contains: '" + buffer.toString("UTF-8") + "'")*/

    //buffer.writeBytes()
    val channel = channelFuture.await().getChannel
    channel.write(message).await
    println("write done!")

    /*channel.close.awaitUninterruptibly
    println("closed")
    //bootstrap.releaseExternalResources()
    println("all done, exit")*/
  }

  val message = {
    import java.io._
    import de.undercouch.bson4jackson._
    import de.undercouch.bson4jackson.io._

    val factory = new BsonFactory()
 
    //serialize data
    val baos = new ByteArrayOutputStream();
    val gen = factory.createJsonGenerator(baos);
    gen.writeStartObject();
    gen.writeEndObject()
    gen.close()

    //val message = Message(109, 0, Query(0, "plugin.acoll", 0, 0, baos.toByteArray))
    protocol.WritableMessage(109, 0, protocol.Query(0, "plugin.acoll", 0, 0), baos.toByteArray)
  }

  def writeMessage(buffer: ChannelBuffer) {
    import java.io._
    import de.undercouch.bson4jackson._
    import de.undercouch.bson4jackson.io._

    val factory = new BsonFactory()
 
    //serialize data
    val baos = new ByteArrayOutputStream();
    val gen = factory.createJsonGenerator(baos);
    gen.writeStartObject();
    gen.writeEndObject()
    gen.close()

    //val message = Message(109, 0, Query(0, "plugin.acoll", 0, 0, baos.toByteArray))
    val message = protocol.WritableMessage(109, 0, protocol.Query(0, "plugin.acoll", 0, 0), baos.toByteArray)

    println("has built message : " + message)

    message writeTo buffer
  }
}

