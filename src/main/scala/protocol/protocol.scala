package org.asyncmongo.protocol

import java.nio.ByteOrder
import org.jboss.netty.bootstrap._
import org.jboss.netty.channel._
import org.jboss.netty.buffer._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.handler.codec.oneone._

import org.codehaus.jackson.map.ObjectMapper

import org.asyncmongo.MongoSystem

 // implicits
object `package` {
  case class ExtendedBuffer(buffer: ChannelBuffer) {
    def writeUTF8(s: String) {
      buffer writeBytes (s.getBytes("UTF-8"))
      buffer writeByte 0
    }
    def write(writable: ChannelBufferWritable) {
      writable writeTo buffer
    }
  }
  implicit def channelBufferToExtendedBuffer(buffer: ChannelBuffer) = ExtendedBuffer(buffer)
}

 // traits
trait ChannelBufferWritable {
  def writeTo(buffer: ChannelBuffer) :Unit
  def size: Int
}

trait ChannelBufferReadable[T] {
  def readFrom(buffer: ChannelBuffer) :T
  def apply(buffer: ChannelBuffer) :T = readFrom(buffer)
}



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

case class WritableMessage[+T <: WritableOp] (
  requestID: Int,
  responseTo: Int,
  op: T,
  //val documents: Array[Byte]
  documents: ChannelBuffer,
  expectingLastError :Boolean
) extends ChannelBufferWritable {
  override def writeTo(buffer: ChannelBuffer) {
    //println("write into buffer, header=" + header + ", op=" + op)
    buffer write header
    buffer write op
    buffer writeBytes documents
  }
  override def size = 16 + op.size + documents.writerIndex
  lazy val header = MessageHeader(size, requestID, responseTo, op.code)
}

object WritableMessage{
  def apply[T <: WritableOp](requestID: Int, responseTo: Int, op: T, documents: Array[Byte], expectingLastError :Boolean) :WritableMessage[T] = {
    WritableMessage(
      requestID,
      responseTo,
      op,
      ChannelBuffers.wrappedBuffer(ByteOrder.LITTLE_ENDIAN, documents),
      expectingLastError)
  }
  def apply[T <: WritableOp](requestID: Int, responseTo: Int, op: T, documents: Array[Byte]) :WritableMessage[T] = WritableMessage.apply(requestID, responseTo, op, documents, false)
  def apply[T <: WritableOp](requestID: Int, responseTo: Int, op: T, documents: ChannelBuffer) :WritableMessage[T] = WritableMessage.apply(requestID, responseTo, op, documents, false)
}

case class ReadReply(
  header: MessageHeader,
  reply: Reply,
  documents: Array[Byte]
)

class WritableMessageEncoder extends OneToOneEncoder {
  def encode(ctx: ChannelHandlerContext, channel: Channel, obj: Object) = {
    obj match {
      case message: WritableMessage[WritableOp] => {
        println("WritableMessageEncoder: encoding " + message)
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
    val buffer = obj.asInstanceOf[ChannelBuffer]
    val header = MessageHeader(buffer)
    val reply = Reply(buffer)

    println("buffer is " + buffer.readableBytes)

    val docs = new Array[Byte](buffer.readableBytes)
    buffer.readBytes(docs)
    println("available ? " + buffer.readableBytes)

    /*val json = MapReaderHandler.handle(reply, buffer).next
    println(header)
    println(reply)
    println(json)*/
    ReadReply(header, reply, docs)
  }
}

object PrebuiltMessages {
  def getLastError(db: String, requestID: Int) = {
    import org.asyncmongo.bson._

    val bson = new Bson
    bson.writeElement("getlasterror", 1)
    
    WritableMessage(requestID, 0, Query(0, db + ".$cmd", 0, 1), bson.getBuffer)
  }
}

class MongoHandler extends SimpleChannelHandler {
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val readReply = e.getMessage.asInstanceOf[ReadReply]
    println("MongoHandler: messageReceived " + readReply)
    MongoSystem.actor ! readReply
    super.messageReceived(ctx, e)
  }
  override def writeComplete(ctx: ChannelHandlerContext, e: WriteCompletionEvent) {
    println("MongoHandler: a write is complete!")
    super.writeComplete(ctx, e)
  }
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    println("MongoHandler: a write is requested!")
    super.writeRequested(ctx, e)
  }
  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    println("MongoHandler: connected")
    super.channelConnected(ctx, e)
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