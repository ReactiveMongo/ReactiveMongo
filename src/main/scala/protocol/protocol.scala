package org.asyncmongo.protocol

import java.nio.ByteOrder
import org.jboss.netty.bootstrap._
import org.jboss.netty.channel._
import org.jboss.netty.buffer._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.handler.codec.oneone._
import org.codehaus.jackson.map.ObjectMapper
import org.asyncmongo.MongoSystem
import org.asyncmongo.utils.RichBuffer._
import org.asyncmongo.utils.BufferAccessors._
import org.codehaus.jackson.JsonNode
import org.jboss.netty.handler.codec.frame.FrameDecoder
import akka.actor.ActorRef


 // traits
trait ChannelBufferWritable {
  //def writeTo(buffer: ChannelBuffer) :Unit
  val writeTo :ChannelBuffer => Unit
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
  override val writeTo = writeTupleToBuffer4( (messageLength, requestID, responseTo, opCode) ) _
  override def size = 4 + 4 + 4 + 4
}

object MessageHeader extends ChannelBufferReadable[MessageHeader] {
  override def readFrom(buffer: ChannelBuffer) = {
    println("byte order of response is " + buffer.order)
    val messageLength = buffer.readInt
    val requestID = buffer.readInt
    val responseTo = buffer.readInt
    val opCode = buffer.readInt
    MessageHeader(
      messageLength,
      requestID,
      responseTo,
      opCode)
  }
}

case class WritableMessage[+T <: WritableOp] (
  requestID: Int,
  responseTo: Int,
  op: T,
  documents: ChannelBuffer
) extends ChannelBufferWritable {
  override val writeTo = { buffer: ChannelBuffer => {
    buffer write header
    buffer write op
    buffer writeBytes documents
  } }
  override def size = 16 + op.size + documents.writerIndex
  lazy val header = MessageHeader(size, requestID, responseTo, op.code)
}

object WritableMessage{
  def apply[T <: WritableOp](requestID: Int, responseTo: Int, op: T, documents: Array[Byte]) :WritableMessage[T] = {
    WritableMessage(
      requestID,
      responseTo,
      op,
      ChannelBuffers.wrappedBuffer(ByteOrder.LITTLE_ENDIAN, documents))
  }
  def apply[T <: WritableOp](op: T, documents: ChannelBuffer) :WritableMessage[T] = WritableMessage.apply((new java.util.Random()).nextInt(Integer.MAX_VALUE), 0, op, documents)
  def apply[T <: WritableOp](op: T, documents: Array[Byte]) :WritableMessage[T] = WritableMessage.apply((new java.util.Random()).nextInt(Integer.MAX_VALUE), 0, op, documents)
}

case class ReadReply(
  header: MessageHeader,
  reply: Reply,
  documents: ChannelBuffer,
  info: ReadReplyInfo
)

case class ReadReplyInfo(channelID: Int, localAddress: String, remoteAddress: String)

class WritableMessageEncoder extends OneToOneEncoder {
  def encode(ctx: ChannelHandlerContext, channel: Channel, obj: Object) = {
    obj match {
      case message: WritableMessage[WritableOp] => {
        val buffer :ChannelBuffer = ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 1000)
        message writeTo buffer
        buffer
      }
      case _ => {
         //println("WritableMessageEncoder: weird... " + obj)
         obj
      }
    }
  }
}

class ReplyFrameDecoder extends FrameDecoder {
  override def decode(context: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer) = {
    val readableBytes = buffer.readableBytes
    if(readableBytes < 4) null
    else {
      buffer.markReaderIndex
      val length = buffer.readInt
      println("decode:: readableBytes=" + readableBytes + ", claimed length is " + length)
      buffer.resetReaderIndex
      if(length <= readableBytes && length > 0)
        buffer.readBytes(length)
      else null
    }
  }
}

class ReplyDecoder extends OneToOneDecoder {
  def decode(ctx: ChannelHandlerContext, channel: Channel, obj: Object) = {
    val buffer = obj.asInstanceOf[ChannelBuffer]
    val header = MessageHeader(buffer)
    val reply = Reply(buffer)

    //println("buffer is " + buffer.readableBytes)

    /*val docs = new Array[Byte](buffer.readableBytes)
    buffer.readBytes(docs)*/ 
    //println("available ? " + buffer.readableBytes)

    /*val json = MapReaderHandler.handle(reply, buffer).next
    println(header)
    println(reply)
    println(json)*/
    import java.net.InetSocketAddress
    ReadReply(header, reply, buffer, ReadReplyInfo(channel.getId, channel.getLocalAddress.asInstanceOf[InetSocketAddress].toString, channel.getRemoteAddress.asInstanceOf[InetSocketAddress].toString))
  }
}

class MongoHandler(receiver: ActorRef) extends SimpleChannelHandler {
  println("MongoHandler: receiver is " + receiver)
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val readReply = e.getMessage.asInstanceOf[ReadReply]
    log(e, "messageReceived " + readReply + " will be send to " + receiver)
    receiver ! readReply
    super.messageReceived(ctx, e)
  }
  override def writeComplete(ctx: ChannelHandlerContext, e: WriteCompletionEvent) {
    log(e, "a write is complete!")
    super.writeComplete(ctx, e)
  }
  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    log(e, "a write is requested!")
    super.writeRequested(ctx, e)
  }
  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log(e, "connected")
    super.channelConnected(ctx, e)
  }
  def log(e: ChannelEvent, s: String) = println("MongoHandler [" + e.getChannel.getId + "] : " + s)
}

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