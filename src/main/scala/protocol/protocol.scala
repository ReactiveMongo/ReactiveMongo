package org.asyncmongo.protocol

import akka.actor.ActorRef
import java.nio.ByteOrder
import org.asyncmongo.utils.RichBuffer._
import org.asyncmongo.utils.BufferAccessors._
import org.jboss.netty.buffer._
import org.jboss.netty.bootstrap._
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.handler.codec.oneone._
import org.jboss.netty.handler.codec.frame.FrameDecoder

 // traits
trait ChannelBufferWritable {
  val writeTo :ChannelBuffer => Unit
  def size: Int
}

trait ChannelBufferReadable[T] {
  def readFrom(buffer: ChannelBuffer) :T
  def apply(buffer: ChannelBuffer) :T = readFrom(buffer)
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
  documents: ChannelBuffer,
  channelIdHint: Option[Int] = None
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
  def apply[T <: WritableOp](op: T) :WritableMessage[T] = WritableMessage.apply(op, new Array[Byte](0))
}

case class ReadReply(
  header: MessageHeader,
  reply: Reply,
  documents: ChannelBuffer,
  info: ReadReplyInfo) {

  lazy val error :Option[ExplainedError] = {
    if(reply.inError) {
      import org.asyncmongo.handlers.DefaultBSONHandlers._
      val bson = DefaultBSONReaderHandler.handle(reply, documents)
      if(bson.hasNext)
        ExplainedError(DefaultBSONReaderHandler.handle(reply, documents).next)
      else None
    } else None
  }
}

case class ExplainedError(
  err: String
)

object ExplainedError {
  import org.asyncmongo.bson._
  
  def apply(bson: DefaultBSONIterator) :Option[ExplainedError] = {
    bson.find(_.name == "err").map {
      case err: BSONString => ExplainedError(err.value)
      case _ => throw new RuntimeException("???")
    }
  }
}

case class ReadReplyInfo(
  channelId: Int,
  localAddress: String,
  remoteAddress: String)

case class ErrorResponse(
  readReply: ReadReply,
  err: String
)

class WritableMessageEncoder extends OneToOneEncoder {
  def encode(ctx: ChannelHandlerContext, channel: Channel, obj: Object) = {
    obj match {
      case message: WritableMessage[WritableOp] => {
        val buffer :ChannelBuffer = ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 1000)
        message writeTo buffer
        //println(java.util.Arrays.toString(buffer.toByteBuffer.array()))
        println("writing to buffer message " + message + " of length=" + buffer.array().length + ", => " + buffer.writerIndex)
        buffer
      }
      case _ => {
         println("WritableMessageEncoder: weird... " + obj)
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
  import java.net.InetSocketAddress

  def decode(ctx: ChannelHandlerContext, channel: Channel, obj: Object) = {
    val buffer = obj.asInstanceOf[ChannelBuffer]
    val header = MessageHeader(buffer)
    val reply = Reply(buffer)

    ReadReply(header, reply, buffer,
      ReadReplyInfo(channel.getId, channel.getLocalAddress.asInstanceOf[InetSocketAddress].toString, channel.getRemoteAddress.asInstanceOf[InetSocketAddress].toString))
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