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
import org.asyncmongo.actors.{Connected, Disconnected}
import org.asyncmongo.protocol.commands.GetLastError
import org.slf4j.{Logger, LoggerFactory}

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

case class Request (
  requestID: Int,
  responseTo: Int,
  op: RequestOp,
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

case class CheckedWriteRequest(
  op: WriteRequestOp,
  documents: ChannelBuffer,
  getLastError: GetLastError
) {
  def apply() :(RequestMaker, RequestMaker) = RequestMaker(op, documents, None) -> getLastError.apply(op.db).maker
}

case class RequestMaker(
  op: RequestOp,
  documents: ChannelBuffer,
  channelIdHint: Option[Int]
) {
  def apply(id: Int) = Request(id, 0, op, documents, None)
}

object RequestMaker {
  def apply(op: RequestOp) :RequestMaker = RequestMaker(op, new LittleEndianHeapChannelBuffer(0), None)
  def apply(op: RequestOp, buffer: ChannelBuffer) :RequestMaker = RequestMaker(op, buffer, None)
}

object Request{
  def apply(requestID: Int, responseTo: Int, op: RequestOp, documents: Array[Byte]) :Request = {
    Request(
      requestID,
      responseTo,
      op,
      ChannelBuffers.wrappedBuffer(ByteOrder.LITTLE_ENDIAN, documents))
  }
  def apply(requestID: Int, op: RequestOp, documents: ChannelBuffer) :Request = Request.apply(requestID, 0, op, documents)
  def apply(requestID: Int, op: RequestOp, documents: Array[Byte]) :Request = Request.apply(requestID, 0, op, documents)
  def apply(requestID: Int, op: RequestOp) :Request = Request.apply(requestID, op, new Array[Byte](0))
}

case class Response(
  header: MessageHeader,
  reply: Reply,
  documents: ChannelBuffer,
  info: ResponseInfo) {

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

case class ResponseInfo(
  channelId: Int,
  localAddress: String,
  remoteAddress: String)

case class ErrorResponse(
  readReply: Response,
  err: String
)

class RequestEncoder extends OneToOneEncoder {
  import RequestEncoder._
  def encode(ctx: ChannelHandlerContext, channel: Channel, obj: Object) = {
    obj match {
      case message: Request => {
        val buffer :ChannelBuffer = ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 1000)
        message writeTo buffer
        logger.trace("writing to buffer message " + message + " of length=" + buffer.array().length + ", => " + buffer.writerIndex)
        buffer
      }
      case _ => {
         logger.error("RequestEncoder: weird... do not know how to encode this object: " + obj)
         obj
      }
    }
  }
}

object RequestEncoder {
  val logger = LoggerFactory.getLogger("protocol/RequestEncoder")
}

class ResponseFrameDecoder extends FrameDecoder {
  override def decode(context: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer) = {
    val readableBytes = buffer.readableBytes
    if(readableBytes < 4) null
    else {
      buffer.markReaderIndex
      val length = buffer.readInt
      buffer.resetReaderIndex
      if(length <= readableBytes && length > 0)
        buffer.readBytes(length)
      else null
    }
  }
}

class ResponseDecoder extends OneToOneDecoder {
  import java.net.InetSocketAddress

  def decode(ctx: ChannelHandlerContext, channel: Channel, obj: Object) = {
    val buffer = obj.asInstanceOf[ChannelBuffer]
    val header = MessageHeader(buffer)
    val reply = Reply(buffer)

    Response(header, reply, buffer,
      ResponseInfo(channel.getId, channel.getLocalAddress.asInstanceOf[InetSocketAddress].toString, channel.getRemoteAddress.asInstanceOf[InetSocketAddress].toString))
  }
}

class MongoHandler(receiver: ActorRef) extends SimpleChannelHandler {
  import MongoHandler._
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val response = e.getMessage.asInstanceOf[Response]
    log(e, "messageReceived " + response + " will be send to " + receiver)
    receiver ! response
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
    receiver ! Connected(e.getChannel.getId)
    super.channelConnected(ctx, e)
  }
  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log(e, "disconnected")
    receiver ! Disconnected(e.getChannel.getId)
  }
  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log(e, "closed")
    receiver ! Disconnected(e.getChannel.getId)
  }
  override def exceptionCaught(ctx: org.jboss.netty.channel.ChannelHandlerContext, e: org.jboss.netty.channel.ExceptionEvent) {
    log(e, "CHANNEL ERROR: " + e.getCause)
  }
  def log(e: ChannelEvent, s: String) = logger.trace("(channel=" + e.getChannel.getId + ") " + s)
}

object MongoHandler {
  private val logger = LoggerFactory.getLogger("protocol/MongoHandler")
}

sealed trait NodeState
sealed trait MongoNodeState {
  val code: Int
}

object NodeState {
  def apply(i: Int) :NodeState = i match {
    case 1 => PRIMARY
    case 2 => SECONDARY
    case 3 => RECOVERING
    case 4 => FATAL
    case 5 => STARTING
    case 6 => UNKNOWN
    case 7 => ARBITER
    case 8 => DOWN
    case 9 => ROLLBACK
    case _ => NONE
  }

  case object PRIMARY    extends NodeState with MongoNodeState { override val code = 1 } // Primary
  case object SECONDARY  extends NodeState with MongoNodeState { override val code = 2 } // Secondary
  case object RECOVERING extends NodeState with MongoNodeState { override val code = 3 } // Recovering (initial syncing, post-rollback, stale members)
  case object FATAL      extends NodeState with MongoNodeState { override val code = 4 } // Fatal error
  case object STARTING   extends NodeState with MongoNodeState { override val code = 5 } // Starting up, phase 2 (forking threads)
  case object UNKNOWN    extends NodeState with MongoNodeState { override val code = 6 } // Unknown state (member has never been reached)
  case object ARBITER    extends NodeState with MongoNodeState { override val code = 7 } // Arbiter
  case object DOWN       extends NodeState with MongoNodeState { override val code = 8 } // Down
  case object ROLLBACK   extends NodeState with MongoNodeState { override val code = 9 } // Rollback
  case object NONE       extends NodeState
  case object NOT_CONNECTED extends NodeState
  case object CONNECTED extends NodeState
}

sealed trait ChannelState
object ChannelState {
  case object Closed extends ChannelState
  case object NotConnected extends ChannelState
  case object Useable extends ChannelState
  case class Authenticating(db: String, user: String, password: String, nonce: Option[String]) extends ChannelState
}