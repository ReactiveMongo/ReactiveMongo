/*
 * Copyright 2012-2013 Stephane Godbillon (@sgodbillon) and Zenexity
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivemongo.core.protocol

import akka.actor.ActorRef
import java.nio.ByteOrder
import org.jboss.netty.buffer._
import org.jboss.netty.bootstrap._
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.handler.codec.oneone._
import org.jboss.netty.handler.codec.frame.FrameDecoder
import reactivemongo.core.actors.{ ChannelConnected, ChannelClosed, ChannelDisconnected }
import reactivemongo.core.commands.GetLastError
import reactivemongo.core.errors._
import reactivemongo.core.netty._
import reactivemongo.utils.LazyLogger
import BufferAccessors._
import reactivemongo.api.ReadPreference
import reactivemongo.api.collections.BufferReader

object `package` {
  implicit class RichBuffer(val buffer: ChannelBuffer) extends AnyVal {
    import scala.collection.mutable.ArrayBuffer
    /** Write a UTF-8 encoded C-Style String. */
    def writeCString(s: String): ChannelBuffer = {
      val bytes = s.getBytes("utf-8")
      buffer writeBytes bytes
      buffer writeByte 0
      buffer
    }

    /** Write a UTF-8 encoded String. */
    def writeString(s: String): ChannelBuffer = {
      val bytes = s.getBytes("utf-8")
      buffer writeInt (bytes.size + 1)
      buffer writeBytes bytes
      buffer writeByte 0
      buffer
    }

    /** Write the contents of the given [[reactivemongo.core.protocol.ChannelBufferWritable]]. */
    def write(writable: ChannelBufferWritable) {
      writable writeTo buffer
    }

    /** Reads a UTF-8 String. */
    def readString(): String = {
      val bytes = new Array[Byte](buffer.readInt - 1)
      buffer.readBytes(bytes)
      buffer.readByte
      new String(bytes, "UTF-8")
    }

    /**
     * Reads an array of Byte of the given length.
     *
     * @param length Length of the newly created array.
     */
    def readArray(length: Int): Array[Byte] = {
      val bytes = new Array[Byte](length)
      buffer.readBytes(bytes)
      bytes
    }

    /** Reads a UTF-8 C-Style String. */
    def readCString(): String = {
      @scala.annotation.tailrec
      def readCString(array: ArrayBuffer[Byte]): String = {
        val byte = buffer.readByte
        if (byte == 0x00)
          new String(array.toArray, "UTF-8")
        else readCString(array += byte)
      }
      readCString(new ArrayBuffer[Byte](16))
    }

  }
}

// traits
/**
 * Something that can be written into a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
 */
trait ChannelBufferWritable {
  /** Write this instance into the given [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]]. */
  def writeTo: ChannelBuffer => Unit
  /** Size of the content that would be written. */
  def size: Int
}

/**
 * A constructor of T instances from a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
 *
 * @tparam T type which instances can be constructed with this.
 */
trait ChannelBufferReadable[T] {
  /** Makes an instance of T from the data from the given buffer. */
  def readFrom(buffer: ChannelBuffer): T
  /** @see readFrom */
  def apply(buffer: ChannelBuffer): T = readFrom(buffer)
}

// concrete classes
/**
 * Header of a Mongo Wire Protocol message.
 *
 * @param messageLength length of this message.
 * @param requestID id of this request (> 0 for request operations, else 0).
 * @param responseTo id of the request that the message including this a response to (> 0 for reply operation, else 0).
 * @param opCode operation code of this message.
 */
case class MessageHeader(
    messageLength: Int,
    requestID: Int,
    responseTo: Int,
    opCode: Int) extends ChannelBufferWritable {
  override val writeTo = writeTupleToBuffer4((messageLength, requestID, responseTo, opCode)) _
  override def size = 4 + 4 + 4 + 4
}

/** Header deserializer from a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]]. */
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

/**
 * Request message.
 *
 * @param requestID id of this request, so that the response may be identifiable. Should be strictly positive.
 * @param op request operation.
 * @param documents body of this request, a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] containing 0, 1, or many documents.
 * @param channelIdHint a hint for sending this request on a particular channel.
 */
case class Request(
    requestID: Int,
    responseTo: Int, // TODO remove, nothing to do here.
    op: RequestOp,
    documents: BufferSequence,
    readPreference: ReadPreference = ReadPreference.primary,
    channelIdHint: Option[Int] = None) extends ChannelBufferWritable {
  override val writeTo = { buffer: ChannelBuffer =>
    buffer write header
    buffer write op
    buffer writeBytes documents.merged
  }
  override def size = 16 + op.size + documents.merged.writerIndex
  /** Header of this request */
  lazy val header = MessageHeader(size, requestID, responseTo, op.code)
}

/**
 * A helper to build write request which result needs to be checked (by sending a [[reactivemongo.core.commands.GetLastError]] command after).
 *
 * @param op write operation.
 * @param documents body of this request, a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] containing 0, 1, or many documents.
 * @param getLastError a [[reactivemongo.core.commands.GetLastError]] command message.
 */
case class CheckedWriteRequest(
    op: WriteRequestOp,
    documents: BufferSequence,
    getLastError: GetLastError) {
  def apply(): (RequestMaker, RequestMaker) = RequestMaker(op, documents) -> getLastError.apply(op.db).maker
}

/**
 * A helper to build requests.
 *
 * @param op write operation.
 * @param documents body of this request, a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] containing 0, 1, or many documents.
 * @param channelIdHint a hint for sending this request on a particular channel.
 */
case class RequestMaker(
    op: RequestOp,
    documents: BufferSequence = BufferSequence.empty,
    readPreference: ReadPreference = ReadPreference.primary,
    channelIdHint: Option[Int] = None) {
  def apply(id: Int) = Request(id, 0, op, documents, readPreference, channelIdHint)
}

/**
 * @define requestID id of this request, so that the response may be identifiable. Should be strictly positive.
 * @define op request operation.
 * @define documentsA body of this request, an Array containing 0, 1, or many documents.
 * @define documentsC body of this request, a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] containing 0, 1, or many documents.
 */
object Request {
  /**
   * Create a request.
   *
   * @param requestID $requestID
   * @param op $op
   * @param documents $documentsA
   */
  def apply(requestID: Int, responseTo: Int, op: RequestOp, documents: Array[Byte]): Request = {
    Request(
      requestID,
      responseTo,
      op,
      BufferSequence(ChannelBuffers.wrappedBuffer(ByteOrder.LITTLE_ENDIAN, documents)))
  }
  /**
   * Create a request.
   *
   * @param requestID $requestID
   * @param op $op
   * @param documents $documentsA
   */
  def apply(requestID: Int, op: RequestOp, documents: Array[Byte]): Request = Request.apply(requestID, 0, op, documents)
  /**
   * Create a request.
   *
   * @param requestID $requestID
   * @param op $op
   */
  def apply(requestID: Int, op: RequestOp): Request = Request.apply(requestID, op, new Array[Byte](0))
}

/**
 * A Mongo Wire Protocol Response messages.
 *
 * @param header header of this response.
 * @param reply the reply operation contained in this response.
 * @param documents body of this response, a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] containing 0, 1, or many documents.
 * @param info some meta information about this response, see [[reactivemongo.core.protocol.ResponseInfo]].
 */
case class Response(
    header: MessageHeader,
    reply: Reply,
    documents: ChannelBuffer,
    info: ResponseInfo) {
  /**
   * if this response is in error, explain this error.
   */
  lazy val error: Option[DatabaseException] = {
    if (reply.inError) {
      val bson = Response.parse(this)
      //val bson = ReplyDocumentIterator(reply, documents)
      if (bson.hasNext)
        Some(ReactiveMongoException(bson.next))
      else None
    } else None
  }
}

object Response {
  import reactivemongo.bson.BSONDocument
  import reactivemongo.bson.DefaultBSONHandlers.BSONDocumentIdentity
  import reactivemongo.api.collections.default.BSONDocumentReaderAsBufferReader
  def parse(response: Response): Iterator[BSONDocument] = ReplyDocumentIterator(response.reply, response.documents)(BSONDocumentReaderAsBufferReader(BSONDocumentIdentity))
}

/**
 * Response meta information.
 *
 * @param channelId the id of the channel that carried this response.
 * @param localAddress string representation of the local address of the channel
 * @param remoteAddress string representation of the remote address of the channel
 */
case class ResponseInfo(
  channelId: Int)

// protocol handlers for netty.
private[reactivemongo] class RequestEncoder extends OneToOneEncoder {
  import RequestEncoder._
  def encode(ctx: ChannelHandlerContext, channel: Channel, obj: Object) = {
    obj match {
      case message: Request => {
        val buffer: ChannelBuffer = ChannelBuffers.buffer(ByteOrder.LITTLE_ENDIAN, message.size) //ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 1000)
        message writeTo buffer
        buffer
      }
      case _ => {
        logger.error("weird... do not know how to encode this object: " + obj)
        obj
      }
    }
  }
}

private[reactivemongo] case class ReplyDocumentIterator[T](private val reply: Reply, private val buffer: ChannelBuffer)(implicit reader: BufferReader[T]) extends Iterator[T] {
  def hasNext = buffer.readable
  def next =
    try {
      reader.read(ChannelBufferReadableBuffer(buffer.readBytes(buffer.getInt(buffer.readerIndex))))
    } catch {
      case e: IndexOutOfBoundsException =>
        /*
         * If this happens, the buffer is exhausted, and there is probably a bug.
         * It may happen if an enumerator relying on it is concurrently applied to many iteratees – which should not be done!
         */
        throw new ReplyDocumentIteratorExhaustedException(e)
    }
}

case class ReplyDocumentIteratorExhaustedException(val cause: Exception) extends Exception(cause)

private[reactivemongo] object RequestEncoder {
  val logger = LazyLogger("reactivemongo.core.protocol.RequestEncoder")
}

private[reactivemongo] class ResponseFrameDecoder extends FrameDecoder {
  override def decode(context: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer) = {
    val readableBytes = buffer.readableBytes
    if (readableBytes < 4) null
    else {
      buffer.markReaderIndex
      val length = buffer.readInt
      buffer.resetReaderIndex
      if (length <= readableBytes && length > 0)
        buffer.readBytes(length)
      else null
    }
  }
}

private[reactivemongo] class ResponseDecoder extends OneToOneDecoder {
  import java.net.InetSocketAddress

  def decode(ctx: ChannelHandlerContext, channel: Channel, obj: Object) = {
    val buffer = obj.asInstanceOf[ChannelBuffer]
    val header = MessageHeader(buffer)
    val reply = Reply(buffer)

    Response(header, reply, buffer,
      ResponseInfo(channel.getId))
  }
}

private[reactivemongo] class MongoHandler(receiver: ActorRef) extends SimpleChannelHandler {
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
    receiver ! ChannelConnected(e.getChannel.getId)
    super.channelConnected(ctx, e)
  }
  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log(e, "disconnected")
    receiver ! ChannelDisconnected(e.getChannel.getId)
  }
  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log(e, "closed")
    receiver ! ChannelClosed(e.getChannel.getId)
  }
  override def exceptionCaught(ctx: org.jboss.netty.channel.ChannelHandlerContext, e: org.jboss.netty.channel.ExceptionEvent) {
    log(e, "CHANNEL ERROR: " + e.getCause)
  }
  def log(e: ChannelEvent, s: String) = logger.trace("(channel=" + e.getChannel.getId + ") " + s)
}

private[reactivemongo] object MongoHandler {
  private val logger = LazyLogger("reactivemongo.core.protocol.MongoHandler")
}