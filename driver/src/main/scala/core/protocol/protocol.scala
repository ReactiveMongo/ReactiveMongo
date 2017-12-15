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

import java.util.{ List => JList }

import shaded.netty.buffer.{ ByteBuf, Unpooled }
import shaded.netty.channel.{ ChannelHandlerContext, ChannelId }

import reactivemongo.api.SerializationPack
import reactivemongo.api.commands.GetLastError

import reactivemongo.core.netty.{ BufferSequence, ChannelBufferReadableBuffer }

import reactivemongo.api.ReadPreference

// traits
/**
 * Something that can be written into a [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]].
 */
trait ChannelBufferWritable {
  /** Write this instance into the given [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]]. */
  def writeTo: ByteBuf => Unit

  /** Size of the content that would be written. */
  def size: Int
}

/**
 * A constructor of T instances from a [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]].
 *
 * @tparam T type which instances can be constructed with this.
 */
trait ChannelBufferReadable[T] {
  /** Makes an instance of T from the data from the given buffer. */
  def readFrom(buffer: ByteBuf): T

  /** @see readFrom */
  def apply(buffer: ByteBuf): T = readFrom(buffer)
}

// concrete classes

/**
 * A helper to build write request which result needs to be checked (by sending a [[reactivemongo.core.commands.GetLastError]] command after).
 *
 * @param op write operation.
 * @param documents body of this request, a [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]] containing 0, 1, or many documents.
 * @param getLastError a [[reactivemongo.core.commands.GetLastError]] command message.
 */
case class CheckedWriteRequest(
  op: WriteRequestOp,
  documents: BufferSequence,
  getLastError: GetLastError) {
  def apply(): (RequestMaker, RequestMaker) = {
    import reactivemongo.api.BSONSerializationPack
    import reactivemongo.api.commands.Command
    import reactivemongo.api.commands.bson.BSONGetLastErrorImplicits.GetLastErrorWriter
    val gleRequestMaker = Command.requestMaker(BSONSerializationPack).
      onDatabase(op.db, getLastError, ReadPreference.primary)(
        GetLastErrorWriter).requestMaker

    RequestMaker(op, documents) -> gleRequestMaker
  }
}

/**
 * A helper to build requests.
 *
 * @param op write operation.
 * @param documents body of this request, a [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]] containing 0, 1, or many documents.
 * @param channelIdHint a hint for sending this request on a particular channel.
 */
case class RequestMaker(
  op: RequestOp,
  documents: BufferSequence = BufferSequence.empty,
  readPreference: ReadPreference = ReadPreference.primary,
  channelIdHint: Option[ChannelId] = None) {

  def apply(@deprecatedName('id) requestID: Int) = Request(
    requestID, 0, op, documents, readPreference, channelIdHint)
}

// protocol handlers for netty.
private[reactivemongo] class RequestEncoder
  extends shaded.netty.handler.codec.MessageToByteEncoder[Request] {
  def encode(
    ctx: ChannelHandlerContext,
    message: Request,
    buffer: ByteBuf) {

    /* DEBUG
    val buf = buffer.duplicate()
    message writeTo buf
    buf.resetReaderIndex()
    val bytes = Array.ofDim[Byte](buf.readableBytes)
    buf.getBytes(0, bytes)
    println(s"$message ---> ${bytes.toList}")
     */

    //println(s"encode#${System identityHashCode message}: ${System identityHashCode buffer}")

    message writeTo buffer

    ()
  }
}

/**
 * A Mongo Wire Protocol Response messages.
 *
 * @param header the header of this response
 * @param reply the reply operation contained in this response
 * @param documents the body of this response, a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] containing 0, 1, or many documents
 * @param info some meta information about this response, see [[reactivemongo.core.protocol.ResponseInfo]]
 */
sealed abstract class Response(
  val header: MessageHeader,
  val reply: Reply,
  val documents: ByteBuf,
  val info: ResponseInfo) extends Product4[MessageHeader, Reply, ChannelBuffer, ResponseInfo] with Serializable {
  @inline def _1 = header
  @inline def _2 = reply
  @inline def _3 = documents
  @inline def _4 = info

  def canEqual(that: Any): Boolean = that match {
    case _: Response => true
    case _           => false
  }

  /** If this response is in error, explain this error. */
  lazy val error: Option[DatabaseException] = {
    if (reply.inError) {
      val bson = Response.parse(this)

      if (bson.hasNext) Some(DatabaseException(bson.next))
      else None
    } else None
  }

  private[reactivemongo] def cursorID(id: Long): Response

  private[reactivemongo] def startingFrom(offset: Int): Response

  override def toString = s"Response($header, $reply, $info)"
}

object Response {
  import reactivemongo.api.BSONSerializationPack
  import reactivemongo.bson.BSONDocument
  import reactivemongo.bson.DefaultBSONHandlers.BSONDocumentIdentity

  def apply(
    header: MessageHeader,
    reply: Reply,
    documents: ByteBuf,
    info: ResponseInfo): Response = Successful(header, reply, documents, info)

  def parse(response: Response): Iterator[BSONDocument] = parse[BSONSerializationPack.type, BSONDocument](BSONSerializationPack)(response, BSONDocumentIdentity)

  @inline private[reactivemongo] def parse[P <: SerializationPack, T](
    pack: P)(response: Response, reader: pack.Reader[T]): Iterator[T] =
    ReplyDocumentIterator.parse(pack)(response)(reader)

  def unapply(response: Response): Option[(MessageHeader, Reply, ChannelBuffer, ResponseInfo)] = Some((response.header, response.reply, response.documents, response.info))

  // ---

  private[reactivemongo] final case class Successful(
    _header: MessageHeader,
    _reply: Reply,
    _documents: ByteBuf,
    _info: ResponseInfo) extends Response(
    _header, _reply, _documents, _info) {

    private[reactivemongo] def cursorID(id: Long): Response =
      copy(_reply = this._reply.copy(cursorID = id))

    private[reactivemongo] def startingFrom(offset: Int): Response =
      copy(_reply = this._reply.copy(startingFrom = offset))
  }

  // For MongoDB 3.2+ response with cursor
  private[reactivemongo] final case class WithCursor(
    _header: MessageHeader,
    _reply: Reply,
    _documents: ByteBuf,
    _info: ResponseInfo,
    ns: String,
    private[core]preloaded: Seq[BSONDocument]) extends Response(
    _header, _reply, _documents, _info) {
    private[reactivemongo] def cursorID(id: Long): Response =
      copy(_reply = this._reply.copy(cursorID = id))

    private[reactivemongo] def startingFrom(offset: Int): Response =
      copy(_reply = this._reply.copy(startingFrom = offset))
  }

  private[reactivemongo] final case class CommandError(
    _header: MessageHeader,
    _reply: Reply,
    _info: ResponseInfo,
    private[reactivemongo]cause: DatabaseException) extends Response(_header, _reply, ChannelBuffers.EMPTY_BUFFER, _info) {
    override lazy val error: Option[DatabaseException] = Some(cause)

    private[reactivemongo] def cursorID(id: Long): Response =
      copy(_reply = this._reply.copy(cursorID = id))

    private[reactivemongo] def startingFrom(offset: Int): Response =
      copy(_reply = this._reply.copy(startingFrom = offset))
  }
}

/**
 * Response meta information.
 *
 * @param channelId the id of the channel that carried this response.
 */
case class ResponseInfo(channelId: Int)

// protocol handlers for netty.
private[reactivemongo] class RequestEncoder extends OneToOneEncoder {
  import RequestEncoder._
  def encode(ctx: ChannelHandlerContext, channel: Channel, obj: Object) =
    obj match {
      case message: Request => {
        val buffer: ChannelBuffer = ChannelBuffers.buffer(ByteOrder.LITTLE_ENDIAN, message.size) //ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 1000)
        message writeTo buffer
        buffer
      }

      case _ => {
        logger.error(s"Weird... do not know how to encode this object: $obj")
        obj
      }
    }

}

object ReplyDocumentIterator {
  private[reactivemongo] def parse[P <: SerializationPack, A](pack: P)(response: Response)(implicit reader: pack.Reader[A]): Iterator[A] = response match {
    case Response.CommandError(_, _, _, cause) => new Iterator[A] {
      val hasNext = false
      @inline def next: A = throw cause
      //throw ReplyDocumentIteratorExhaustedException(cause)
    }

    case Response.WithCursor(_, _, _, _, _, preloaded) => {
      val buf = response.documents

      if (buf.readableBytes == 0) {
        Iterator.empty
      } else {
        try {
          buf.skipBytes(buf.getInt(buf.readerIndex))

          def docs = apply[P, A](pack)(response.reply, buf)

          val firstBatch = preloaded.iterator.map { bson =>
            pack.deserialize(pack.document(bson), reader)
          }

          firstBatch ++ docs
        } catch {
          case cause: Exception => new Iterator[A] {
            val hasNext = false
            @inline def next: A = throw cause
            //throw ReplyDocumentIteratorExhaustedException(cause)
          }
        }
      }
    }

    case _ => apply[P, A](pack)(response.reply, response.documents)
  }

  // TODO: Deprecated the `reply` parameter
  def apply[P <: SerializationPack, A](pack: P)(reply: Reply, buffer: ByteBuf)(implicit reader: pack.Reader[A]): Iterator[A] = new Iterator[A] {
    override val isTraversableAgain = false // TODO: Add test
    override def hasNext = buffer.isReadable()

    override def next = try {
      val sz = buffer.getIntLE(buffer.readerIndex)
      //val cbrb = ChannelBufferReadableBuffer(buffer readBytes sz)
      val cbrb = ChannelBufferReadableBuffer(buffer readSlice sz)

      pack.readAndDeserialize(cbrb, reader)
    } catch {
      case e: IndexOutOfBoundsException =>
        /*
         * If this happens, the buffer is exhausted, and there is probably a bug.
         * It may happen if an enumerator relying on it is concurrently applied to many iteratees – which should not be done!
         */
        throw ReplyDocumentIteratorExhaustedException(e)
    }
  }
}

case class ReplyDocumentIteratorExhaustedException(
  val cause: Exception) extends Exception(cause)

private[reactivemongo] object Debug {
  val ParentBuf = shaded.netty.util.AttributeKey.
    newInstance[Int]("parentBuf")
}

/**
 * Either waits Netty receives the sufficient amount of bytes
 * to read at least one frame (MongoDB message), or split byte buffer
 * if it contains more than one frame.
 */
private[reactivemongo] class ResponseFrameDecoder
  extends shaded.netty.handler.codec.ByteToMessageDecoder {

  //private val rand = new scala.util.Random(System identityHashCode this)

  override def decode(
    context: ChannelHandlerContext,
    buffer: ByteBuf,
    out: JList[Object]): Unit = {

    //val id = rand.nextInt()
    //println(s"_before  #${id}: ${buffer.refCnt}")

    /*
    if (buffer.refCnt == 0) { // recycled buffer
      buffer.retain()
    }
     */

    frames(buffer, buffer.readableBytes, out)

    /*
    context.attr(Debug.ParentBuf).set(id)

    println(s"_after   #${id}: ${out.size} => ${buffer.refCnt}")
     */

    ()
    /*
  } catch {
    case cause: Throwable =>
      println(s"$cause : ${buffer.readableBytes}")
      cause.printStackTrace()
      throw cause
     */
  }

  @annotation.tailrec
  private def frames(buffer: ByteBuf, readableBytes: Int, out: JList[Object]) {
    if (readableBytes > 0) {
      buffer.markReaderIndex()

      //println("_frames_1")

      // MessageHeader.messageLength (including the int32 size of messageLength)
      val messageLength = buffer.readIntLE()

      buffer.resetReaderIndex()

      if (messageLength <= readableBytes) {
        //out.add(buffer readBytes messageLength)

        val frame = buffer.readRetainedSlice(messageLength)
        // #frm1: .. as need to be kept up to the [[ResponseDecoder]]

        //println(s"_frame   #${id}: ${buffer.refCnt} (${frame.refCnt}) <- ${buffer.readerIndex}")

        out.add(frame)

        frames(buffer, readableBytes - messageLength, out)
      }
    }
  }

  // ---

  @inline private def document(buf: ByteBuf) = Try[BSONDocument] {
    val docBuf = ChannelBufferReadableBuffer(
      buf readBytes buf.getInt(buf.readerIndex))

    reactivemongo.api.BSONSerializationPack.readFromBuffer(docBuf)
  }
}

private[reactivemongo] class ResponseDecoder
  extends shaded.netty.handler.codec.MessageToMessageDecoder[ByteBuf] {

  override def decode(
    context: ChannelHandlerContext,
    frame: ByteBuf, // see ResponseFrameDecoder
    out: JList[Object]): Unit = {

    /*
    try {
      println(s"_decode2 #${context.attr(Debug.ParentBuf).get}: ${frame.refCnt}")
    } catch {
      case cause: Throwable =>
        cause.printStackTrace()
    }
     */

    /*
    val attr =
      context.attr(shaded.netty.util.AttributeKey.
        newInstance[Int]("parentBuf")).get

    println(s"_decode2${attr}: ${frame.refCnt}")
     */

    val readableBytes = frame.readableBytes

    if (readableBytes < MessageHeader.size) {
      //frame.readBytes(readableBytes) // discard
      frame.discardReadBytes()

      throw new IllegalStateException(
        s"Invalid message size: $readableBytes < ${MessageHeader.size}")
    }

    // ---

    //println("_decode2b")

    val header: MessageHeader = try {
      MessageHeader(frame)
    } catch {
      case cause: Throwable =>
        //println("_cause_1")

        frame.discardReadBytes()
        //frame.readBytes(frame.readableBytes)

        throw new IllegalStateException("Invalid message header", cause)
    }

    if (header.messageLength > readableBytes) {
      frame.discardReadBytes()
      //frame.readBytes(readableBytes - header.size)

      throw new IllegalStateException(
        s"Invalid message length: ${header.messageLength} < ${readableBytes}")

    } else if (header.opCode != Reply.code) {
      frame.discardReadBytes()
      //frame.readBytes(readableBytes - header.size)

      throw new IllegalStateException(
        s"Unexpected opCode ${header.opCode} != ${Reply.code}")

    }

    // ---

    //println(s"_before_reply: ${frame.refCnt}")

    val reply = Reply(frame)
    val chanId = Option(context).map(_.channel.id).orNull

    //val docs = frame.copy() // 'detach' as frame is reused by networking
    val docs: ByteBuf = {
      // Copy as unpooled (non-derived) buffer
      val buf = Unpooled.buffer(frame.readableBytes)

      buf.writeBytes(frame)

      buf
    }

    //println(s"_here: ${frame.refCnt}")

    out.add(Response(header, reply, docs, ResponseInfo(chanId)))

    frame.release() // X

    ()
  }
}
