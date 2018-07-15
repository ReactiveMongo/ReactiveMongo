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

import reactivemongo.io.netty.buffer.{ ByteBuf, Unpooled }
import reactivemongo.io.netty.channel.{ ChannelHandlerContext, ChannelId }

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
  extends reactivemongo.io.netty.handler.codec.MessageToByteEncoder[Request] {
  def encode(
    ctx: ChannelHandlerContext,
    message: Request,
    buffer: ByteBuf): Unit = {

    message writeTo buffer

    ()
  }
}

object ReplyDocumentIterator {
  private[reactivemongo] def parse[P <: SerializationPack, A](pack: P)(response: Response)(implicit reader: pack.Reader[A]): Iterator[A] = response match {
    case Response.CommandError(_, _, _, cause) =>
      new Iterator[A] {
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
          buf.skipBytes(buf.getIntLE(buf.readerIndex))

          def docs = parseDocuments[P, A](pack)(buf)

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

    case _ => parseDocuments[P, A](pack)(response.documents)
  }

  @deprecated("Use `parseDocuments`", "0.14.0")
  def apply[P <: SerializationPack, A](pack: P)(reply: Reply, buffer: ByteBuf)(implicit reader: pack.Reader[A]): Iterator[A] = parseDocuments[P, A](pack)(buffer)

  private[core] def parseDocuments[P <: SerializationPack, A](pack: P)(buffer: ByteBuf)(implicit reader: pack.Reader[A]): Iterator[A] = new Iterator[A] {
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
  val ParentBuf = reactivemongo.io.netty.util.AttributeKey.
    newInstance[Int]("parentBuf")
}

/**
 * Either waits Netty receives the sufficient amount of bytes
 * to read at least one frame (MongoDB message), or split byte buffer
 * if it contains more than one frame.
 */
private[reactivemongo] class ResponseFrameDecoder
  extends reactivemongo.io.netty.handler.codec.ByteToMessageDecoder {

  //private val rand = new scala.util.Random(System identityHashCode this)

  override def decode(
    context: ChannelHandlerContext,
    buffer: ByteBuf,
    out: JList[Object]): Unit = {

    frames(buffer, buffer.readableBytes, out)

    ()
  }

  @annotation.tailrec
  private def frames(buffer: ByteBuf, readableBytes: Int, out: JList[Object]): Unit = {
    if (readableBytes > 0) {
      buffer.markReaderIndex()

      // MessageHeader.messageLength (including the int32 size of messageLength)
      val messageLength = buffer.readIntLE()

      buffer.resetReaderIndex()

      if (messageLength <= readableBytes) {
        val frame = buffer.readRetainedSlice(messageLength)
        // #frm1: .. as need to be kept up to the [[ResponseDecoder]]

        out.add(frame)

        frames(buffer, readableBytes - messageLength, out)
      }
    }
  }
}

private[reactivemongo] class ResponseDecoder
  extends reactivemongo.io.netty.handler.codec.MessageToMessageDecoder[ByteBuf] {

  import scala.util.{ Failure, Success, Try }
  import reactivemongo.core.errors.DatabaseException
  import reactivemongo.bson.{
    BSONBooleanLike,
    BSONDocument,
    BSONNumberLike
  }

  override def decode(
    context: ChannelHandlerContext,
    frame: ByteBuf, // see ResponseFrameDecoder
    out: JList[Object]): Unit = {

    val readableBytes = frame.readableBytes

    if (readableBytes < MessageHeader.size) {
      //frame.readBytes(readableBytes) // discard
      frame.discardReadBytes()

      throw new IllegalStateException(
        s"Invalid message size: $readableBytes < ${MessageHeader.size}")
    }

    // ---

    val header: MessageHeader = try {
      MessageHeader(frame)
    } catch {
      case cause: Throwable =>
        frame.discardReadBytes()

        throw new IllegalStateException("Invalid message header", cause)
    }

    if (header.messageLength > readableBytes) {
      frame.discardReadBytes()

      throw new IllegalStateException(
        s"Invalid message length: ${header.messageLength} < ${readableBytes}")

    } else if (header.opCode != Reply.code) {
      frame.discardReadBytes()

      throw new IllegalStateException(
        s"Unexpected opCode ${header.opCode} != ${Reply.code}")

    }

    // ---

    val reply = Reply(frame)
    val chanId = Option(context).map(_.channel.id).orNull
    def info = ResponseInfo(chanId)

    def response = if (reply.cursorID == 0 && reply.numberReturned > 0) {
      // Copy as unpooled (non-derived) buffer
      val docs = Unpooled.buffer(frame.readableBytes)

      docs.writeBytes(frame)
      frame.release()

      first(docs) match {
        case Failure(cause) => {
          //cause.printStackTrace()
          Response.CommandError(header, reply, info, DatabaseException(cause))
        }

        case Success(doc) => {
          val ok = doc.getAs[BSONBooleanLike]("ok")
          def failed = {
            val r = {
              if (reply.inError) reply
              else reply.copy(flags = reply.flags | 0x02)
            }

            Response.CommandError(header, r, info, DatabaseException(doc))
          }

          doc.getAs[BSONDocument]("cursor") match {
            case Some(cursor) if ok.exists(_.toBoolean) => {
              val ry = for {
                id <- cursor.getAs[BSONNumberLike]("id").map(_.toLong)
                ns <- cursor.getAs[String]("ns")
                batch <- cursor.getAs[Seq[BSONDocument]]("firstBatch").orElse(
                  cursor.getAs[Seq[BSONDocument]]("nextBatch"))

              } yield (ns, batch, reply.copy(
                cursorID = id,
                numberReturned = batch.size))

              docs.resetReaderIndex()

              ry.fold(Response(header, reply, docs, info)) {
                case (ns, batch, r) => Response.WithCursor(
                  header, r, docs, info, ns, batch)
              }
            }

            case Some(_) => failed

            case _ => {
              docs.resetReaderIndex()

              if (ok.forall(_.toBoolean)) {
                Response(header, reply, docs, info)
              } else { // !ok
                failed
              }
            }
          }
        }
      }
    } else if (reply.numberReturned > 0) {
      // Copy as unpooled (non-derived) buffer
      val docs = Unpooled.buffer(frame.readableBytes)

      docs.writeBytes(frame)
      frame.release()

      Response(header, reply, docs, info)
    } else {
      frame.discardReadBytes()

      Response(header, reply, Unpooled.EMPTY_BUFFER, info)
    }

    out.add(response)

    ()
  }

  @inline private def first(buf: ByteBuf) =
    Try[reactivemongo.bson.BSONDocument] {
      val sz = buf.getIntLE(buf.readerIndex)
      val bytes = Array.ofDim[Byte](sz)

      // Avoid .readBytes(sz) which internally allocate a ByteBuf
      // (which would require to manage its release)
      buf.readBytes(bytes)

      val docBuf = ChannelBufferReadableBuffer(Unpooled wrappedBuffer bytes)

      reactivemongo.api.BSONSerializationPack.readFromBuffer(docBuf)
    }
}
