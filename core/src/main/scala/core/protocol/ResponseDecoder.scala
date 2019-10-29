package reactivemongo.core.protocol

import java.util.{ List => JList }

import scala.util.{ Failure, Success, Try }

import reactivemongo.io.netty.buffer.{ ByteBuf, Unpooled }
import reactivemongo.io.netty.channel.ChannelHandlerContext

import reactivemongo.bson.{ BSONBooleanLike, BSONDocument, BSONNumberLike }

import reactivemongo.core.netty.ChannelBufferReadableBuffer
import reactivemongo.core.errors.DatabaseException

private[reactivemongo] class ResponseDecoder
  extends reactivemongo.io.netty.handler.codec.MessageToMessageDecoder[ByteBuf] {

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

      ResponseDecoder.first(docs) match {
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
              val withCursor: Option[Response] = for {
                id <- cursor.getAs[BSONNumberLike]("id").map(_.toLong)
                ns <- cursor.getAs[String]("ns")
                batch <- cursor.getAs[Seq[BSONDocument]]("firstBatch").orElse(
                  cursor.getAs[Seq[BSONDocument]]("nextBatch"))
              } yield {
                val r = reply.copy(cursorID = id, numberReturned = batch.size)

                Response.WithCursor(header, r, docs, info, ns, cursor, batch)
              }

              docs.resetReaderIndex()

              withCursor getOrElse Response(header, reply, docs, info)
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

      Response(header, reply, docs, info)
    } else {
      frame.discardReadBytes()

      Response(header, reply, Unpooled.EMPTY_BUFFER, info)
    }

    out.add(response)

    ()
  }
}

private[reactivemongo] object ResponseDecoder {
  @inline private[reactivemongo] def first(buf: ByteBuf) = Try[BSONDocument] {
    val sz = buf.getIntLE(buf.readerIndex)
    val bytes = Array.ofDim[Byte](sz)

    // Avoid .readBytes(sz) which internally allocate a ByteBuf
    // (which would require to manage its release)
    buf.readBytes(bytes)

    val docBuf = ChannelBufferReadableBuffer(Unpooled wrappedBuffer bytes)

    reactivemongo.api.BSONSerializationPack.readFromBuffer(docBuf)
  }
}
