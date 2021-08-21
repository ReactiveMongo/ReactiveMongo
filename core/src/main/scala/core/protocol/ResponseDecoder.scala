package reactivemongo.core.protocol

import java.util.{ List => JList }

import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import reactivemongo.io.netty.buffer.{ ByteBuf, Unpooled }
import reactivemongo.io.netty.channel.{ ChannelHandlerContext, ChannelId }

import reactivemongo.api.Compressor
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.BSONSerializationPack

import reactivemongo.core.errors.DatabaseException

private[reactivemongo] class ResponseDecoder
  extends reactivemongo.io.netty.handler.codec.MessageToMessageDecoder[ByteBuf] {

  override def decode(
    context: ChannelHandlerContext,
    frame: ByteBuf, // see ResponseFrameDecoder
    out: JList[Object]): Unit = {

    out.add(decodeResponse(
      channelId = Option(context).map(_.channel.id),
      frame = frame))

    ()
  }

  private[reactivemongo] def decodeResponse(
    channelId: Option[ChannelId],
    frame: ByteBuf): Response = {

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
      case NonFatal(cause) =>
        frame.discardReadBytes()

        throw new IllegalStateException("Invalid message header", cause)
    }

    if (header.messageLength > readableBytes) {
      frame.discardReadBytes()

      throw new IllegalStateException(
        s"Invalid message length: ${header.messageLength} < ${readableBytes}")

    } else if (header.opCode != Reply.code && header.opCode != CompressedOp.code) {
      frame.discardReadBytes()

      throw new IllegalStateException(
        s"Unexpected opCode ${header.opCode} != ${Reply.code}")

    }

    if (header.opCode == CompressedOp.code) {
      decompress(channelId, frame, header)
    } else {
      decodeReply(channelId, frame, header)
    }
  }

  private[reactivemongo] def decompress(
    channelId: Option[ChannelId],
    frame: ByteBuf,
    header: MessageHeader): Response = {
    val originalOpCode = frame.readIntLE
    val uncompressedSize = frame.readIntLE
    val compressorId = frame.readUnsignedByte

    val uncompress: Function2[ByteBuf, ByteBuf, Try[Int]] = compressorId match {
      case Compressor.Zlib.id =>
        buffer.Zlib.DefaultCompressor.decode(_: ByteBuf, _: ByteBuf)

      case Compressor.Zstd.id =>
        buffer.Zstd.DefaultCompressor.decode(_: ByteBuf, _: ByteBuf)

      case Compressor.Snappy.id =>
        buffer.Snappy.DefaultCompressor.decode(_: ByteBuf, _: ByteBuf)

      case id =>
        throw new IllegalStateException(
          s"Unexpected compressor ID: ${id}")
    }

    val newHeader = header.copy(
      messageLength = uncompressedSize,
      opCode = originalOpCode)

    val newFrame = Unpooled.directBuffer(uncompressedSize)

    uncompress(frame, newFrame) match {
      case Failure(cause) =>
        throw cause

      case _ =>
    }

    frame.release()

    decodeReply(channelId, newFrame, newHeader)
  }

  private def decodeReply(
    channelId: Option[ChannelId],
    frame: ByteBuf,
    header: MessageHeader): Response = {

    val reply = Reply(frame)
    val chanId = channelId.orNull
    def info = new ResponseInfo(chanId)

    def response = if (reply.cursorID == 0 && reply.numberReturned > 0) {
      // Copy as unpooled (non-derived) buffer
      val docs = Unpooled.buffer(frame.readableBytes)

      docs.writeBytes(frame)

      ResponseDecoder.first(docs) match {
        case Failure(cause) =>
          Response.CommandError(header, reply, info, DatabaseException(cause))

        case Success(doc) => {
          val ok = doc.booleanLike("ok") getOrElse false

          def failed = {
            val r = {
              if (reply.inError) reply
              else reply.copy(flags = reply.flags | 0x02)
            }

            Response.CommandError(
              header, r, info, DatabaseException(BSONSerializationPack)(doc))
          }

          doc.getAsOpt[BSONDocument]("cursor") match {
            case Some(cursor) if ok => {
              val withCursor: Option[Response] = for {
                id <- cursor.long("id")
                ns <- cursor.string("ns")

                batch <- cursor.getAsOpt[Seq[BSONDocument]]("firstBatch").
                  orElse(cursor.getAsOpt[Seq[BSONDocument]]("nextBatch"))
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

              if (ok) {
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

    response
  }
}

private[reactivemongo] object ResponseDecoder {
  @inline def first(buf: ByteBuf) = Try[BSONDocument] {
    val sz = buf.getIntLE(buf.readerIndex)
    val bytes = Array.ofDim[Byte](sz)

    // Avoid .readBytes(sz) which internally allocate a ByteBuf
    // (which would require to manage its release)
    buf.readBytes(bytes)

    val docBuf = reactivemongo.api.bson.buffer.ReadableBuffer(bytes)

    reactivemongo.api.bson.collection.
      BSONSerializationPack.readFromBuffer(docBuf)
  }
}
