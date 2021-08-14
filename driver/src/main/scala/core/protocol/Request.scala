package reactivemongo.core.protocol

import scala.util.{ Success, Try }

import reactivemongo.io.netty.buffer.{ ByteBuf, Unpooled }
import reactivemongo.io.netty.channel.ChannelId

import reactivemongo.core.netty.BufferSequence

import reactivemongo.core.protocol.buffer.ChannelBufferWritable

import reactivemongo.api.{ Compressor, ReadPreference }

/**
 * Request message.
 *
 * @param requestID the ID of this request, so that the response may be identifiable. Should be strictly positive.
 * @param op request operation.
 * @param channelIdHint a hint for sending this request on a particular channel.
 */
private[reactivemongo] final class Request private (
  val requestID: Int,
  val responseTo: Int,
  val op: RequestOp,
  val payload: ByteBuf,
  val readPreference: ReadPreference = ReadPreference.primary,
  val channelIdHint: Option[ChannelId] = None) extends ChannelBufferWritable {

  private val payloadSize = payload.writerIndex

  val writeTo: ByteBuf => Unit = { buffer =>
    header writeTo buffer
    op writeTo buffer

    buffer writeBytes payload

    ()
  }

  def size = /* MsgHeader: */ 16 + op.size + payloadSize

  /** Header of this request */
  lazy val header = new MessageHeader(size, requestID, responseTo, op.code)

  override def toString =
    s"Request($requestID, $responseTo, $op, $readPreference, $channelIdHint)"
}

/**
 * @define requestID id of this request, so that the response may be identifiable. Should be strictly positive.
 * @define op request operation.
 * @define documentsA body of this request, an Array containing 0, 1, or many documents.
 * @define documentsC body of this request, a [[http://netty.io/4.0/api/io/netty/buffer/ByteBuf.html ByteBuf]] containing 0, 1, or many documents.
 */
private[reactivemongo] object Request {
  /**
   * Create a request.
   *
   * @param requestID $requestID
   * @param op $op
   * @param documents body of this request, a [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]] containing 0, 1, or many documents.
   */
  def apply(
    requestID: Int,
    responseTo: Int,
    op: RequestOp,
    documents: BufferSequence,
    readPreference: ReadPreference = ReadPreference.primary,
    channelIdHint: Option[ChannelId] = None): Request = new Request(
    requestID, responseTo, op, documents.merged, readPreference, channelIdHint)

  /**
   * Create a request.
   *
   * @param requestID $requestID
   * @param op $op
   * @param documents $documentsA
   */
  def apply(
    requestID: Int,
    responseTo: Int,
    op: RequestOp,
    documents: Array[Byte]): Request = new Request(
    requestID,
    responseTo,
    op,
    Unpooled.wrappedBuffer(documents))

  /**
   * Create a request.
   *
   * @param requestID $requestID
   * @param op $op
   * @param documents $documentsA
   */
  def apply(requestID: Int, op: RequestOp, documents: Array[Byte]): Request =
    Request.apply(requestID, 0, op, documents)

  /**
   * Create a request.
   *
   * @param requestID $requestID
   * @param op $op
   */
  def apply(requestID: Int, op: RequestOp): Request =
    Request.apply(requestID, op, new Array[Byte](0))

  /**
   * @param req the request to be compressed
   * @param compressor the compressor to be applied (if required)
   */
  def compress(req: Request, compressor: Compressor): Try[Request] = {
    import req.op.{ code => originalOpCode }

    if (originalOpCode == CompressedOp.code) {
      // Already compressed
      Success(req)
    } else {
      val uncompressedSize = req.size - 16 /* MsgHeader */

      def uncompressedData: ByteBuf = {
        val buf = Unpooled.directBuffer(uncompressedSize)

        req.op writeTo buf

        req.payload.resetReaderIndex()
        buf writeBytes req.payload
      }

      val compressed: Try[ByteBuf] = compressor match {
        case Compressor.Snappy => {
          val bufSize = org.xerial.snappy.Snappy.
            maxCompressedLength(uncompressedSize)

          val out = Unpooled.directBuffer(bufSize)

          buffer.Snappy().encode(uncompressedData, out).map(_ => out)
        }

        case Compressor.Zstd => {
          val bufSize = com.github.luben.zstd.Zstd.
            compressBound(uncompressedSize.toLong).toInt

          val out = Unpooled.directBuffer(bufSize)

          buffer.Zstd().encode(uncompressedData, out).map(_ => out)
        }

        case Compressor.Zlib(level) => {
          val out = Unpooled.directBuffer(
            (uncompressedSize.toDouble * 1.2D).toInt)

          buffer.Zlib(level).encode(uncompressedData, out).map(_ => out)
        }

        case _ =>
          Try(uncompressedData)
      }

      compressed.map { payload =>
        new Request(
          requestID = req.requestID,
          responseTo = req.responseTo,
          op = new CompressedOp(
            expectsResponse = req.op.expectsResponse,
            requiresPrimary = req.op.requiresPrimary,
            originalOpCode = req.op.code,
            uncompressedSize = uncompressedSize,
            compressorId = compressor.id),
          payload = payload,
          readPreference = req.readPreference,
          channelIdHint = req.channelIdHint)
      }
    }
  }
}
