package reactivemongo.core.protocol

import scala.util.{ Success, Try }
import scala.util.control.NonFatal

import reactivemongo.io.netty.buffer.{ ByteBuf, Unpooled }
import reactivemongo.io.netty.channel.ChannelId

import reactivemongo.core.protocol.buffer.ChannelBufferWritable

import reactivemongo.api.{ Compressor, ReadPreference }
import reactivemongo.api.commands.CommandKind

/**
 * Request message.
 *
 * @param requestID the ID of this request, so that the response may be identifiable. Should be strictly positive.
 * @param op request operation.
 * @param channelIdHint a hint for sending this request on a particular channel.
 */
private[reactivemongo] final class Request private (
    val kind: CommandKind,
    val requestID: Int,
    val responseTo: Int,
    val op: RequestOp,
    val payload: ByteBuf,
    val readPreference: ReadPreference = ReadPreference.primary,
    val channelIdHint: Option[ChannelId] = None)
    extends ChannelBufferWritable {

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
 * @define kind the command kind of this request
 * @define requestID id of this request, so that the response may be identifiable. Should be strictly positive.
 * @define op request operation.
 * @define documentsA body of this request, an Array containing 0, 1, or many documents.
 * @define documentsC body of this request, a [[http://netty.io/4.0/api/io/netty/buffer/ByteBuf.html ByteBuf]] containing 0, 1, or many documents.
 */
private[reactivemongo] object Request {

  /**
   * Create a request.
   *
   * @param kind $kind
   * @param requestID $requestID
   * @param op $op
   * @param documents body of this request, a [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]] containing 0, 1, or many documents.
   */
  def apply(
      kind: CommandKind,
      requestID: Int,
      responseTo: Int,
      op: RequestOp,
      documents: ByteBuf,
      readPreference: ReadPreference = ReadPreference.primary,
      channelIdHint: Option[ChannelId] = None,
      callerSTE: Seq[StackTraceElement] = Seq.empty
    ): Request =
    try {
      new Request(
        kind,
        requestID,
        responseTo,
        op,
        documents,
        readPreference,
        channelIdHint
      )
    } catch {
      case NonFatal(cause) =>
        val trace = cause.getStackTrace
        val callerTrace = Array.newBuilder[StackTraceElement] ++= trace

        if (trace.nonEmpty) {
          callerTrace += new StackTraceElement("---", "---", "---", -1)
        }

        callerTrace ++= callerSTE

        cause.setStackTrace(callerTrace.result())

        throw cause
    }

  /**
   * Create a request.
   *
   * @param kind $kind
   * @param requestID $requestID
   * @param op $op
   * @param documents $documentsA
   */
  def apply(
      kind: CommandKind,
      requestID: Int,
      responseTo: Int,
      op: RequestOp,
      documents: Array[Byte]
    ): Request = new Request(
    kind,
    requestID,
    responseTo,
    op,
    Unpooled.wrappedBuffer(documents)
  )

  /**
   * Create a request.
   *
   * @param kind $kind
   * @param requestID $requestID
   * @param op $op
   * @param documents $documentsA
   */
  def apply(
      kind: CommandKind,
      requestID: Int,
      op: RequestOp,
      documents: Array[Byte]
    ): Request =
    Request.apply(kind, requestID, 0, op, documents)

  /**
   * Create a request.
   *
   * @param kind $kind
   * @param requestID $requestID
   * @param op $op
   */
  def apply(kind: CommandKind, requestID: Int, op: RequestOp): Request =
    Request.apply(kind, requestID, op, new Array[Byte](0))

  /**
   * @param req the request to be compressed
   * @param compressor the compressor to be applied (if required)
   */
  def compress(
      req: Request,
      compressor: Compressor,
      allocDirect: Int => ByteBuf
    ): Try[Request] = {
    import req.op.{ code => originalOpCode }

    if (originalOpCode == CompressedOp.code) {
      // Already compressed
      Success(req)
    } else {
      val uncompressedSize = req.size - 16 /* MsgHeader */

      def withData[T](f: ByteBuf => T): T = {
        var buf: ByteBuf = null

        try {
          buf = allocDirect(uncompressedSize)

          req.op writeTo buf

          buf writeBytes req.payload

          f(buf)
        } catch {
          case NonFatal(cause) =>
            throw cause
        } finally {
          if (buf != null) {
            buf.release()
            ()
          }
        }
      }

      val compressed: Try[ByteBuf] = compressor match {
        case Compressor.Snappy =>
          Try {
            val bufSize =
              org.xerial.snappy.Snappy.maxCompressedLength(uncompressedSize)

            allocDirect(bufSize)
          }.flatMap { out =>
            withData {
              buffer.Snappy().encode(_, out).map(_ => out)
            }
          }

        case Compressor.Zstd =>
          Try {
            val bufSize = com.github.luben.zstd.Zstd
              .compressBound(uncompressedSize.toLong)
              .toInt

            allocDirect(bufSize)
          }.flatMap { out =>
            withData {
              buffer.Zstd().encode(_, out).map(_ => out)
            }
          }

        case Compressor.Zlib(level) =>
          Try {
            val bufSize = (uncompressedSize.toDouble * 1.2D).toInt

            allocDirect(bufSize)
          }.flatMap { out =>
            withData {
              buffer.Zlib(level).encode(_, out).map(_ => out)
            }
          }

        case _ =>
          Success(req.payload)
      }

      compressed.map { payload =>
        new Request(
          kind = req.kind,
          requestID = req.requestID,
          responseTo = req.responseTo,
          op = new CompressedOp(
            expectsResponse = req.op.expectsResponse,
            requiresPrimary = req.op.requiresPrimary,
            originalOpCode = req.op.code,
            uncompressedSize = uncompressedSize,
            compressorId = compressor.id
          ),
          payload = payload,
          readPreference = req.readPreference,
          channelIdHint = req.channelIdHint
        )
      }
    }
  }
}
