package reactivemongo.core.protocol

import reactivemongo.io.netty.buffer.{ ByteBuf, Unpooled }
import reactivemongo.io.netty.channel.ChannelId

import reactivemongo.core.netty.BufferSequence

import reactivemongo.api.ReadPreference

/**
 * Request message.
 *
 * @param requestID the ID of this request, so that the response may be identifiable. Should be strictly positive.
 * @param op request operation.
 * @param documents body of this request, a [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]] containing 0, 1, or many documents.
 * @param channelIdHint a hint for sending this request on a particular channel.
 */
case class Request(
  requestID: Int,
  responseTo: Int,
  op: RequestOp,
  documents: BufferSequence,
  readPreference: ReadPreference = ReadPreference.primary,
  channelIdHint: Option[ChannelId] = None) extends ChannelBufferWritable {

  val writeTo: ByteBuf => Unit = { buffer =>
    header writeTo buffer
    op writeTo buffer

    buffer writeBytes documents.merged

    ()
  }

  def size = 16 + op.size + documents.merged.writerIndex

  /** Header of this request */
  lazy val header = MessageHeader(size, requestID, responseTo, op.code)

  override def toString =
    s"Request($requestID, $responseTo, $op, $readPreference, $channelIdHint)"
}

/**
 * @define requestID id of this request, so that the response may be identifiable. Should be strictly positive.
 * @define op request operation.
 * @define documentsA body of this request, an Array containing 0, 1, or many documents.
 * @define documentsC body of this request, a [[http://netty.io/4.0/api/io/netty/buffer/ByteBuf.html ByteBuf]] containing 0, 1, or many documents.
 */
object Request {
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
    documents: Array[Byte]): Request = Request(
    requestID,
    responseTo,
    op,
    BufferSequence(Unpooled wrappedBuffer documents))

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

}
