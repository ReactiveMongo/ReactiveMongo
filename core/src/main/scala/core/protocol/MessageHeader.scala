package reactivemongo.core.protocol

import reactivemongo.io.netty.buffer.ByteBuf

import reactivemongo.core.protocol.BufferAccessors.writeTupleToBuffer4

/**
 * Header of a Mongo Wire Protocol message.
 *
 * @param messageLength length of this message.
 * @param requestID id of this request (> 0 for request operations, else 0).
 * @param responseTo id of the request that the message including this a response to (> 0 for reply operation, else 0).
 * @param opCode operation code of this message.
 */
private[reactivemongo] class MessageHeader(
  val messageLength: Int,
  val requestID: Int,
  val responseTo: Int,
  val opCode: Int) extends ChannelBufferWritable {

  val writeTo: ByteBuf => Unit = writeTupleToBuffer4(
    (messageLength, requestID, responseTo, opCode)) _

  override val size = MessageHeader.size

  private[core] lazy val tupled =
    Tuple4(messageLength, requestID, responseTo, opCode)

  private[core] def copy(
    messageLength: Int = this.messageLength,
    requestID: Int = this.requestID,
    responseTo: Int = this.responseTo,
    opCode: Int = this.opCode): MessageHeader = new MessageHeader(
    messageLength, requestID, responseTo, opCode)

  override def equals(that: Any): Boolean = that match {
    case other: MessageHeader =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString: String =
    s"MessageHeader($messageLength, $requestID, $responseTo, $opCode)"
}

/** Header deserializer from a [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]]. */
private[reactivemongo] object MessageHeader
  extends ChannelBufferReadable[MessageHeader] {

  override def readFrom(buffer: ByteBuf): MessageHeader = {
    val messageLength = buffer.readIntLE
    val requestID = buffer.readIntLE
    val responseTo = buffer.readIntLE
    val opCode = buffer.readIntLE

    new MessageHeader(messageLength, requestID, responseTo, opCode)
  }

  private[core] val size = 4 * 4 // 4 * int32
}
