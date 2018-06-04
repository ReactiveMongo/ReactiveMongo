package reactivemongo.core.protocol

import shaded.netty.buffer.ByteBuf

import reactivemongo.core.protocol.BufferAccessors.writeTupleToBuffer4

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

  val writeTo: ByteBuf => Unit = writeTupleToBuffer4(
    (messageLength, requestID, responseTo, opCode)) _

  override val size = MessageHeader.size
}

/** Header deserializer from a [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]]. */
object MessageHeader extends ChannelBufferReadable[MessageHeader] {
  override def readFrom(buffer: ByteBuf): MessageHeader = {
    val messageLength = buffer.readIntLE
    val requestID = buffer.readIntLE
    val responseTo = buffer.readIntLE
    val opCode = buffer.readIntLE

    MessageHeader(messageLength, requestID, responseTo, opCode)
  }

  private[core] val size = 4 * 4 // 4 * int32
}
