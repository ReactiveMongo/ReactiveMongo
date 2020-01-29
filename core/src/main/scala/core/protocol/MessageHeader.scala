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
@deprecated("No longer a case class, no longer a case class", "0.20.3")
class MessageHeader private[core] (
  val messageLength: Int,
  val requestID: Int,
  val responseTo: Int,
  val opCode: Int) extends ChannelBufferWritable
  with Product4[Int, Int, Int, Int] with Serializable {

  val writeTo: ByteBuf => Unit = writeTupleToBuffer4(
    (messageLength, requestID, responseTo, opCode)) _

  override val size = MessageHeader.size

  private[core] lazy val tupled =
    Tuple4(messageLength, requestID, responseTo, opCode)

  @deprecated("No longer case class", "0.20.3")
  @inline def _1 = messageLength

  @deprecated("No longer case class", "0.20.3")
  @inline def _2 = requestID

  @deprecated("No longer case class", "0.20.3")
  @inline def _3 = responseTo

  @deprecated("No longer case class", "0.20.3")
  @inline def _4 = opCode

  @deprecated("No longer case class", "0.20.3")
  def canEqual(that: Any): Boolean = that match {
    case _: MessageHeader => true
    case _                => false
  }

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
@deprecated("Internal: will be made private", "0.20.3")
object MessageHeader extends scala.runtime.AbstractFunction4[Int, Int, Int, Int, MessageHeader] with ChannelBufferReadable[MessageHeader] {

  def apply(
    messageLength: Int,
    requestID: Int,
    responseTo: Int,
    opCode: Int): MessageHeader =
    new MessageHeader(messageLength, requestID, responseTo, opCode)

  @deprecated("No longer case class", "0.20.3")
  def unapply(header: MessageHeader): Option[Tuple4[Int, Int, Int, Int]] =
    Option(header).map(_.tupled)

  override def readFrom(buffer: ByteBuf): MessageHeader = {
    val messageLength = buffer.readIntLE
    val requestID = buffer.readIntLE
    val responseTo = buffer.readIntLE
    val opCode = buffer.readIntLE

    MessageHeader(messageLength, requestID, responseTo, opCode)
  }

  private[core] val size = 4 * 4 // 4 * int32
}
