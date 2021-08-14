package reactivemongo.core.protocol

import java.util.{ List => JList }

import reactivemongo.io.netty.buffer.ByteBuf
import reactivemongo.io.netty.channel.ChannelHandlerContext

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
