package reactivemongo.core.protocol.buffer

import java.nio.ByteBuffer

import scala.util.Try

import org.xerial.snappy.{ Snappy => Z }

import reactivemongo.io.netty.buffer.ByteBuf

private[reactivemongo] final class Snappy {

  def decode(in: ByteBuf, out: ByteBuf): Try[Int] = Try {
    val outNioBuffer: ByteBuffer =
      out.internalNioBuffer(out.writerIndex, out.writableBytes)

    val inNioBuffer = in.nioBuffer()
    val sz = Z.uncompress(inNioBuffer, outNioBuffer)

    in.readerIndex(in.writerIndex)
    out.writerIndex(sz)

    sz
  }

  def encode(in: ByteBuf, out: ByteBuf): Try[Unit] = Try {
    val sz: Int = if (in.isDirect) {
      val inNioBuffer: ByteBuffer = in.nioBuffer
      val outNioBuffer: ByteBuffer =
        out.internalNioBuffer(out.writerIndex, out.writableBytes)

      val z = Z.compress(inNioBuffer, outNioBuffer)

      in.readerIndex(inNioBuffer.position())

      z
    } else {
      val bytes = in.array
      val compressed = Z.compress(bytes)

      out.writeBytes(compressed)

      in.readerIndex(bytes.size)

      compressed.size
    }

    out.writerIndex(sz)

    ()
  }
}

private[reactivemongo] object Snappy {
  def apply(): Snappy = new Snappy()

  lazy val DefaultCompressor = Snappy()
}
