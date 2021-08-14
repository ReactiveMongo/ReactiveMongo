package reactivemongo.core.protocol.buffer

import java.nio.ByteBuffer

import scala.util.Try

import org.xerial.snappy.{ Snappy => Z }

import reactivemongo.io.netty.buffer.ByteBuf

private[reactivemongo] final class Snappy {
  def decode(in: ByteBuf, out: ByteBuf): Try[Int] = Try {
    val outNioBuffer: ByteBuffer =
      out.internalNioBuffer(out.writerIndex, out.writableBytes)

    val sz = Z.uncompress(in.nioBuffer, outNioBuffer)

    out.writerIndex(sz)

    sz
  }

  def encode(in: ByteBuf, out: ByteBuf): Try[Unit] = Try {
    if (in.isDirect) {
      val outNioBuffer: ByteBuffer =
        out.internalNioBuffer(out.writerIndex, out.writableBytes)

      Z.compress(in.nioBuffer, outNioBuffer)
    } else {
      val compressed = Z.compress(in.array)

      out.writeBytes(compressed)

      out.writerIndex(compressed.size)
    }

    ()
  }
}

private[reactivemongo] object Snappy {
  def apply(): Snappy = new Snappy()

  lazy val DefaultCompressor = Snappy()
}
