package reactivemongo.core.protocol

import java.io.OutputStream

import java.util.zip.{ Deflater, DeflaterOutputStream, InflaterOutputStream }

import scala.util.Try
import scala.util.control.NonFatal

import reactivemongo.io.netty.buffer.{ ByteBuf, ByteBufOutputStream }

private[reactivemongo] final class Zlib(compressionLevel: Int) {
  def decode(in: ByteBuf, out: ByteBuf): Try[Int] = Try {
    val outStream = new InflaterOutputStream(new ByteBufOutputStream(out))

    try {
      val before = out.writerIndex

      copy(in, outStream, blockSize = 8192)

      out.writerIndex - before
    } finally {
      outStream.close()
    }
  }

  def encode(in: ByteBuf, out: ByteBuf): Try[Unit] = Try {
    val deflater = new Deflater(compressionLevel)

    val outStream = new DeflaterOutputStream(
      new ByteBufOutputStream(out), deflater)

    try {
      copy(in, outStream, blockSize = 8192)
    } finally {
      try {
        deflater.finish()
      } catch {
        case NonFatal(_) =>
      }

      outStream.close()
    }
  }

  // ---

  @annotation.tailrec
  private def copy(in: ByteBuf, out: OutputStream, blockSize: Int): Unit = {
    val readable = in.readableBytes

    if (readable > 0) {
      val nextChunkSize = Math.min(blockSize, readable)

      in.readBytes(out, nextChunkSize)

      out.flush()

      copy(in, out, blockSize)
    }
  }
}

private[reactivemongo] object Zlib {
  def apply(compressionLevel: Int = -1): Zlib = new Zlib(compressionLevel)
}
