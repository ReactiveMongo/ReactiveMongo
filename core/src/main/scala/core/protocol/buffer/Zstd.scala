package reactivemongo.core.protocol.buffer

import java.nio.ByteBuffer

import scala.util.Try

import reactivemongo.io.netty.buffer.{ ByteBuf, PooledByteBufAllocator }

import com.github.luben.zstd.{ Zstd => Z }

/**
 * [[https://facebook.github.io/zstd Zstandard]] compression
 *
 * Inspired from https://github.com/netty/netty/blob/master/codec/src/main/java/io/netty/handler/codec/compression/ZstdEncoder.java
 */
private[reactivemongo] final class Zstd(
    blockSize: Int,
    compressionLevel: Int,
    allocDirect: Int => ByteBuf) {

  def decode(in: ByteBuf, out: ByteBuf): Try[Int] = Try {
    val inNioBuffer: ByteBuffer = in.nioBuffer()

    val outNioBuffer: ByteBuffer =
      out.internalNioBuffer(out.writerIndex, out.writableBytes)

    val sz = Z.decompress(outNioBuffer, inNioBuffer)

    in.readerIndex(in.writerIndex)
    out.writerIndex(sz)

    sz
  }

  def encode(in: ByteBuf, out: ByteBuf): Try[Unit] =
    compress(in, out, buffer = allocDirect(blockSize))

  def compress(in: ByteBuf, out: ByteBuf, buffer: ByteBuf): Try[Unit] = {
    @annotation.tailrec
    def go(): Unit = {
      val length = in.readableBytes

      if (length > 0) {
        val nextChunkSize = Math.min(length, buffer.writableBytes)

        in.readBytes(buffer, nextChunkSize)

        if (!buffer.isWritable()) {
          flushBufferedData(buffer, out)
        }

        go()
      }
    }

    Try {
      buffer.clear()
      go()
    }.map { _ =>
      if (buffer != null && buffer.isReadable()) {
        flushBufferedData(buffer, out)
      }

      buffer.release()

      ()
    }
  }

  // Unsafe
  private def flushBufferedData(buffer: ByteBuf, out: ByteBuf): Unit = {
    val flushableBytes = buffer.readableBytes

    if (flushableBytes > 0) {
      val bufSize = Z.compressBound(flushableBytes.toLong).toInt

      out.ensureWritable(bufSize)

      val idx = out.writerIndex()

      val outNioBuffer = out.internalNioBuffer(idx, out.writableBytes)

      val compressedLength = Z.compress(
        outNioBuffer,
        buffer.internalNioBuffer(buffer.readerIndex(), flushableBytes),
        compressionLevel
      )

      out.writerIndex(idx + compressedLength)
      buffer.clear()

      ()
    }
  }
}

private[reactivemongo] object Zstd {

  /**
   * Default compression level
   */
  val DefaultCompressionLevel: Int = 3

  /**
   * Default block size
   */
  val DefaultBlockSize: Int = 1 << 16 // 64 KB

  def apply(
      blockSize: Int = DefaultBlockSize,
      compressionLevel: Int = DefaultCompressionLevel,
      allocDirect: Int => ByteBuf = buffer(_)
    ): Zstd = new Zstd(
    blockSize = blockSize,
    compressionLevel = compressionLevel,
    allocDirect = allocDirect
  )

  @inline def buffer(blockSize: Int): ByteBuf =
    PooledByteBufAllocator.DEFAULT.directBuffer(blockSize)

  lazy val DefaultCompressor = Zstd()
}
