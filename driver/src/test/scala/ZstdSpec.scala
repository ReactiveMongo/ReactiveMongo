package reactivemongo.core.protocol

import scala.util.Random

import reactivemongo.io.netty.buffer.{
  ByteBufInputStream,
  ByteBuf,
  PooledByteBufAllocator,
  Unpooled
}

import com.github.luben.zstd.ZstdInputStream

final class ZstdSpec extends org.specs2.mutable.Specification {
  "Zstd".title

  "Compression" should {
    val zstd = Zstd()

    "work with random data" >> {
      def spec(sz: Int) = {
        val input: Array[Byte] = {
          val bytes = Array.ofDim[Byte](sz)
          Random.nextBytes(bytes)
          bytes
        }
        val in = Unpooled.wrappedBuffer(input)
        val out = PooledByteBufAllocator.DEFAULT.directBuffer(sz)

        s"size $sz" in {
          zstd.encode(in, out) must beSuccessfulTry({}) and {
            decompress(out, sz).array() must_=== input
          }
        }
      }

      spec(1024)
      spec(8096)
      spec(16192)
      spec(524288)
    }

    "work with recurrent data" >> {
      def spec(sz: Int) = {
        val input: Array[Byte] = Array.fill[Byte](sz)(1.toByte)
        val in = Unpooled.wrappedBuffer(input)
        val out = PooledByteBufAllocator.DEFAULT.directBuffer(sz)

        s"size $sz" in {
          zstd.encode(in, out) must beSuccessfulTry({}) and {
            decompress(out, sz).array() must_=== input
          }
        }
      }

      spec(1024)
      spec(8096)
      spec(16192)
      spec(524288)
    }
  }

  // ---

  def decompress(compressed: ByteBuf, originalLength: Int) = {
    val is = new ByteBufInputStream(compressed, true)
    val decompressed = Array.ofDim[Byte](originalLength)

    var zstdIs: ZstdInputStream = null

    @annotation.tailrec
    def read(remaining: Int): Unit = if (remaining > 0) {
      val r = zstdIs.read(
        decompressed, originalLength - remaining, remaining);

      if (r > 0) {
        read(remaining - r)
      }
    }

    try {
      zstdIs = new ZstdInputStream(is)

      read(originalLength)

      //assertEquals(-1, zstdIs.read())
    } finally {
      if (zstdIs != null) {
        zstdIs.close()
      } else {
        is.close()
      }
    }

    Unpooled.wrappedBuffer(decompressed)
  }
}
