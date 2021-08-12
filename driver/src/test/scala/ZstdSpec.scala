package reactivemongo.core.protocol

import scala.util.Random

import reactivemongo.io.netty.buffer.{ PooledByteBufAllocator, Unpooled }

final class ZstdSpec extends org.specs2.mutable.Specification {
  "Zstd".title

  "Compression" should {
    val zstd = Zstd()

    def roundtrip(data: => Array[Byte]) = {
      val input: Array[Byte] = data
      val sz = input.size
      val orig = Unpooled.wrappedBuffer(input)
      val compressed = PooledByteBufAllocator.DEFAULT.directBuffer(sz)

      s"size $sz" in {
        zstd.encode(
          in = orig,
          out = compressed) must beSuccessfulTry({}) and {
          val decompressed = PooledByteBufAllocator.DEFAULT.directBuffer(sz)

          zstd.decode(
            in = compressed, out = decompressed) must beSuccessfulTry(sz)

          (0 until decompressed.readableBytes).forall { i =>
            input(i) == decompressed.readByte
          } must beTrue
        }
      }
    }

    "work with random data" >> {
      def data(sz: Int): Array[Byte] = {
        val bytes = Array.ofDim[Byte](sz)
        Random.nextBytes(bytes)
        bytes
      }

      roundtrip(data(1024))
      roundtrip(data(8096))
      roundtrip(data(16192))
      roundtrip(data(524288))
    }

    "work with recurrent data" >> {
      def data(sz: Int): Array[Byte] = {
        val byte = Random.nextInt.toByte
        Array.fill[Byte](sz)(byte)
      }

      roundtrip(data(1024))
      roundtrip(data(8096))
      roundtrip(data(16192))
      roundtrip(data(524288))
    }
  }
}
