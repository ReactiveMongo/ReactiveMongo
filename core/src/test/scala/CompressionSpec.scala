package reactivemongo.core.protocol.buffer

import scala.util.{ Random, Try }

import reactivemongo.io.netty.buffer.{
  ByteBuf,
  PooledByteBufAllocator,
  Unpooled
}

final class CompressionSpec extends org.specs2.mutable.Specification {
  "Compression".title

  { // Zstandard
    val zstd = Zstd()

    spec(
      name = "Zstandard",
      encode = zstd.encode(_: ByteBuf, _: ByteBuf),
      decode = zstd.decode(_: ByteBuf, _: ByteBuf))
  }

  { // Zlib
    val defaultZlib = Zlib.DefaultCompressor

    spec(
      name = "Zlib (default)",
      encode = defaultZlib.encode(_: ByteBuf, _: ByteBuf),
      decode = defaultZlib.decode(_: ByteBuf, _: ByteBuf))

    // Best speed
    val bestSpeedZlib = Zlib(1)

    spec(
      name = "Zlib (best speed)",
      encode = bestSpeedZlib.encode(_: ByteBuf, _: ByteBuf),
      decode = bestSpeedZlib.decode(_: ByteBuf, _: ByteBuf))

    // Best compression
    val bestCompressionZlib = Zlib(9)

    spec(
      name = "Zlib (best compression)",
      encode = bestCompressionZlib.encode(_: ByteBuf, _: ByteBuf),
      decode = bestCompressionZlib.decode(_: ByteBuf, _: ByteBuf))
  }

  { // Snappy
    val snappy = Snappy()

    spec(
      name = "Snappy",
      encode = snappy.encode(_: ByteBuf, _: ByteBuf),
      decode = snappy.decode(_: ByteBuf, _: ByteBuf))
  }

  // ---

  private def spec(
    name: String,
    encode: Function2[ByteBuf, ByteBuf, Try[Unit]],
    decode: Function2[ByteBuf, ByteBuf, Try[Int]]) = {
    def roundtrip(data: => Array[Byte]) = {
      val input: Array[Byte] = data
      val sz = input.size
      val orig = Unpooled.wrappedBuffer(input)
      val compressed = PooledByteBufAllocator.DEFAULT.directBuffer(sz)

      s"size $sz" in {
        encode(orig, compressed) must beSuccessfulTry({}) and {
          val decompressed = PooledByteBufAllocator.DEFAULT.directBuffer(sz)

          decode(compressed, decompressed) must beSuccessfulTry(sz)

          (0 until decompressed.readableBytes).forall { i =>
            input(i) == decompressed.readByte
          } must beTrue
        }
      }
    }

    s"Compression $name" should {
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
}
