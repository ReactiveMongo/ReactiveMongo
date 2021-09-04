package reactivemongo.core.protocol

import reactivemongo.api.bson.BSONDocument

import reactivemongo.io.netty.buffer.{ ByteBuf, Unpooled }

final class ResponseDecoderSpec extends org.specs2.mutable.Specification {
  "Response decoder".title

  section("unit")

  "ResponseDecoder" should {
    lazy val decoder = new ResponseDecoder

    "decompress" in {
      val bytes = Array[Byte](
        /* originalOpCode:   */ 0x01, 0x00, 0x00, 0x00,
        /* uncompressedSize: */ 0x5F, 0x00, 0x00, 0x00,
        /* compressor ID:    */ 0x01,
        // Payload:
        0x5F, 0x04, 0x08, 0x00, 0x36, 0x01, 0x00, 0x5C, 0x01, 0x00, 0x00, 0x00, 0x4B, 0x00, 0x00, 0x00, //
        0x03, 0x63, 0x75, 0x72, 0x73, 0x6F, 0x72, 0x00, 0x32, 0x00, 0x00, 0x00, 0x12, 0x69, 0x64, 0x00, //
        0x11, 0x01, 0x0C, 0x02, 0x6E, 0x73, 0x00, 0x01, 0x34, 0x50, 0x66, 0x6F, 0x6F, 0x2E, 0x62, 0x61, //
        0x72, 0x00, 0x04, 0x66, 0x69, 0x72, 0x73, 0x74, 0x42, 0x61, 0x74, 0x63, 0x68, 0x00, 0x05, 0x05, //
        0x25, 0x30, 0x01, 0x6F, 0x6B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0.toByte, 0x3F, 0x00)

      withDirectBuffer(bytes.size) { cbuf =>
        val compressed: ByteBuf = {
          cbuf.writeBytes(bytes)
          cbuf
        }

        val msgHeader = new MessageHeader(104, 156, 3000, 2012)

        val response = decoder.decompress(
          channelId = None,
          frame = compressed,
          header = msgHeader,
          allocDirect = Unpooled.directBuffer(_: Int))

        response.header must_=== msgHeader.copy(
          messageLength = 95,
          opCode = Reply.code) and {
          response.reply must_=== new Reply(
            flags = 8,
            cursorID = 0L,
            startingFrom = 0,
            numberReturned = 0)
        } and {
          ResponseDecoder.first(response.documents) must beSuccessfulTry(
            BSONDocument(
              "cursor" -> BSONDocument(
                "id" -> 0L,
                "ns" -> "foo.bar",
                "firstBatch" -> Seq.empty[BSONDocument]),
              "ok" -> 1D))
        }
      }
    }
  }

  private def withDirectBuffer[T](size: Int)(f: ByteBuf => T) = {
    val buf = Unpooled.directBuffer(size)

    try {
      f(buf)
    } finally {
      buf.release()
      ()
    }
  }
}
