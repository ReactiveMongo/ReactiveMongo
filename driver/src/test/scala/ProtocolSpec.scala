import reactivemongo.io.netty.buffer.Unpooled

import reactivemongo.bson.BSONDocument

import org.specs2.specification.core.Fragments
import org.specs2.concurrent.ExecutionEnv

final class ProtocolSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification {

  "Protocol" title

  import reactivemongo.api.tests.{
    Response,
    query,
    getBytes,
    messageHeader,
    readMessageHeader,
    readReply,
    reply => _reply
  }

  section("unit")

  val header = messageHeader(205, 0, 0, 1 /* OP_REPLY */ )

  header.toString should {
    def buffer = Unpooled.buffer(msg1Bytes.size, msg1Bytes.size)

    "be read from Netty buffer" in {
      readMessageHeader(buffer writeBytes msg1Bytes) must_=== header
    }

    "be written to Netty buffer" in {
      val buf = buffer

      header.writeTo(buf) must_=== ({}) and {
        val written = Array.ofDim[Byte](header.size)

        buf.resetReaderIndex()
        buf.getBytes(0, written)

        written must_=== msg1Bytes.take(header.size)
      }
    }
  }

  val reply = _reply(8, 0, 0, 1)

  reply.toString should {
    val byteSize = msg1Bytes.size - header.size
    def buffer = Unpooled.buffer(byteSize, byteSize)

    "be read from Netty buffer (after message)" in {
      readReply(
        buffer writeBytes msg1Bytes.drop(header.size)) must_=== reply
    }
  }

  "Request" should {
    "be written to Netty buffer" >> {
      "for Query" in {
        val queryOp = query(4, f"admin.$$cmd", 0, 1)
        val buffer = Unpooled.buffer(queryOp.size, queryOp.size)
        val opBytes = Array[Byte](4, 0, 0, 0, 97, 100, 109, 105, 110, 46, 36, 99, 109, 100, 0, 0, 0, 0, 0, 1, 0, 0, 0)

        queryOp.writeTo(buffer) must_=== ({}) and {
          getBytes(buffer, queryOp.size) must_=== opBytes
        }
      }

      "for isMaster" in {
        import reactivemongo.api.tests.isMasterRequest

        val req = isMasterRequest(reqId = 0)
        val buffer = Unpooled.buffer(req.size, req.size)
        val bytes = Array[Byte](58, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -44, 7, 0, 0, 4, 0, 0, 0, 97, 100, 109, 105, 110, 46, 36, 99, 109, 100, 0, 0, 0, 0, 0, 1, 0, 0, 0, 19, 0, 0, 0, 16, 105, 115, 109, 97, 115, 116, 101, 114, 0, 1, 0, 0, 0, 0)

        req.writeTo(buffer) must_=== ({}) and {
          getBytes(buffer, req.size) must_=== bytes
        }
      }
    }
  }

  "Response" should {
    import reactivemongo.api.tests.{ decodeResponse, decodeFrameResp, preload }

    "be decoded from Netty" >> {
      Fragments.foreach(Seq[(Array[Byte], Int)](
        msg1Bytes -> 1, // exactly 1 frame
        (msg1Bytes ++ msg1Bytes.take(10)) -> 1, // 1 frame + some bytes
        (msg1Bytes ++ msg1Bytes ++ msg1Bytes) -> 3, // exactly 3 frames
        (msg1Bytes ++ msg1Bytes ++ msg1Bytes.
          dropRight(3)) -> 2 /* 2 frames + some bytes*/ )) {
        case (bytes, frameCount) => s"as $frameCount frames" in {
          decodeFrameResp(bytes) must have size (frameCount)
        }
      }
    }

    "be read from Netty buffer" in {
      decodeResponse(msg1Bytes) {
        case (buf, response) => response.header must_=== header and {
          response.reply must_=== reply
        } and {
          import response.{ documents, info }

          val offset = header.size + ( /*reply*/ 4 + 8 + 4 + 4)
          val docsSize = msg1Bytes.size - offset

          // Alter the internal buffer to ensure the `documents` one is detached
          buf.setIndex(0, 0)
          buf.writeInt(0)

          val expectedBytes = msg1Bytes.drop(offset)

          preload(response) must beLike[(Response, BSONDocument)] {
            case (resp, doc) => resp.header must_=== header and {
              resp.reply must_=== reply
            } and {
              resp.info must_=== info
            } and {
              doc.getAs[Boolean]("ismaster") must beSome(true)
            } and {
              getBytes(resp.documents, docsSize) must_=== expectedBytes
            }
          }.await and {
            getBytes(documents, docsSize) must_=== expectedBytes
          }
        }
      }
    }
  }

  section("unit")

  // ---

  lazy val msg1Bytes = Array[Byte](-51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, -87, 0, 0, 0, 8, 105, 115, 109, 97, 115, 116, 101, 114, 0, 1, 16, 109, 97, 120, 66, 115, 111, 110, 79, 98, 106, 101, 99, 116, 83, 105, 122, 101, 0, 0, 0, 0, 1, 16, 109, 97, 120, 77, 101, 115, 115, 97, 103, 101, 83, 105, 122, 101, 66, 121, 116, 101, 115, 0, 0, 108, -36, 2, 16, 109, 97, 120, 87, 114, 105, 116, 101, 66, 97, 116, 99, 104, 83, 105, 122, 101, 0, -24, 3, 0, 0, 9, 108, 111, 99, 97, 108, 84, 105, 109, 101, 0, 121, -89, -110, -101, 95, 1, 0, 0, 16, 109, 97, 120, 87, 105, 114, 101, 86, 101, 114, 115, 105, 111, 110, 0, 5, 0, 0, 0, 16, 109, 105, 110, 87, 105, 114, 101, 86, 101, 114, 115, 105, 111, 110, 0, 0, 0, 0, 0, 8, 114, 101, 97, 100, 79, 110, 108, 121, 0, 0, 1, 111, 107, 0, 0, 0, 0, 0, 0, 0, -16, 63, 0)

}
