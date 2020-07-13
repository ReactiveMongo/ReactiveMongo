package reactivemongo

import java.util.Arrays

import reactivemongo.api.bson.{
  buffer,
  BSONArray,
  BSONDocument,
  BSONDouble,
  BSONInteger,
  BSONLong,
  BSONNull,
  BSONObjectID,
  BSONString,
  BSONUndefined,
  BSONValue
}
import reactivemongo.api.bson.collection.BSONSerializationPack

final class BsonSpec extends org.specs2.mutable.Specification {
  "BSON serialization" title

  val simple = Array[Byte](0x16, 0x00, 0x00, 0x00, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00, 0x06, 0x00, 0x00, 0x00, 'w', 'o', 'r', 'l', 'd', 0x00, 0x00)

  val embeddingArray = Array[Byte](70, 0, 0, 0, 7, 95, 105, 100, 0, 80, 55, -110, -63, -104, 69, -121, -105, 27, 20, 83, 14, 4, 66, 83, 79, 78, 0, 42, 0, 0, 0, 2, 48, 0, 8, 0, 0, 0, 97, 119, 101, 115, 111, 109, 101, 0, 1, 49, 0, 51, 51, 51, 51, 51, 51, 20, 64, 1, 50, 0, 0, 0, 0, 0, 0, 8, -97, 64, 0, 0)

  val bsonArray = Array[Byte](42, 0, 0, 0, 2, 48, 0, 8, 0, 0, 0, 97, 119, 101, 115, 111, 109, 101, 0, 1, 49, 0, 51, 51, 51, 51, 51, 51, 20, 64, 1, 50, 0, 0, 0, 0, 0, 0, 8, -97, 64, 0)

  section("unit")

  "Codec" should {
    "produce a simple document" in {
      val doc = BSONDocument("hello" -> BSONString("world"))

      compare(simple, makeBuffer(doc))
    }

    "produce a simple doc through a traversable" in {
      val buf = makeBuffer(BSONDocument("hello" -> BSONString("world")))

      compare(simple, makeBuffer(makeDocument(buffer.ReadableBuffer(buf))))
    }

    "produce a document embedding an array" in {
      BSONObjectID.parse("503792c1984587971b14530e").
        aka("parsed") must beSuccessfulTry[BSONObjectID].like {
          case oid =>
            val doc = BSONDocument(
              "_id" -> oid,
              "BSON" -> BSONArray(
                BSONString("awesome"),
                BSONDouble(5.05),
                BSONDouble(1986)))

            compare(embeddingArray, makeBuffer(doc))
        }
    }

    "produce a document embedding an array through traversable" in {
      BSONObjectID.parse("503792c1984587971b14530e").
        aka("parsed") must beSuccessfulTry[BSONObjectID].like {
          case oid =>
            val buf = makeBuffer(BSONDocument(
              "_id" -> oid,
              "BSON" -> BSONArray(
                BSONString("awesome"),
                BSONDouble(5.05),
                BSONDouble(1986))))

            compare(
              embeddingArray,
              makeBuffer(makeDocument(buffer.ReadableBuffer(buf))))
        }
    }

    "produce nested subdocuments and arrays" in {
      val expected = Array[Byte](72, 0, 0, 0, 3, 112, 117, 115, 104, 65, 108, 108, 0, 58, 0, 0, 0, 4, 99, 111, 110, 102, 105, 103, 0, 45, 0, 0, 0, 3, 48, 0, 37, 0, 0, 0, 2, 110, 97, 109, 101, 0, 7, 0, 0, 0, 102, 111, 111, 98, 97, 114, 0, 2, 118, 97, 108, 117, 101, 0, 4, 0, 0, 0, 98, 97, 114, 0, 0, 0, 0, 0)

      // {"pushAll":{"config":[{"name":"foobar","value":"bar"}]}}
      val subsubdoc = BSONDocument("name" -> BSONString("foobar"), "value" -> BSONString("bar"))
      val arr = BSONArray(subsubdoc)
      val subdoc = BSONDocument("config" -> arr)
      val doc = BSONDocument("pushAll" -> subdoc)

      compare(expected, makeBuffer(doc))
    }

    "concat two arrays" in {
      val array1 = BSONArray(BSONInteger(1), BSONInteger(2))
      val array2 = BSONArray(BSONString("a"), BSONString("b"))
      val mergedArray = array1 ++ array2
      val str = mergedArray.values.map {
        case BSONString(value)  => value.toString
        case BSONInteger(value) => value.toString
        case _                  => "NOELEM"
      }.mkString(",")

      str must equalTo("1,2,a,b")
    }

    "build arrays with mixed values and optional values" in {
      val array = BSONArray(
        BSONInteger(1),
        Some(BSONInteger(2)),
        Option.empty[BSONValue],
        Some(BSONInteger(4)))

      val str = array.values.map {
        case BSONInteger(value) => value.toString
        case _                  => "NOELEM"
      }.mkString(",")

      str mustEqual "1,2,4"
    }

    "extract booleans and numbers" in {
      val docLike = BSONDocument(
        "likeFalseInt" -> BSONInteger(0),
        "likeFalseLong" -> BSONLong(0),
        "likeFalseDouble" -> BSONDouble(0.0),
        "likeFalseUndefined" -> BSONUndefined,
        "likeFalseNull" -> BSONNull,
        "likeTrueInt" -> BSONInteger(1),
        "likeTrueLong" -> BSONLong(2),
        "likeTrueDouble" -> BSONDouble(3D),
        "likeFalseNegDouble" -> BSONDouble(-0.1),
        "anInt" -> BSONInteger(200),
        "aLong" -> BSONLong(12345678912L),
        "aDouble" -> BSONDouble(9876543210.98))

      docLike.booleanLike("likeFalseInt") must beSome(false) and {
        docLike.booleanLike("likeFalseLong") must beSome(false)
      } and {
        docLike.booleanLike("likeFalseDouble") must beSome(false)
      } and {
        docLike.booleanLike("likeFalseUndefined") must beSome(false)
      } and {
        docLike.booleanLike("likeFalseNull") must beSome(false)
      } and {
        docLike.booleanLike("likeTrueInt") must beSome(true)
      } and {
        docLike.booleanLike("likeTrueLong") must beSome(true)
      } and {
        docLike.booleanLike("likeTrueDouble") must beSome(true)
      } and {
        docLike.booleanLike("likeFalseNegDouble") must beSome(false)
      } and {
        docLike.double("anInt") must beSome(200)
      } and {
        docLike.double("aLong") must beSome(12345678912L)
      } and {
        docLike.double("aDouble") must beSome(9876543210.98)
      }
    }
  }

  "Serialization pack" should {
    import reactivemongo.io.netty.channel.DefaultChannelId
    import reactivemongo.api.tests._

    val doc = BSONDocument("foo" -> 1, "bar" -> "LOREM")

    "parse a simple document" in {
      def buf = channelBuffer(doc).toReadableBuffer

      val ser = BSONSerializationPack

      makeDocument(buf) must_=== doc and {
        ser.readAndDeserialize(buf, ser.IdentityReader) must_=== doc
      }
    }

    "parse a message" in {
      def message = buffer.ReadableBuffer(bufferSeq(doc).merged)

      val ser = BSONSerializationPack

      makeDocument(message) must_=== doc and {
        ser.readAndDeserialize(message, ser.IdentityReader) must_=== doc
      }
    }

    "parse a response" in {
      val resp = fakeResponse(
        doc,
        reqID = isMasterReqId,
        respTo = 1,
        chanId = DefaultChannelId.newInstance())

      val ser = BSONSerializationPack

      ser.readAndDeserialize(resp, ser.IdentityReader) must_=== doc
    }
  }

  section("unit")

  // ---

  import reactivemongo.io.netty.buffer.ByteBuf

  def compare(origin: Array[Byte], buffer: ByteBuf) = {
    val array = new Array[Byte](buffer.writerIndex)

    buffer.readBytes(array)

    val result = array.corresponds(origin)(_ == _)

    if (!result) {
      log(origin, array, buffer)
      failure
    } else success
  }

  def log(origin: Array[Byte], test: Array[Byte], buffer: ByteBuf) = {
    println(Arrays.toString(origin))
    println(Arrays.toString(test))
    println(Arrays.toString(buffer.array()))
  }

  @inline def makeBuffer(doc: BSONDocument): ByteBuf =
    BSONSerializationPack.writeToBuffer(buffer.WritableBuffer.empty, doc).buffer

  @inline def makeDocument(buf: buffer.ReadableBuffer): BSONDocument =
    BSONSerializationPack.readFromBuffer(buf)
}
