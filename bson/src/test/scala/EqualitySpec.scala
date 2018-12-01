package reactivemongo
package bson

import org.specs2.specification.core.Fragments

import reactivemongo.bson.buffer.{
  ArrayReadableBuffer,
  DefaultBufferHandler,
  ArrayBSONBuffer
}, DefaultBufferHandler._

class EqualitySpec extends org.specs2.mutable.Specification {
  "Equality" title

  section("unit")

  "BSONDBPointer" should {
    "permit equality to work" in {
      def create() = BSONDBPointer(
        "coll", Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

      create() must_=== create()
    }

    "retain equality through serialization/deserialization" in {
      val dbp1 = BSONDBPointer("coll", Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
      val writeBuffer = new ArrayBSONBuffer
      BSONDBPointerBufferHandler.write(dbp1, writeBuffer)
      val writeBytes = writeBuffer.array
      val readBuffer = ArrayReadableBuffer(writeBytes)
      val readBytes = readBuffer.slice(readBuffer.readable()).readArray(readBuffer.readable())

      writeBytes must_=== readBytes and {
        val bdp2 = BSONDBPointerBufferHandler.read(readBuffer)
        dbp1 must_=== bdp2
      }
    }
  }

  "BSONObjectID" should {
    "permit equality to work" in {
      val boid1 = BSONObjectID.parse("0102030405060708090a0b0c").get
      val boid2 = BSONObjectID.parse("0102030405060708090a0b0c").get

      boid1 must_=== boid2
    }

    "retain equality through serialization/deserialization" in {
      val boid1 = BSONObjectID.parse("0102030405060708090a0b0c").get
      val writeBuffer = new ArrayBSONBuffer
      BSONObjectIDBufferHandler.write(boid1, writeBuffer)
      val writeBytes = writeBuffer.array
      val readBuffer = ArrayReadableBuffer(writeBytes)
      val readBytes = readBuffer.slice(readBuffer.readable()).readArray(readBuffer.size)
      writeBytes must beEqualTo(readBytes)
      val boid2 = BSONObjectIDBufferHandler.read(readBuffer)
      boid1 must beEqualTo(boid2)
    }
  }

  "BSONArray" should {
    "permit equality to work" in {
      val ba1 = BSONArray(Seq(BSONInteger(42), BSONString("42"), BSONDouble(42.0), BSONDateTime(0)))
      val ba2 = ba1.copy()
      ba1 must beEqualTo(ba2)
    }

    "retain equality through serialization/deserialization" in {
      val ba1 = BSONArray(Seq(
        BSONInteger(42), BSONString("42"), BSONDouble(42.0), BSONDateTime(0)))

      val writeBuffer = new ArrayBSONBuffer
      BSONArrayBufferHandler.write(ba1, writeBuffer)

      val writeBytes = writeBuffer.array
      val readBuffer = ArrayReadableBuffer(writeBytes)
      val readBytes = readBuffer.slice(readBuffer.readable()).
        readArray(readBuffer.size)
      writeBytes must beEqualTo(readBytes)

      BSONArrayBufferHandler.read(readBuffer) must beTypedEqualTo(ba1)
    }
  }

  "BSONDocument" should {
    "retain equality through serialization/deserialization" in {
      val b1 = BSONDocument(Seq(
        "boolean" -> BSONBoolean(value = true),
        "int" -> BSONInteger(42),
        "long" -> BSONLong(42L),
        "double" -> BSONDouble(42.0),
        "string" -> BSONString("forty-two"),
        "datetime" -> BSONDateTime(System.currentTimeMillis()),
        "timestamp" -> BSONTimestamp(System.currentTimeMillis()),
        "binary" -> BSONBinary(Array[Byte](1, 2, 3), Subtype.GenericBinarySubtype),
        "objectid" -> BSONObjectID(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)),
        "dbpointer" -> BSONDBPointer("coll", Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)),
        "array" -> BSONArray(Seq(BSONInteger(42), BSONString("42"), BSONDouble(42.0), BSONDateTime(0)))))
      val writeBuffer = new ArrayBSONBuffer
      DefaultBufferHandler.write(writeBuffer, b1)
      val writeBytes = writeBuffer.array
      val readBuffer = ArrayReadableBuffer(writeBytes)
      val readBytes = readBuffer.slice(readBuffer.readable()).readArray(readBuffer.size)
      writeBytes must beEqualTo(readBytes)

      DefaultBufferHandler.readDocument(readBuffer).
        aka("result") must beSuccessfulTry(b1)
    }
  }

  "BSONBooleanLike" should {
    Fragments.foreach[BSONValue](
      BSONValueFixtures.bsonIntFixtures ++
        BSONValueFixtures.bsonDoubleFixtures ++
        BSONValueFixtures.bsonLongFixtures ++
        BSONValueFixtures.bsonBoolFixtures ++
        BSONValueFixtures.bsonDecimalFixtures ++
        Seq(BSONNull, BSONUndefined)) { v =>

        s"retain equality through handler for $v" in {
          BSON.read[BSONValue, BSONBooleanLike](v) must beTypedEqualTo(
            BSON.read[BSONValue, BSONBooleanLike](v))

        }
      }
  }

  "BSONNumberLike" should {
    Fragments.foreach[BSONValue](
      BSONValueFixtures.bsonIntFixtures ++
        BSONValueFixtures.bsonDoubleFixtures ++
        BSONValueFixtures.bsonLongFixtures ++
        BSONValueFixtures.bsonDateTimeFixtures ++
        BSONValueFixtures.bsonTsFixtures ++
        BSONValueFixtures.bsonDecimalFixtures) { v =>

        s"retain equality through handler for $v" in {
          BSON.read[BSONValue, BSONNumberLike](v) must beTypedEqualTo(
            BSON.read[BSONValue, BSONNumberLike](v))
        }
      }
  }

  section("unit")
}
