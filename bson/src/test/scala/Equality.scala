import org.specs2.mutable._
import reactivemongo.bson._
import reactivemongo.bson.buffer.DefaultBufferHandler._
import reactivemongo.bson.buffer.{ ArrayReadableBuffer, DefaultBufferHandler, ArrayBSONBuffer }

class Equality extends Specification {

  "BSONDBPointer" should {
    "permit equality to work" in {
      val dbp1 = BSONDBPointer("coll", Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
      val dbp2 = dbp1.copy()
      dbp1 must beEqualTo(dbp2)
    }
    "retain equality through serialization/deserialization" in {
      val dbp1 = BSONDBPointer("coll", Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
      val writeBuffer = new ArrayBSONBuffer
      BSONDBPointerBufferHandler.write(dbp1, writeBuffer)
      val writeBytes = writeBuffer.array
      val readBuffer = ArrayReadableBuffer(writeBytes)
      val readBytes = readBuffer.slice(readBuffer.readable()).readArray(readBuffer.readable())
      writeBytes must beEqualTo(readBytes)
      val bdp2 = BSONDBPointerBufferHandler.read(readBuffer)
      dbp1 must beEqualTo(bdp2)
    }
  }

  "BSONObjectID" should {
    "permit equality to work" in {
      val boid1 = BSONObjectID("0102030405060708090a0b0c")
      val boid2 = BSONObjectID("0102030405060708090a0b0c")
      boid1 must beEqualTo(boid2)
    }
    "retain equality through serialization/deserialization" in {
      val boid1 = BSONObjectID("0102030405060708090a0b0c")
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
      val ba1 = BSONArray(Seq(BSONInteger(42), BSONString("42"), BSONDouble(42.0), BSONDateTime(0)))
      val writeBuffer = new ArrayBSONBuffer
      BSONArrayBufferHandler.write(ba1, writeBuffer)
      val writeBytes = writeBuffer.array
      val readBuffer = ArrayReadableBuffer(writeBytes)
      val readBytes = readBuffer.slice(readBuffer.readable()).readArray(readBuffer.size)
      writeBytes must beEqualTo(readBytes)
      val ba2 = BSONArrayBufferHandler.read(readBuffer)
      ba1 must beEqualTo(ba2)
    }
  }

  "BSONDocument" should {
    "retain equality through serialization/deserialization" in {
      val b1 = BSONDocument(Seq(
        "boolean" → BSONBoolean(value = true),
        "int" → BSONInteger(42),
        "long" → BSONLong(42L),
        "double" → BSONDouble(42.0),
        "string" → BSONString("forty-two"),
        "datetime" → BSONDateTime(System.currentTimeMillis()),
        "timestamp" → BSONTimestamp(System.currentTimeMillis()),
        "binary" → BSONBinary(Array[Byte](1, 2, 3), Subtype.GenericBinarySubtype),
        "objectid" → BSONObjectID(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)),
        "dbpointer" → BSONDBPointer("coll", Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)),
        "array" → BSONArray(Seq(BSONInteger(42), BSONString("42"), BSONDouble(42.0), BSONDateTime(0)))
      ))
      val writeBuffer = new ArrayBSONBuffer
      DefaultBufferHandler.write(writeBuffer, b1)
      val writeBytes = writeBuffer.array
      val readBuffer = ArrayReadableBuffer(writeBytes)
      val readBytes = readBuffer.slice(readBuffer.readable()).readArray(readBuffer.size)
      writeBytes must beEqualTo(readBytes)
      val result = DefaultBufferHandler.readDocument(readBuffer)
      result.isSuccess must beTrue
      val b2 = result.get
      b1 must beEqualTo(b2)
    }
  }

}
