import org.specs2.mutable._
import reactivemongo.bson._

import java.util.Arrays

class BsonSpec extends Specification {
  val simple = Array[Byte] (0x16, 0x00, 0x00, 0x00, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00, 0x06, 0x00, 0x00, 0x00, 'w', 'o', 'r', 'l', 'd', 0x00, 0x00)

  val embeddingArray = Array[Byte] (70, 0, 0, 0, 7, 95, 105, 100, 0, 80, 55, -110, -63, -104, 69, -121, -105, 27, 20, 83, 14, 4, 66, 83, 79, 78, 0, 42, 0, 0, 0, 2, 48, 0, 8, 0, 0, 0, 97, 119, 101, 115, 111, 109, 101, 0, 1, 49, 0, 51, 51, 51, 51, 51, 51, 20, 64, 1, 50, 0, 0, 0, 0, 0, 0, 8, -97, 64, 0, 0)

  val bsonArray = Array[Byte] (42, 0, 0, 0, 2, 48, 0, 8, 0, 0, 0, 97, 119, 101, 115, 111, 109, 101, 0, 1, 49, 0, 51, 51, 51, 51, 51, 51, 20, 64, 1, 50, 0, 0, 0, 0, 0, 0, 8, -97, 64, 0)

  "ReactiveMongo" should {
    "produce a simple doc" in {
      val buffer = BSONDocument("hello" -> BSONString("world")).makeBuffer
      compare(simple, buffer)
    }
    "produce a simple doc through a traversable" in {
      val buffer = BSONDocument("hello" -> BSONString("world")).makeBuffer
      val buffer2 = BSONDocument(buffer).makeBuffer
      compare(simple, buffer2)
    }
    "produce a simple doc through a traversable through appendable" in {
      val buffer = BSONDocument("hello" -> BSONString("world")).toTraversable.toAppendable.makeBuffer
      compare(simple, buffer)
    }
    "produce a document embedding an array" in {
      val buffer = BSONDocument(
        "_id" -> new BSONObjectID("503792c1984587971b14530e"),
          "BSON" -> BSONArray(
          BSONString("awesome"),
          BSONDouble(5.05),
          BSONDouble(1986)
        )).makeBuffer
      compare(embeddingArray, buffer)
    }
    "produce a document embedding an array through traversable" in {
      val buffer = BSONDocument(
        "_id" -> new BSONObjectID("503792c1984587971b14530e"),
          "BSON" -> BSONArray(
          BSONString("awesome"),
          BSONDouble(5.05),
          BSONDouble(1986)
        )).makeBuffer
      val buffer2 = BSONDocument(buffer).makeBuffer
      compare(embeddingArray, buffer2)
    }
    "produce a document embedding an array through traversable through appendable" in {
      val traversable = BSONDocument(
        "_id" -> new BSONObjectID("503792c1984587971b14530e"),
          "BSON" -> BSONArray(
          BSONString("awesome"),
          BSONDouble(5.05),
          BSONDouble(1986)
        )).toTraversable
      traversable.getAs[TraversableBSONArray]("BSON").get.buffer.array
      val buffer = traversable.toAppendable.makeBuffer
      compare(embeddingArray, buffer)
    }
    "produce an array through through traversable through appendable" in {
      val traversable = BSONArray(
        BSONString("awesome"),
        BSONDouble(5.05),
        BSONDouble(1986)
      ).toTraversable
      val buffer = traversable.toAppendable.makeBuffer
      compare(bsonArray, buffer)
    }
  }

  def compare(origin: Array[Byte], buffer: org.jboss.netty.buffer.ChannelBuffer) = {
    val array = new Array[Byte](buffer.writerIndex)
    buffer.readBytes(array)
    val result = array.corresponds(origin)(_ == _)

    if(!result) {
      log(origin, array, buffer)
      failure
    } else success
  }

  def log(origin: Array[Byte], test: Array[Byte], buffer: org.jboss.netty.buffer.ChannelBuffer) = {
    println(Arrays.toString(origin))
    println(Arrays.toString(test))
    println(Arrays.toString(buffer.array()))
  }
}