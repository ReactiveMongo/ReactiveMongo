/*
 * Copyright 2013 Stephane Godbillon
 * @sgodbillon
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.specs2.mutable._
import reactivemongo.bson._
import buffer._
import java.util.Arrays
import reactivemongo.bson.DefaultBSONHandlers._

import java.nio.ByteBuffer
import java.util.Arrays

class SerializationSpecs extends Specification {

  def compare(a1: Array[Byte], a2: Array[Byte]) = {
    if (Arrays.equals(a1, a2)) {
      success
    } else {
      //println(s"\texpected:\n${Arrays.toString(a1)},\n\tgot:\n${Arrays.toString(a2)}")
      failure
    }
  }

  val ismaster = Array[Byte](-72, 1, 0, 0, 2, 115, 101, 116, 78, 97, 109, 101, 0, 7, 0, 0, 0, 114, 101, 97, 99, 116, 109, 0, 16, 115, 101, 116, 86, 101, 114, 115, 105, 111, 110, 0, 3, 0, 0, 0, 8, 105, 115, 109, 97, 115, 116, 101, 114, 0, 1, 8, 115, 101, 99, 111, 110, 100, 97, 114, 121, 0, 0, 4, 104, 111, 115, 116, 115, 0, -122, 0, 0, 0, 2, 48, 0, 36, 0, 0, 0, 83, 116, 101, 112, 104, 97, 110, 101, 115, 45, 77, 97, 99, 66, 111, 111, 107, 45, 80, 114, 111, 45, 50, 46, 108, 111, 99, 97, 108, 58, 50, 55, 48, 49, 55, 0, 2, 49, 0, 36, 0, 0, 0, 83, 116, 101, 112, 104, 97, 110, 101, 115, 45, 77, 97, 99, 66, 111, 111, 107, 45, 80, 114, 111, 45, 50, 46, 108, 111, 99, 97, 108, 58, 50, 55, 48, 49, 57, 0, 2, 50, 0, 36, 0, 0, 0, 83, 116, 101, 112, 104, 97, 110, 101, 115, 45, 77, 97, 99, 66, 111, 111, 107, 45, 80, 114, 111, 45, 50, 46, 108, 111, 99, 97, 108, 58, 50, 55, 48, 49, 56, 0, 0, 2, 112, 114, 105, 109, 97, 114, 121, 0, 36, 0, 0, 0, 83, 116, 101, 112, 104, 97, 110, 101, 115, 45, 77, 97, 99, 66, 111, 111, 107, 45, 80, 114, 111, 45, 50, 46, 108, 111, 99, 97, 108, 58, 50, 55, 48, 49, 55, 0, 2, 109, 101, 0, 36, 0, 0, 0, 83, 116, 101, 112, 104, 97, 110, 101, 115, 45, 77, 97, 99, 66, 111, 111, 107, 45, 80, 114, 111, 45, 50, 46, 108, 111, 99, 97, 108, 58, 50, 55, 48, 49, 55, 0, 16, 109, 97, 120, 66, 115, 111, 110, 79, 98, 106, 101, 99, 116, 83, 105, 122, 101, 0, 0, 0, 0, 1, 16, 109, 97, 120, 77, 101, 115, 115, 97, 103, 101, 83, 105, 122, 101, 66, 121, 116, 101, 115, 0, 0, 108, -36, 2, 16, 109, 97, 120, 87, 114, 105, 116, 101, 66, 97, 116, 99, 104, 83, 105, 122, 101, 0, -24, 3, 0, 0, 9, 108, 111, 99, 97, 108, 84, 105, 109, 101, 0, 6, 89, 36, -79, 72, 1, 0, 0, 16, 109, 97, 120, 87, 105, 114, 101, 86, 101, 114, 115, 105, 111, 110, 0, 2, 0, 0, 0, 16, 109, 105, 110, 87, 105, 114, 101, 86, 101, 114, 115, 105, 111, 110, 0, 0, 0, 0, 0, 1, 111, 107, 0, 0, 0, 0, 0, 0, 0, -16, 63, 0)
  val cpxDoc = Array[Byte](73, 1, 0, 0, 7, 95, 105, 100, 0, 84, 37, 32, -44, 86, 16, 26, 53, 33, 121, 71, -94, 2, 110, 97, 109, 101, 0, 5, 0, 0, 0, 106, 97, 99, 107, 0, 4, 99, 111, 110, 116, 97, 99, 116, 0, 113, 0, 0, 0, 3, 48, 0, 52, 0, 0, 0, 2, 116, 112, 101, 0, 4, 0, 0, 0, 116, 101, 108, 0, 2, 110, 117, 109, 0, 11, 0, 0, 0, 56, 55, 54, 56, 49, 55, 50, 54, 51, 56, 0, 1, 116, 114, 117, 99, 0, 0, 0, 0, 0, 0, 0, 88, 64, 0, 3, 49, 0, 50, 0, 0, 0, 2, 116, 112, 101, 0, 4, 0, 0, 0, 112, 114, 111, 0, 2, 110, 117, 109, 0, 9, 0, 0, 0, 48, 57, 56, 55, 49, 48, 57, 50, 0, 1, 116, 114, 117, 99, 0, -51, -52, -52, -52, -52, 76, 49, -64, 0, 0, 2, 97, 100, 100, 114, 101, 115, 115, 0, 36, 0, 0, 0, 108, 107, 110, 99, 118, 101, 111, 119, 110, 118, 101, 111, 119, 110, 118, 32, 119, 111, 105, 118, 110, 101, 119, 59, 111, 118, 110, 32, 113, 39, 112, 106, 102, 110, 32, 0, 13, 102, 117, 110, 99, 0, 44, 0, 0, 0, 102, 117, 110, 99, 116, 105, 111, 110, 32, 40, 41, 32, 123, 32, 118, 97, 114, 32, 97, 32, 61, 32, 57, 57, 59, 32, 114, 101, 116, 117, 114, 110, 32, 104, 101, 121, 32, 43, 32, 57, 57, 32, 125, 0, 5, 100, 97, 116, 97, 0, 9, 0, 0, 0, 0, -115, -86, -33, -45, -35, 123, -37, -121, -32, 11, 114, 120, 0, 91, 97, 122, 93, 123, 52, 125, 0, 105, 0, 7, 105, 100, 50, 0, 84, 37, 32, -44, 86, 16, 26, 53, 33, 121, 71, -95, 2, 101, 110, 100, 0, 7, 0, 0, 0, 101, 110, 102, 105, 110, 33, 0, 0)

  val expectedWholeDocumentBytes = Array[Byte](-45, 0, 0, 0, 2, 110, 97, 109, 101, 0, 6, 0, 0, 0, 74, 97, 109, 101, 115, 0, 16, 97, 103, 101, 0, 27, 0, 0, 0, 2, 115, 117, 114, 110, 97, 109, 101, 49, 0, 4, 0, 0, 0, 74, 105, 109, 0, 1, 115, 99, 111, 114, 101, 0, 10, -41, -93, 112, 61, 10, 15, 64, 8, 111, 110, 108, 105, 110, 101, 0, 1, 7, 95, 105, 100, 0, 81, 23, -58, 57, 26, -91, 98, -87, 0, -104, -10, 33, 3, 99, 111, 110, 116, 97, 99, 116, 0, 95, 0, 0, 0, 4, 101, 109, 97, 105, 108, 115, 0, 63, 0, 0, 0, 2, 48, 0, 18, 0, 0, 0, 106, 97, 109, 101, 115, 64, 101, 120, 97, 109, 112, 108, 101, 46, 111, 114, 103, 0, 2, 49, 0, 26, 0, 0, 0, 115, 112, 97, 109, 97, 100, 100, 114, 106, 97, 109, 101, 115, 64, 101, 120, 97, 109, 112, 108, 101, 46, 111, 114, 103, 0, 0, 2, 97, 100, 114, 101, 115, 115, 0, 7, 0, 0, 0, 99, 111, 117, 99, 111, 117, 0, 0, 18, 108, 97, 115, 116, 83, 101, 101, 110, 0, -21, 96, -32, -60, 60, 1, 0, 0, 0)

  "BSON Raw Buffer Writer" should {
    import reactivemongo.bson.lowlevel._

    "serialize a whole document" in {
      val buf = new LowLevelBsonDocWriter(new ArrayBSONBuffer)
      buf.
        putString("name", "James").
        putInt("age", 27).
        putString("surname1", "Jim").
        putDouble("score", 3.88).
        putBoolean("online", true).
        putObjectId("_id", BSONObjectID("5117c6391aa562a90098f621").valueAsArray).
        openDocument("contact").
        openArray("emails").
        putString("0", "james@example.org").
        putString("1", "spamaddrjames@example.org").
        close.
        putString("adress", "coucou").
        close.
        putLong("lastSeen", 1360512704747L).
        close
      compare(expectedWholeDocumentBytes, buf.result.array)
    }

    "list all fields in a bson doc" in {
      /*val doc = BSONDocument(
        "name" -> "James",
        "age" -> 27,
        "surname1" -> Some("Jim"),
        "surname2" -> None,
        "score" -> 3.88,
        "online" -> true,
        "_id" -> BSONObjectID("5117c6391aa562a90098f621"),
        "contact" -> BSONDocument(
          "emails" -> BSONArray(
            Some("james@example.org"),
            None,
            Some("spamaddrjames@example.org")),
          "adress" -> BSONString("coucou")),
        "lastSeen" -> BSONLong(1360512704747L))
      val buffer = new ArrayBSONBuffer
       DefaultBufferHandler.write(buffer, doc)*/

      def listAll[A <: ReadableBuffer](buf: LowLevelBsonDocReader[A], spaces: Int = 0): Unit = {
        //val buf = new LowLevelBsonDocReader(b)
        buf.fieldStream.map { f =>
          //print(" " * spaces)

          f match {
            case StructureField(_, _, b) =>
              //println(f.name)
              listAll(b, spaces + 2)
            case LazyField(tpe, _, b) =>
              if (tpe == 0x02 || tpe == 0x0D || tpe == 0x0E) {
                //println(s"${f.name} -> ${b.readString}")
              } else if (tpe == 0x05) {
                val length = b.readInt
                val subtype = b.readByte
                //println(s"l=$length, subtype=$subtype, readable=${b.readable}")
                val contents = b.readArray(length)
                //println(s"${f.name} -> binary (l=$length, subtype=$subtype, hex=${utils.Converters.hex2Str(contents)} contents=${contents.mkString(", ")})")
              } else {
                //println(s"${f.name} -> <${b.readable} bytes>")
              }
            case v: ValueField[_] => {
              //println(s"${f.name} -> ${v.value}")
            }

            case NoValue(tpe, _) => {
              //println(s"${f.name} -> <singleton $tpe>")
            }
          }
          //if(f.tpe == 0x03 || f.tpe == 0x04)
          //  listAll(f.value, spaces + 2)
        }.force
        /*val fields = buf.fieldStream.map(println(_)).force
        for(f <- fields) {
          if(f.tpe == 0x03 || f.tpe == 0x04)
            new LowLevelBsonDocReader(f.value)
        }*/
      }
      listAll(new LowLevelBsonDocReader(ArrayReadableBuffer(cpxDoc)))
      //println(fields.toList)
      true mustEqual true
    }
  }

  "BSON Default Serializer" should {
    "serialize a whole document" in {
      val doc = BSONDocument(
        "name" -> "James",
        "age" -> 27,
        "surname1" -> Some("Jim"),
        "surname2" -> None,
        "score" -> 3.88,
        "online" -> true,
        "_id" -> BSONObjectID("5117c6391aa562a90098f621"),
        "contact" -> BSONDocument(
          "emails" -> BSONArray(
            Some("james@example.org"),
            None,
            Some("spamaddrjames@example.org")
          ),
          "adress" -> BSONString("coucou")
        ),
        "lastSeen" -> BSONLong(1360512704747L)
      )
      val buffer = new ArrayBSONBuffer
      DefaultBufferHandler.write(buffer, doc)
      compare(expectedWholeDocumentBytes, buffer.array)
    }

    "serialize a document containing a boolean" in {
      val dbool = BSONDocument("boo" -> BSONBoolean(true))
      val expected = Array[Byte](11, 0, 0, 0, 8, 98, 111, 111, 0, 1, 0)
      val buffer = new ArrayBSONBuffer
      DefaultBufferHandler.write(buffer, dbool)
      compare(expected, buffer.array)
    }
    "serialize a document containing a double" in {
      val ddoub = BSONDocument("doo" -> BSONDouble(9))
      val expected = Array[Byte](18, 0, 0, 0, 1, 100, 111, 111, 0, 0, 0, 0, 0, 0, 0, 34, 64, 0)
      val buffer = new ArrayBSONBuffer
      DefaultBufferHandler.write(buffer, ddoub)
      compare(expected, buffer.array)
    }
    "serialize a document containing an integer" in {
      val dint = BSONDocument("int" -> BSONInteger(8))
      val expected = Array[Byte](14, 0, 0, 0, 16, 105, 110, 116, 0, 8, 0, 0, 0, 0)
      val buffer = new ArrayBSONBuffer
      DefaultBufferHandler.write(buffer, dint)
      compare(expected, buffer.array)
    }
    "serialize a document containing a string" in {
      val dstr = BSONDocument("str" -> BSONString("str"))
      val expected = Array[Byte](18, 0, 0, 0, 2, 115, 116, 114, 0, 4, 0, 0, 0, 115, 116, 114, 0, 0)
      val buffer = new ArrayBSONBuffer
      DefaultBufferHandler.write(buffer, dstr)
      compare(expected, buffer.array)
    }
    "serialize a document containing an array" in {
      val arr = BSONArray(BSONBoolean(true))
      val expected = Array[Byte](9, 0, 0, 0, 8, 48, 0, 1, 0)
      val buffer = new ArrayBSONBuffer
      DefaultBufferHandler.write(buffer, arr)
      compare(expected, buffer.array)
    }
    "serialize a document containing a document containing a document" in {
      val docdoc = BSONDocument("doc" -> BSONDocument("str" -> BSONString("strv")))
      val expected = Array[Byte](29, 0, 0, 0, 3, 100, 111, 99, 0, 19, 0, 0, 0, 2, 115, 116, 114, 0, 5, 0, 0, 0, 115, 116, 114, 118, 0, 0, 0)
      val buffer = new ArrayBSONBuffer
      DefaultBufferHandler.write(buffer, docdoc)
      compare(expected, buffer.array)
    }
    "serialize a document containing an array" in {
      val docarray = BSONDocument("contact" -> BSONDocument(
        "emails" -> BSONArray(
          BSONString("james@example.org"),
          BSONString("spamaddrjames@example.org")
        ),
        "address" -> BSONString("coucou")
      ))
      val expected = Array[Byte](110, 0, 0, 0, 3, 99, 111, 110, 116, 97, 99, 116, 0, 96, 0, 0, 0, 4, 101, 109, 97, 105, 108, 115, 0, 63, 0, 0, 0, 2, 48, 0, 18, 0, 0, 0, 106, 97, 109, 101, 115, 64, 101, 120, 97, 109, 112, 108, 101, 46, 111, 114, 103, 0, 2, 49, 0, 26, 0, 0, 0, 115, 112, 97, 109, 97, 100, 100, 114, 106, 97, 109, 101, 115, 64, 101, 120, 97, 109, 112, 108, 101, 46, 111, 114, 103, 0, 0, 2, 97, 100, 100, 114, 101, 115, 115, 0, 7, 0, 0, 0, 99, 111, 117, 99, 111, 117, 0, 0, 0)
      val buffer = new ArrayBSONBuffer
      DefaultBufferHandler.write(buffer, docarray)
      compare(expected, buffer.array)
    }
    "serialize a document containing a long" in {
      val dlong = BSONDocument("long" -> BSONLong(8888122134234l))
      val expected = Array[Byte](19, 0, 0, 0, 18, 108, 111, 110, 103, 0, -38, -50, 92, 109, 21, 8, 0, 0, 0)
      val buffer = new ArrayBSONBuffer
      DefaultBufferHandler.write(buffer, dlong)
      compare(expected, buffer.array)
    }
  }

  "BSON Default Deserializer" should {
    "deserialize a complex document" in {
      val buffer = ArrayReadableBuffer(expectedWholeDocumentBytes)
      val doc = DefaultBufferHandler.readDocument(buffer)

      doc.isSuccess mustEqual true
    }
  }
}
