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
      println(s"\texpected:\n${Arrays.toString(a1)},\n\tgot:\n${Arrays.toString(a2)}")
      failure
    }
  }

  val expectedWholeDocumentBytes = Array[Byte](-45, 0, 0, 0, 2, 110, 97, 109, 101, 0, 6, 0, 0, 0, 74, 97, 109, 101, 115, 0, 16, 97, 103, 101, 0, 27, 0, 0, 0, 2, 115, 117, 114, 110, 97, 109, 101, 49, 0, 4, 0, 0, 0, 74, 105, 109, 0, 1, 115, 99, 111, 114, 101, 0, 10, -41, -93, 112, 61, 10, 15, 64, 8, 111, 110, 108, 105, 110, 101, 0, 1, 7, 95, 105, 100, 0, 81, 23, -58, 57, 26, -91, 98, -87, 0, -104, -10, 33, 3, 99, 111, 110, 116, 97, 99, 116, 0, 95, 0, 0, 0, 4, 101, 109, 97, 105, 108, 115, 0, 63, 0, 0, 0, 2, 48, 0, 18, 0, 0, 0, 106, 97, 109, 101, 115, 64, 101, 120, 97, 109, 112, 108, 101, 46, 111, 114, 103, 0, 2, 49, 0, 26, 0, 0, 0, 115, 112, 97, 109, 97, 100, 100, 114, 106, 97, 109, 101, 115, 64, 101, 120, 97, 109, 112, 108, 101, 46, 111, 114, 103, 0, 0, 2, 97, 100, 114, 101, 115, 115, 0, 7, 0, 0, 0, 99, 111, 117, 99, 111, 117, 0, 0, 18, 108, 97, 115, 116, 83, 101, 101, 110, 0, -21, 96, -32, -60, 60, 1, 0, 0, 0)

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
            Some("spamaddrjames@example.org")),
          "adress" -> BSONString("coucou")),
        "lastSeen" -> BSONLong(1360512704747L))
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
          BSONString("spamaddrjames@example.org")),
        "address" -> BSONString("coucou")))
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
      doc.recover {
        case e => e.printStackTrace()
      }
      doc.isSuccess mustEqual true
    }
  }
}