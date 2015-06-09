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
import scala.util._

class Types extends Specification {

  "Generating BSONObjectID" should {
    "not throw a SocketException" in {

      /*
       * for i in `seq 1 257`; do
       *   openvpn --mktun --dev tun$i
       *   ip link set tun$i up
       *   ip -6 addr add 2001:DB8::`printf %04x $i`/128 dev tun$i
       * done
       */
      BSONObjectID.generate must beAnInstanceOf[BSONObjectID]
    }
  }

  "BSON document" should {
    "be empty" in {
      BSONDocument().elements must beEmpty and (
        BSONDocument.empty.elements must beEmpty) and (
        document.elements must beEmpty) and (
        document().elements must beEmpty)
    }

    "be created with a new element " in {
      val doc = BSONDocument.empty ++ ("foo" -> 1)
      doc must_== BSONDocument("foo" -> 1)
    }
  }

  "BSON array" should {
    "be empty" in {
      BSONArray().values must beEmpty and (
        BSONArray.empty.values must beEmpty) and (
        array.values must beEmpty) and (
        array().values must beEmpty)

    }

    "be created with a new element " in {
      val arr = BSONArray.empty ++ ("foo", "bar")
      arr must_== BSONArray("foo", "bar")
    }
  }

  "BSONBinary" should {
    "be read as byte array" in {
      val bytes = Array[Byte](1,2,3)
      val bson = BSONBinary(bytes, Subtype.GenericBinarySubtype)

      bson.byteArray must_== bytes
    }
  }
}
