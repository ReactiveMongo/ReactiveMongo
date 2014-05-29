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
}
