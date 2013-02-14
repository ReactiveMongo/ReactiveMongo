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
import java.util.Arrays
import reactivemongo.bson.DefaultBSONHandlers._

import scala.util._

class Handlers extends Specification {
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
  
  "Complex Document" should {
    "have a name == 'James'" in {
      doc.getTry("name") mustEqual Success(Some(BSONString("James")))
      doc.getAsTry[BSONString]("name") mustEqual Success(Some(BSONString("James")))
      doc.getAsTry[String]("name") mustEqual Success(Some("James"))
      
      doc.getAsTry[BSONInteger]("name").isFailure mustEqual true
      doc.getAs[BSONInteger]("name") mustEqual None
      doc.getAsTry[Int]("name").isFailure mustEqual true
      doc.getAs[Int]("name") mustEqual None
      doc.getAsTry[BSONNumberLike]("name").isFailure mustEqual true
      
      doc.get("name").get.seeAsTry[String] mustEqual Success("James")
      doc.get("name").get.seeAsTry[Int].isFailure mustEqual true
      doc.get("name").get.seeAsOpt[String] mustEqual Some("James")
    }
    "have a scode == 3.88" in {
      doc.getTry("score") mustEqual Success(Some(BSONDouble(3.88)))
      doc.getAsTry[BSONDouble]("score") mustEqual Success(Some(BSONDouble(3.88)))
      doc.getAsTry[Double]("score") mustEqual Success(Some(3.88))
      
      doc.getAsTry[BSONInteger]("score").isFailure mustEqual true
      doc.getAsTry[Int]("score").isFailure mustEqual true
      
      val tryNumberLike = doc.getAsTry[BSONNumberLike]("score")
      tryNumberLike.isSuccess mustEqual true
      tryNumberLike.get.isDefined mustEqual true
      tryNumberLike.get.get.toDouble mustEqual 3.88
      tryNumberLike.get.get.toFloat mustEqual 3.88f
      tryNumberLike.get.get.toLong mustEqual 3
      tryNumberLike.get.get.toInt mustEqual 3
      
      val tryBooleanLike = doc.getAsTry[BSONBooleanLike]("score")
      tryBooleanLike.isSuccess mustEqual true
      tryBooleanLike.get.isDefined mustEqual true
      tryBooleanLike.get.get.toBoolean mustEqual true
    }
  }
}