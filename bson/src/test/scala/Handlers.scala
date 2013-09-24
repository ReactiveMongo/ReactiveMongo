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
import reactivemongo.bson.exceptions._
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

  val array = BSONArray(
    BSONString("elem0"),
    None,
    1,
    2.222,
    BSONDocument(
      "name" -> "Joe"),
    BSONArray(0L),
    "pp[4]")

  "Complex Document" should {
    "have a name == 'James'" in {
      doc.getTry("name") mustEqual Success(BSONString("James"))
      doc.getAsTry[BSONString]("name") mustEqual Success(BSONString("James"))
      doc.getAsTry[String]("name") mustEqual Success("James")

      doc.getAsTry[BSONInteger]("name").isFailure mustEqual true
      doc.getAsTry[BSONInteger]("name").get must throwA[DeserializationException]
      doc.getAs[BSONInteger]("name") mustEqual None
      doc.getAsTry[Int]("name").isFailure mustEqual true
      doc.getAsTry[Int]("name").get must throwA[DeserializationException]
      doc.getAs[Int]("name") mustEqual None
      doc.getAsTry[BSONNumberLike]("name").isFailure mustEqual true
      doc.getAsTry[BSONNumberLike]("name").get must throwA[UnsupportedOperationException]

      doc.get("name").get.seeAsTry[String] mustEqual Success("James")
      doc.get("name").get.seeAsTry[Int].isFailure mustEqual true
      doc.get("name").get.seeAsOpt[String] mustEqual Some("James")
    }
    "have a score == 3.88" in {
      doc.getTry("score") mustEqual Success(BSONDouble(3.88))
      doc.getAsTry[BSONDouble]("score") mustEqual Success(BSONDouble(3.88))
      doc.getAsTry[Double]("score") mustEqual Success(3.88)

      doc.getAsTry[BSONInteger]("score").isFailure mustEqual true
      doc.getAsTry[Int]("score").isFailure mustEqual true

      doc.getAsUnflattenedTry[BSONInteger]("score").isFailure mustEqual true
      doc.getAsUnflattenedTry[BSONDouble]("score").get.isDefined mustEqual true

      val tryNumberLike = doc.getAsTry[BSONNumberLike]("score")
      tryNumberLike.isSuccess mustEqual true
      tryNumberLike.get.toDouble mustEqual 3.88
      tryNumberLike.get.toFloat mustEqual 3.88f
      tryNumberLike.get.toLong mustEqual 3
      tryNumberLike.get.toInt mustEqual 3

      val tryBooleanLike = doc.getAsTry[BSONBooleanLike]("score")
      tryBooleanLike.isSuccess mustEqual true
      tryBooleanLike.get.toBoolean mustEqual true
    }
    "should not have a surname2" in {
      doc.getTry("surname2").isFailure mustEqual true
      doc.getUnflattenedTry("surname2").isSuccess mustEqual true
      doc.getUnflattenedTry("surname2").get.isDefined mustEqual false
    }
  }

  "Document" should {
    val doc = BSONDocument("test" -> BSONNull, "test2" -> BSONUndefined)

    "handle 'null'" in {
      doc.getAs[BSONString]("test") mustEqual None
      doc.getAs[String]("test") mustEqual None
      doc.getAsTry[BSONString]("test") mustEqual Failure(ValueIsNull)
      doc.getAsTry[String]("test") mustEqual Failure(ValueIsNull)
      doc.getAsTry[BSONBoolean]("test") mustEqual Failure(ValueIsNull)
      doc.getAsTry[Boolean]("test") mustEqual Failure(ValueIsNull)
      doc.getAsUnflattenedTry[BSONString]("test") mustEqual Success(None)
      doc.getAsUnflattenedTry[String]("test") mustEqual Success(None)
      doc.getAsUnflattenedTry[BSONBoolean]("test") mustEqual Success(None)
      doc.getAsUnflattenedTry[Boolean]("test") mustEqual Success(None)
      doc.getAs[BSONBoolean]("test") mustEqual None
      doc.getAs[Boolean]("test") mustEqual None
      doc.getAs[BSONBooleanLike]("test").get.toBoolean mustEqual false
    }

    "handle 'key not found'" in {
      doc.getAs[BSONString]("test1") mustEqual None
      doc.getAs[String]("test1") mustEqual None
      doc.getAsTry[BSONString]("test1") mustEqual Failure(DocumentKeyNotFound("test1"))
      doc.getAsTry[String]("test1") mustEqual Failure(DocumentKeyNotFound("test1"))
      doc.getAsTry[BSONBoolean]("test1") mustEqual Failure(DocumentKeyNotFound("test1"))
      doc.getAsTry[Boolean]("test1") mustEqual Failure(DocumentKeyNotFound("test1"))
      doc.getAsUnflattenedTry[BSONString]("test1") mustEqual Success(None)
      doc.getAsUnflattenedTry[String]("test1") mustEqual Success(None)
      doc.getAsUnflattenedTry[BSONBoolean]("test1") mustEqual Success(None)
      doc.getAsUnflattenedTry[Boolean]("test1") mustEqual Success(None)
      doc.getAs[BSONBoolean]("test1") mustEqual None
      doc.getAs[Boolean]("test1") mustEqual None
      doc.getAsTry[BSONBooleanLike]("test1") mustEqual Failure(DocumentKeyNotFound("test1"))
    }

    "handle 'undefined'" in {
      doc.getAs[BSONString]("test2") mustEqual None
      doc.getAs[String]("test2") mustEqual None
      doc.getAsTry[BSONString]("test2") mustEqual Failure(ValueIsNull)
      doc.getAsTry[String]("test2") mustEqual Failure(ValueIsNull)
      doc.getAsTry[BSONBoolean]("test2") mustEqual Failure(ValueIsNull)
      doc.getAsTry[Boolean]("test2") mustEqual Failure(ValueIsNull)
      doc.getAsUnflattenedTry[BSONString]("test2") mustEqual Success(None)
      doc.getAsUnflattenedTry[String]("test2") mustEqual Success(None)
      doc.getAsUnflattenedTry[BSONBoolean]("test2") mustEqual Success(None)
      doc.getAsUnflattenedTry[Boolean]("test2") mustEqual Success(None)
      doc.getAs[BSONBoolean]("test2") mustEqual None
      doc.getAs[Boolean]("test2") mustEqual None
      doc.getAs[BSONBooleanLike]("test").get.toBoolean mustEqual false
    }
  }

  "Complex Array" should {
    "be of size = 6" in {
      array.length mustEqual 6
    }
    "have a an int = 2 at index 2" in {
      array.get(1).isDefined mustEqual true
      array.get(1).get mustEqual BSONInteger(1)
      array.getAs[Int](1) mustEqual Some(1)
    }
    "get bsondocunment at index 3" in {
      val maybedoc = array.getAs[BSONDocument](3)
      maybedoc.isDefined mustEqual true
      val maybename = maybedoc.get.getAs[String]("name")
      maybename.isDefined mustEqual true
      maybename.get mustEqual "Joe"
    }
    "get bsonarray at index 4" in {
      val tdoc = array.getAsTry[BSONDocument](4)
      tdoc.isFailure mustEqual true
      tdoc.failed.get.isInstanceOf[exceptions.DocumentKeyNotFound] mustEqual false
      val tarray = array.getAsTry[BSONArray](4)
      tarray.isSuccess mustEqual true
      val olong = tarray.get.getAs[BSONLong](0)
      olong.isDefined mustEqual true
      olong.get mustEqual BSONLong(0L)
      val booleanlike = tarray.get.getAs[BSONBooleanLike](0)
      booleanlike.isDefined mustEqual true
      booleanlike.get.toBoolean mustEqual false
    }
  }

  case class Album(
    name: String,
    releaseYear: Int,
    tracks: List[String])

  case class Artist(
    name: String,
    albums: List[Album])

  val neilYoung = Artist(
    "Neil Young",
    List(
      Album(
        "Everybody Knows this is Nowhere",
        1969,
        List(
          "Cinnamon Girl",
          "Everybody Knows this is Nowhere",
          "Round & Round (it Won't Be Long)",
          "Down By the River",
          "Losing End (When You're On)",
          "Running Dry (Requiem For the Rockets)",
          "Cowgirl in the Sand"))))

  implicit object AlbumHandler extends BSONDocumentWriter[Album] with BSONDocumentReader[Album] {
    def write(album: Album) = BSONDocument(
      "name" -> album.name,
      "releaseYear" -> album.releaseYear,
      "tracks" -> album.tracks)

    def read(doc: BSONDocument) = Album(
      doc.getAs[String]("name").get,
      doc.getAs[Int]("releaseYear").get,
      doc.getAs[List[String]]("tracks").get)
  }

  implicit object ArtistHandler extends BSONDocumentWriter[Artist] with BSONDocumentReader[Artist] {
    def write(artist: Artist) =
      BSONDocument(
        "name" -> artist.name,
        "albums" -> artist.albums)

    def read(doc: BSONDocument) = {
      Artist(doc.getAs[String]("name").get, doc.getAs[List[Album]]("albums").get)
    }
  }

  "Neil Young" should {
    "produce the expected BSONDocument" in {
      val doc = BSON.write(neilYoung)
      BSONDocument.pretty(doc) mustEqual """{
  name: BSONString(Neil Young),
  albums: [
    0: {
      name: BSONString(Everybody Knows this is Nowhere),
      releaseYear: BSONInteger(1969),
      tracks: [
        0: BSONString(Cinnamon Girl),
        1: BSONString(Everybody Knows this is Nowhere),
        2: BSONString(Round & Round (it Won't Be Long)),
        3: BSONString(Down By the River),
        4: BSONString(Losing End (When You're On)),
        5: BSONString(Running Dry (Requiem For the Rockets)),
        6: BSONString(Cowgirl in the Sand)
      ]
    }
  ]
}"""
      val ny2 = BSON.readDocument[Artist](doc)
      val allSongs = doc.getAs[List[Album]]("albums").toList.flatten.flatMap(_.tracks)
      allSongs mustEqual List(
        "Cinnamon Girl",
        "Everybody Knows this is Nowhere",
        "Round & Round (it Won't Be Long)",
        "Down By the River",
        "Losing End (When You're On)",
        "Running Dry (Requiem For the Rockets)",
        "Cowgirl in the Sand")
      ny2 mustEqual neilYoung
      success
    }
  }
}