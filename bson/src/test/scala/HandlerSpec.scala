package reactivemongo.bson

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
import java.util.Date

class HandlerSpec extends org.specs2.mutable.Specification {
  "Handler" title

  section("unit")

  "BSONBinary" should {
    import reactivemongo.bson.buffer.ArrayReadableBuffer

    "be read as byte array" in {
      val bytes = Array[Byte](1, 3, 5, 7)
      val bin = BSONBinary(
        ArrayReadableBuffer(bytes), Subtype.GenericBinarySubtype)

      bin.as[Array[Byte]] aka "read #1" must_=== bytes and (
        bin.as[Array[Byte]] aka "read #2" must_=== bytes)
    }
  }

  "Complex Document" should {
    lazy val doc = BSONDocument(
      "name" -> "James",
      "age" -> 27,
      "surname1" -> Some("Jim"),
      "surname2" -> None,
      "score" -> 3.88,
      "online" -> true,
      "_id" -> BSONObjectID.parse("5117c6391aa562a90098f621").get, // !!
      "contact" -> BSONDocument(
        "emails" -> BSONArray(
          Some("james@example.org"),
          None,
          Some("spamaddrjames@example.org")),
        "adress" -> BSONString("coucou")),
      "lastSeen" -> BSONLong(1360512704747L))

    "have a name == 'James'" in {
      doc.getTry("name") must beSuccessfulTry(BSONString("James"))
      doc.getAsTry[BSONString]("name") must beSuccessfulTry(BSONString("James"))
      doc.getAsTry[String]("name") must beSuccessfulTry("James")

      doc.getAsTry[BSONInteger]("name") must beFailedTry
      doc.getAs[BSONInteger]("name") must beNone
      doc.getAsTry[Int]("name") must beFailedTry
      doc.getAs[Int]("name") must beNone
      doc.getAsTry[BSONNumberLike]("name") must beFailedTry

      doc.get("name").get.seeAsTry[String] must beSuccessfulTry("James")
      doc.get("name").get.seeAsTry[Int] must beFailedTry
      doc.get("name").get.seeAsOpt[String] must beSome("James")
    }

    "have a score == 3.88" in {
      doc.getTry("score") must beSuccessfulTry(BSONDouble(3.88))
      doc.getAsTry[BSONDouble]("score") must beSuccessfulTry(BSONDouble(3.88))
      doc.getAsTry[Double]("score") must beSuccessfulTry(3.88)

      doc.getAsTry[BSONInteger]("score") must beFailedTry
      doc.getAsTry[Int]("score") must beFailedTry

      doc.getAsUnflattenedTry[BSONInteger]("score") must beFailedTry
      doc.getAsUnflattenedTry[BSONDouble]("score").get.isDefined must beTrue

      doc.getAsTry[BSONNumberLike]("score") must beSuccessfulTry.like {
        case num =>
          num.toDouble must_=== 3.88 and (num.toFloat must_=== 3.88f) and (
            num.toLong must_=== 3) and (num.toInt must_=== 3)
      }

      doc.getAsTry[BSONBooleanLike]("score").
        map(_.toBoolean) must beSuccessfulTry(true)
    }

    "should not have a surname2" in {
      doc.getTry("surname2") must beFailedTry and (
        doc.getUnflattenedTry("surname2") must beSuccessfulTry(
          Option.empty[BSONValue]))
    }

    "should be read" in {
      BSONDocumentReader(_.getAsTry[String]("name").get).read(doc).
        aka("name") must_=== "James"
    }

    "be written" in {
      BSONDocumentWriter { s: String => BSONDocument(f"$$foo" -> s) }.
        write("bar") must_=== BSONDocument(f"$$foo" -> "bar")
    }
  }

  "Complex Array" should {
    lazy val array = BSONArray(
      BSONString("elem0"),
      None,
      1,
      2.222,
      BSONDocument(
        "name" -> "Joe"),
      BSONArray(0L),
      "pp[4]")

    "be of size = 6" in {
      array.size must_=== 6
    }

    "have a an int = 2 at index 2" in {
      array.get(1) must beSome(BSONInteger(1)) and (
        array.getAs[Int](1) must beSome(1))
    }

    "get bsondocument at index 3" in {
      array.getAs[BSONDocument](3) must beSome.which {
        _.getAs[String]("name") must beSome("Joe")
      }
    }

    "get bsonarray at index 4" in {
      val tdoc = array.getAsTry[BSONDocument](4)
      tdoc must beFailedTry
      tdoc.failed.get.isInstanceOf[exceptions.DocumentKeyNotFound] must beFalse
      array.getAsTry[BSONArray](4) must beSuccessfulTry.like {
        case tarray =>
          tarray.getAs[BSONLong](0) must beSome(BSONLong(0L)) and (
            tarray.getAs[BSONBooleanLike](0).
            map(_.toBoolean) must beSome(false))
      }
    }
  }

  "Map" should {
    "write primitives values" in {
      val input = Map("a" -> 1, "b" -> 2)
      val result = DefaultBSONHandlers.MapWriter(BSONStringHandler, BSONIntegerHandler).write(input)

      result must_=== BSONDocument("a" -> 1, "b" -> 2)
    }

    "read primitives values" in {
      val input = BSONDocument("a" -> 1, "b" -> 2)
      val handler = implicitly[BSONReader[BSONDocument, Map[String, Int]]]
      val result = handler.read(input)

      result must_=== Map("a" -> 1, "b" -> 2)
    }

    case class Foo(label: String, count: Int)
    implicit val fooWriter = BSONDocumentWriter[Foo] { foo => BSONDocument("label" -> foo.label, "count" -> foo.count) }
    implicit val fooReader = BSONDocumentReader[Foo] { document =>
      val foo = for {
        label <- document.getAs[String]("label")
        count <- document.getAs[Int]("count")
      } yield Foo(label, count)
      foo.get
    }

    "write complex values" in {
      val expectedResult = BSONDocument(
        "a" -> BSONDocument("label" -> "foo", "count" -> 10),
        "b" -> BSONDocument("label" -> "foo2", "count" -> 20))
      val input = Map("a" -> Foo("foo", 10), "b" -> Foo("foo2", 20))
      val result = DefaultBSONHandlers.MapWriter(BSONStringHandler, fooWriter).write(input)

      result must_=== expectedResult
    }

    "read complex values" in {
      val expectedResult = Map("a" -> Foo("foo", 10), "b" -> Foo("foo2", 20))
      val input = BSONDocument(
        "a" -> BSONDocument("label" -> "foo", "count" -> 10),
        "b" -> BSONDocument("label" -> "foo2", "count" -> 20))
      val handler = implicitly[BSONReader[BSONDocument, Map[String, Foo]]]
      val result = handler.read(input)

      result must_=== expectedResult
    }
  }

  "BSONDateTime" should {
    val time = System.currentTimeMillis()
    val bson = BSONDateTime(time)
    val date = new Date(time)
    val handler = implicitly[BSONHandler[BSONDateTime, Date]]

    "be read as date" in {
      handler.read(bson) must_=== date and (
        handler.widenReader.readTry(bson: BSONValue).
        aka("widen read") must beSuccessfulTry(date)) and (
          handler.widenReader.readTry {
            val str: BSONValue = BSONString("foo")
            str
          } must beFailedTry)
    }

    "be written from a date" in {
      handler.write(date) must_=== bson
    }
  }

  "BSONNumberLike" should {
    val reader = implicitly[BSONReader[BSONValue, BSONNumberLike]]

    "read BSONTimestamp" in {
      val time = System.currentTimeMillis()
      val num = time / 1000L
      val bson = BSONTimestamp(num)

      reader.readOpt(bson).map(_.toLong) must beSome(num * 1000L) and (
        reader.widenReader.readTry(bson: BSONValue).
        map(_.toLong) must beSuccessfulTry(num * 1000L)) and (
          reader.widenReader.readTry {
            val l: BSONValue = BSONArray(1L)
            l
          } must beFailedTry)
    }
  }

  "BSONString" should {
    val reader = BSONReader { bson: BSONString => bson.value }

    "be read #1" in {
      reader.afterRead(_ => 1).readTry(BSONString("lorem")).
        aka("mapped BSON") must beSuccessfulTry(1)
    }

    "be read #2" in {
      reader.beforeRead { i: BSONInteger =>
        BSONString(s"lorem:${i.value}")
      }.readTry(BSONInteger(2)) must beSuccessfulTry("lorem:2")
    }

    val writer = BSONWriter { str: String => BSONString(str) }

    "be written #1" in {
      writer.afterWrite(bs => BSONInteger(bs.value.length)).write("foo").
        aka("mapped BSON") must_=== BSONInteger(3)
    }

    "be written #2" in {
      writer.beforeWrite((_: (Int, Int)).toString).write(1 -> 2).
        aka("mapped BSON") must_=== BSONString("(1,2)")
    }
  }

  "Custom class" should {
    case class Foo(bar: String)
    implicit val w = BSONWriter[Foo, BSONString] { f => BSONString(f.bar) }
    implicit val r = BSONReader[BSONString, Foo] { s => Foo(s.value) }

    val foo = Foo("lorem")
    val bson = BSONString("lorem")

    "be read" in {
      w.write(foo) must_=== bson
    }

    "be written" in {
      r.read(bson) must_=== foo
    }

    "be handled (provided there are reader and writer)" in {
      val h = implicitly[BSONHandler[BSONString, Foo]]

      h.write(foo) must_=== bson and (h.read(bson) must_=== foo)
    }
  }

  "Neil Young" should {
    implicit def artHandler = ArtistHandler

    "produce the expected pretty representation" in {
      val birthInstant = java.time.Instant.parse("1982-10-11T17:30:00Z")

      val neilYoung = Artist(
        "Neil Young",
        new Date(birthInstant.toEpochMilli),
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

      val doc = BSON.write(neilYoung)

      BSONDocument.pretty(doc) must_=== """{
  "name": "Neil Young",
  "birthDate": ISODate("1982-10-11T17:30:00Z"),
  "albums": [
    {
      "name": "Everybody Knows this is Nowhere",
      "releaseYear": 1969,
      "tracks": [
        "Cinnamon Girl",
        "Everybody Knows this is Nowhere",
        "Round & Round (it Won't Be Long)",
        "Down By the River",
        "Losing End (When You're On)",
        "Running Dry (Requiem For the Rockets)",
        "Cowgirl in the Sand"
      ]
    }
  ]
}""".replaceAll("\r", "") and {
        implicit def albHandler: BSONDocumentReader[Album] = AlbumHandler

        val ny2 = BSON.readDocument[Artist](doc)
        val allSongs = doc.getAs[List[Album]]("albums").getOrElse(List.empty).flatMap(_.tracks)
        allSongs must_=== List(
          "Cinnamon Girl",
          "Everybody Knows this is Nowhere",
          "Round & Round (it Won't Be Long)",
          "Down By the River",
          "Losing End (When You're On)",
          "Running Dry (Requiem For the Rockets)",
          "Cowgirl in the Sand")

        ny2 must_=== neilYoung
      }
    }
  }

  section("unit")

  // ---

  case class Album(
    name: String,
    releaseYear: Int,
    tracks: List[String])

  case class Artist(
    name: String,
    birthDate: Date,
    albums: List[Album])

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

  object ArtistHandler extends BSONDocumentWriter[Artist] with BSONDocumentReader[Artist] {
    def write(artist: Artist) =
      BSONDocument(
        "name" -> artist.name,
        "birthDate" -> artist.birthDate,
        "albums" -> artist.albums)

    def read(doc: BSONDocument) = (for {
      name <- doc.getAs[String]("name")
      birthDate <- doc.getAs[Date]("birthDate")
      albums <- doc.getAs[List[Album]]("albums")
    } yield Artist(name, birthDate, albums)).get
  }
}
