package samples

import reactivemongo.bson._

object BSONSamples {
  /*
   * Every BSON type has its matching class or object in the BSON library of ReactiveMongo.
   * 
   * For example:
   * - BSONString for strings
   * - BSONDouble for double values
   * - BSONInteger for integer values
   * - BSONLong for long values
   * - BSONObjectID for MongoDB ObjectIds
   * - BSONDocument for MongoDB documents
   * - BSONArray for MongoDB Arrays
   * - BSONBinary for binary values (raw binary arrays stored in the document)
   * - etc.
   * 
   * All this classes or objects extend the trait BSONValue.
   * 
   * You can build documents with the BSONDocument class.
   * 
   * It accepts tuples of String and BSONValue.
   * 
   * Let's build a very simple document representing an album.
   */
  val album = BSONDocument(
    "title" -> BSONString("Everybody Knows this is Nowhere"),
    "releaseYear" -> BSONInteger(1969))

  /*
   * You can read a BSONDocument using the `get` method,
   * which will return an Option of BSONValue whether the requested field is present or not.
   */
  val albumTitle = album.get("title")
  albumTitle match {
    case Some(BSONString(title)) => println(s"The title of this album is $title")
    case _                       => println("this document does not contain a title (or title is not a BSONString)")
  }

  /*
   * Writing complex BSONDocuments can be slightly verbose if you have to use BSONValues directly.
   * Luckily ReactiveMongo provides `BSONHandlers`, which are both BSONReader and BSONWriter.
   * 
   * A BSONWriter[T, B <: BSONValue] is an instance that transforms some `T` instance into a `BSONValue`.
   * A BSONReader[B <: BSONValue, T] is an instance that transforms a `BSONValue` into a `T` instance.
   * 
   * There are some predefined (implicit) handlers that are available when you import reactivemongo.bson._, including:
   * - String <-> BSONString
   * - Int <-> BSONInteger
   * - Long <-> BSONLong
   * - Double <-> BSONDouble
   * - Boolean <-> BSONBoolean
   * 
   * Each value that can be written using a BSONWriter can be used directly when calling a BSONDocument constructor.
   */
  val album2 = BSONDocument(
    "title" -> "Everybody Knows this is Nowhere",
    "releaseYear" -> 1969)

  /*
   * Easier, right? Note that this does _not_ use implicit conversions, but implicit type classes.
   * 
   * Getting values follow the same principle using getAs(String) method.
   * 
   * This method is parametrized with a type that can be transformed into a BSONValue using a BSONReader instance
   * that is implicitly available in the scope (again, the default readers are already imported if you imported
   * reactivemongo.bson._).
   * 
   * If the value could not be found, or if the reader could not deserialize it (often because the type did not match),
   * None will be returned.
   */

  val albumTitle2 = album2.getAs[String]("title") // Some("Everybody Knows this is Nowhere")
  val albumTitle3 = album2.getAs[BSONString]("title") // Some(BSONString("Everybody Knows this is Nowhere"))

  /*
   * Another cool feature of BSONDocument constructors is to give Options of BSONValues (or Options of instances that can
   * be written into BSONValues). The resulting BSONDocument will contain only defined options.
   */
  val album3 = BSONDocument(
    "title" -> "Everybody Knows this is Nowhere",
    "releaseYear" -> 1969,
    "hiddenTrack" -> None,
    "allMusicRating" -> Some(5.0))

  val album3PrettyBSONRepresentation = BSONDocument.pretty(album3)
  /* gives:
   * {
   *   title: BSONString(Everybody Knows this is Nowhere),
   *   releaseYear: BSONInteger(1969),
   *   allMusicRating: BSONDouble(5.0)
   * }
   */

  /*
   * You can write your own Writers and Readers for your models.
   * Let's define a model for Album, and its BSONWriter and BSONReader.
   */
  case class SimpleAlbum(
    title: String,
    releaseYear: Int,
    hiddenTrack: Option[String],
    allMusicRating: Option[Double])

  implicit object SimpleAlbumWriter extends BSONDocumentWriter[SimpleAlbum] {
    def write(album: SimpleAlbum): BSONDocument = BSONDocument(
      "title" -> album.title,
      "releaseYear" -> album.releaseYear,
      "hiddenTrack" -> album.hiddenTrack,
      "allMusicRating" -> album.allMusicRating)
  }

  implicit object SimpleAlbumReader extends BSONDocumentReader[SimpleAlbum] {
    def read(doc: BSONDocument): SimpleAlbum = {
      SimpleAlbum(
        doc.getAs[String]("title").get,
        doc.getAs[Int]("releaseYear").get,
        doc.getAs[String]("hiddenTrack"),
        doc.getAs[Double]("allMusicRating"))
    }
  }

  /*
   * You should have noted that our reader and writer extend BSONDocumentReader[T] and BSONDocumentWriter[T].
   * 
   * These two traits are just a shorthand for BSONReader[B <: BSONValue, T] and BSONWriter[T, B <: BSONValue].
   */

  /*
   * OK, now, what if I want to store all the tracks names of the album? Or, in other words,
   * how can we deal with collections?
   * 
   * First of all, you can safely infer that all seqs and sets can be serialized as BSONArrays.
   * Using BSONArray follows the same patterns as BSONDocument.
   */

  val album4 = BSONDocument(
    "title" -> "Everybody Knows this is Nowhere",
    "releaseYear" -> 1969,
    "hiddenTrack" -> None,
    "allMusicRating" -> Some(5.0),
    "tracks" -> BSONArray(
      "Cinnamon Girl",
      "Everybody Knows this is Nowhere",
      "Round & Round (it Won't Be Long)",
      "Down By the River",
      "Losing End (When You're On)",
      "Running Dry (Requiem For the Rockets)",
      "Cowgirl in the Sand"))

  val tracksOfAlbum4 = album4.getAs[BSONArray]("tracks").map { array =>
    array.values.map { track =>
      // here, I get a track as a BSONValue.
      // I can use `seeAsOpt[T]` to safely get an Option of its value as a `T`
      track.seeAsOpt[String].get
    }
  }

  /*
   * Using BSONArray does what we want, but this code is pretty verbose.
   * Would it not be nice to deal directly with collections?
   * 
   * Here again, there is a converter for Traversables of types that can be transformed into BSONValues.
   * For example, if you have a List[Something], if there is an implicit BSONWriter of Something to some BSONValue in the scope,
   * you can use it as is, without giving explicitly a BSONArray.
   * 
   * The same logic applies for reading BSONArray values.
   */
  val album5 = BSONDocument(
    "title" -> "Everybody Knows this is Nowhere",
    "releaseYear" -> 1969,
    "hiddenTrack" -> None,
    "allMusicRating" -> Some(5.0),
    "tracks" -> List(
      "Cinnamon Girl",
      "Everybody Knows this is Nowhere",
      "Round & Round (it Won't Be Long)",
      "Down By the River",
      "Losing End (When You're On)",
      "Running Dry (Requiem For the Rockets)",
      "Cowgirl in the Sand"))

  val tracksOfAlbum5 = album5.getAs[List[String]]("tracks")
  // returns an Option[List[String]] if `tracks` is a BSONArray containing BSONStrings :)

  /*
   * So, now we can rewrite our reader and writer for albums including tracks.
   */
  case class Album(
    title: String,
    releaseYear: Int,
    hiddenTrack: Option[String],
    allMusicRating: Option[Double],
    tracks: List[String])

  implicit object AlbumWriter extends BSONDocumentWriter[Album] {
    def write(album: Album): BSONDocument = BSONDocument(
      "title" -> album.title,
      "releaseYear" -> album.releaseYear,
      "hiddenTrack" -> album.hiddenTrack,
      "allMusicRating" -> album.allMusicRating,
      "tracks" -> album.tracks)
  }

  implicit object AlbumReader extends BSONDocumentReader[Album] {
    def read(doc: BSONDocument): Album = Album(
      doc.getAs[String]("title").get,
      doc.getAs[Int]("releaseYear").get,
      doc.getAs[String]("hiddenTrack"),
      doc.getAs[Double]("allMusicRating"),
      doc.getAs[List[String]]("tracks").toList.flatten)
  }

  /*
   * Obviously, you can combine these readers and writers to de/serialize more complex object graphs.
   * Let's write an Artist model, containing a list of Albums.
   */
  case class Artist(
    name: String,
    albums: List[Album])

  implicit object ArtistWriter extends BSONDocumentWriter[Artist] {
    def write(artist: Artist): BSONDocument = BSONDocument(
      "name" -> artist.name,
      "albums" -> artist.albums)
  }

  implicit object ArtistReader extends BSONDocumentReader[Artist] {
    def read(doc: BSONDocument): Artist = Artist(
      doc.getAs[String]("name").get,
      doc.getAs[List[Album]]("albums").toList.flatten)
  }

  val neilYoung = Artist(
    "Neil Young",
    List(
      Album(
        "Everybody Knows this is Nowhere",
        1969,
        None,
        Some(5),
        List(
          "Cinnamon Girl",
          "Everybody Knows this is Nowhere",
          "Round & Round (it Won't Be Long)",
          "Down By the River",
          "Losing End (When You're On)",
          "Running Dry (Requiem For the Rockets)",
          "Cowgirl in the Sand"))))

  val neilYoungDoc = BSON.write(neilYoung)
  /*
   * Here, we get an "ambiguous implicits" problem, which is normal because we have more than one Reader of BSONDocuments
   * available in our scope (SimpleArtistReader, ArtistReader, AlbumReader, etc.).
   * 
   * So we have to explicitly give the type of the instance we want to get from the document.
   */
  val neilYoungAgain = BSON.readDocument[Artist](neilYoungDoc)

  object BigDecimalExamples {
    // BigDecimal to BSONDouble Example
    // naive implementation, does not support values > Double.MAX_VALUE
    object BigDecimalBSONNaive {
      implicit object BigDecimalHandler extends BSONHandler[BSONDouble, BigDecimal] {
        def read(double: BSONDouble) = BigDecimal(double.value)
        def write(bd: BigDecimal) = BSONDouble(bd.toDouble)
      }

      case class SomeClass(bigd: BigDecimal)

      // USING HAND WRITTEN HANDLER
      implicit object SomeClassHandler extends BSONDocumentReader[SomeClass] with BSONDocumentWriter[SomeClass] {
        def read(doc: BSONDocument) = {
          SomeClass(doc.getAs[BigDecimal]("bigd").get)
        }
        def write(sc: SomeClass) = {
          BSONDocument("bigd" -> sc.bigd)
        }
      }
      // OR, USING MACROS
      // implicit val someClassHandler = Macros.handler[SomeClass]

      val sc1 = SomeClass(BigDecimal(1786381))
      val bsonSc1 = BSON.write(sc1)
      val sc1FromBSON = BSON.readDocument[SomeClass](bsonSc1)
    }

    // exact BigDecimal de/serialization
    object BSONBigDecimalBigInteger {

      implicit object BigIntHandler extends BSONDocumentReader[BigInt] with BSONDocumentWriter[BigInt] {
        def write(bigInt: BigInt): BSONDocument = BSONDocument(
          "signum" -> bigInt.signum,
          "value" -> BSONBinary(bigInt.toByteArray, Subtype.UserDefinedSubtype))
        def read(doc: BSONDocument): BigInt = BigInt(
          doc.getAs[Int]("signum").get,
          {
            val buf = doc.getAs[BSONBinary]("value").get.value
            buf.readArray(buf.readable)
          })
      }

      implicit object BigDecimalHandler extends BSONDocumentReader[BigDecimal] with BSONDocumentWriter[BigDecimal] {
        def write(bigDecimal: BigDecimal) = BSONDocument(
          "scale" -> bigDecimal.scale,
          "precision" -> bigDecimal.precision,
          "value" -> BigInt(bigDecimal.underlying.unscaledValue()))
        def read(doc: BSONDocument) = BigDecimal.apply(
          doc.getAs[BigInt]("value").get,
          doc.getAs[Int]("scale").get,
          new java.math.MathContext(doc.getAs[Int]("precision").get))
      }
      val bigInt = BigInt(888)
      bigInt.bigInteger.signum()
      val bigDecimal = BigDecimal(1908713, 12)

      case class SomeClass(bd: BigDecimal)

      implicit val someClassHandler = Macros.handler[SomeClass]

      val someClassValue = SomeClass(BigDecimal(1908713, 12))
      val bsonBigDecimal = BSON.writeDocument(someClassValue)
      val someClassValueFromBSON = BSON.readDocument[SomeClass](bsonBigDecimal)
      println(s"someClassValue == someClassValueFromBSON ? ${someClassValue equals someClassValueFromBSON}")
    }
  }
}