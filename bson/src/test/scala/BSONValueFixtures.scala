import reactivemongo.bson._

object BSONValueFixtures {
  val bsonDoubleFixtures = List(
    BSONDouble(0D), BSONDouble(-2D), BSONDouble(12.34D))

  val bsonStrFixtures = List(BSONString("foo"), BSONString("lorem"))

  val bsonIntFixtures = List(BSONInteger(-1), BSONInteger(2345))

  val bsonArrayFixtures = List(
    BSONArray(bsonDoubleFixtures), BSONArray(bsonStrFixtures),
    BSONArray(bsonIntFixtures),
    BSONArray(bsonIntFixtures ++ bsonStrFixtures))

  val bsonDocFixtures = List(
    BSONDocument.empty,
    BSONDocument("foo" -> "bar"),
    BSONDocument("lorem" -> 2, "ipsum" -> "value"),
    BSONDocument("ipsum" -> "value", "foo" -> 2D),
    BSONDocument("_id" -> "unique", "values" -> BSONArray(bsonStrFixtures)),
    BSONDocument(
      "position" -> 1000,
      "nested" -> BSONDocument("lorem" -> 2, "ipsum" -> "value")))

  val bsonBinFixtures = List(
    BSONBinary(Array[Byte](0, 1, 2), Subtype.GenericBinarySubtype),
    BSONBinary(Array[Byte](3, 4, 4), Subtype.FunctionSubtype))

  val bsonOidFixtures = List(
    BSONObjectID.generate(), BSONObjectID.generate(), BSONObjectID.generate())

  val bsonBoolFixtures = List(BSONBoolean(false), BSONBoolean(true))

  val bsonDateTimeFixtures = List(BSONDateTime(0L), BSONDateTime(123L))

  val bsonRegexFixtures = List(
    BSONRegex("/foo/bar/", "g"), BSONRegex("/LOREM/ipsum/", "i"))

  val bsonDBPFixtures: List[BSONDBPointer] = bsonOidFixtures.map { oid =>
    BSONDBPointer(
      value = System.identityHashCode(oid).toString,
      id = oid.valueAsArray)
  }

  val bsonJSFixtures = List(BSONJavaScript("foo()"), BSONJavaScript("bar()"))

  val bsonSymFixtures = List(BSONSymbol("foo"), BSONSymbol("bar"))

  val bsonJSWsFixtures = List(
    BSONJavaScriptWS("foo()"), BSONJavaScriptWS("bar()"))

  val bsonTsFixtures = List(BSONTimestamp(0L), BSONTimestamp(1L),
    BSONTimestamp(123L), BSONTimestamp(45678L))

  val bsonLongFixtures = List(BSONLong(-1L), BSONLong(0), BSONLong(12345L))

  val bsonConstFixtures = List(BSONUndefined, BSONNull, BSONMinKey, BSONMaxKey)

  lazy val bsonValueFixtures = bsonDoubleFixtures ++ bsonStrFixtures ++ (
    bsonIntFixtures ++ bsonArrayFixtures ++ bsonDocFixtures ++ bsonBinFixtures) ++ bsonOidFixtures ++ bsonBoolFixtures ++ bsonDateTimeFixtures ++ (
      bsonRegexFixtures ++ bsonDBPFixtures ++ bsonJSFixtures ++ bsonSymFixtures) ++ bsonJSWsFixtures ++ bsonTsFixtures ++ bsonLongFixtures

  lazy val elementProducerFixtures: List[ElementProducer] =
    bsonValueFixtures.map {
      case p: ElementProducer => p
      case v =>
        BSONElement(v.hashCode.toString, v)
    }
}
