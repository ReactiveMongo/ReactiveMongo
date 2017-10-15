package reactivemongo

import reactivemongo.bson._

object BSONValueFixtures {
  val bsonDoubleFixtures = List(
    BSONDouble(0D), BSONDouble(-2D), BSONDouble(12.34D))

  val bsonStrFixtures = List(BSONString("foo"), BSONString("lorem"))
  val bsonStrByteSizes = List(8, 10)

  val bsonIntFixtures = List(BSONInteger(-1), BSONInteger(2345))

  val bsonArrayFixtures = List(
    BSONArray(bsonDoubleFixtures), BSONArray(bsonStrFixtures),
    BSONArray(bsonIntFixtures),
    BSONArray(bsonIntFixtures ++ bsonStrFixtures))

  val bsonArrayByteSizes = List(38, 29, 19, 43)

  val bsonDocFixtures = List(
    BSONDocument.empty,
    BSONDocument("foo" -> "bar"),
    BSONDocument("lorem" -> 2, "ipsum" -> "value"),
    BSONDocument("ipsum" -> "value", "foo" -> 2D),
    BSONDocument("_id" -> "unique", "values" -> BSONArray(bsonStrFixtures)),
    BSONDocument(
      "position" -> 1000,
      "nested" -> BSONDocument("lorem" -> 2, "ipsum" -> "value")))

  val bsonDocByteSizes = List(5, 18, 33, 35, 58, 60)

  val bsonBinFixtures = List(
    BSONBinary(Array[Byte](0, 1, 2), Subtype.GenericBinarySubtype),
    BSONBinary(Array[Byte](3, 4, 4), Subtype.FunctionSubtype),
    BSONBinary(Array[Byte](4, 5, 6, 7, 8), Subtype.GenericBinarySubtype))

  val bsonBinByteSizes = List(8, 8, 10)

  val bsonOidFixtures = List(
    BSONObjectID.generate(), BSONObjectID.generate(), BSONObjectID.generate())

  val bsonBoolFixtures = List(BSONBoolean(false), BSONBoolean(true))

  val bsonDateTimeFixtures = List(BSONDateTime(0L), BSONDateTime(123L))

  val bsonRegexFixtures = List(
    BSONRegex("/foo/bar/", "g"), BSONRegex("/LOREM/ipsum/", "i"))

  val bsonRegexByteSizes = List(12, 16)

  val bsonDBPFixtures: List[BSONDBPointer] = bsonOidFixtures.map { oid =>
    BSONDBPointer(
      value = java.util.UUID.randomUUID().toString,
      id = oid.valueAsArray)
  }

  val bsonJSFixtures = List(
    BSONJavaScript("foo()"), BSONJavaScript("bar()"),
    BSONJavaScript("lorem(0)"))

  val bsonJSByteSizes = List(10, 10, 13)

  val bsonSymFixtures = List(
    BSONSymbol("foo"), BSONSymbol("bar"), BSONSymbol("lorem"))

  val bsonSymByteSizes = List(8, 8, 10)

  val bsonJSWsFixtures = List(
    BSONJavaScriptWS("foo()"), BSONJavaScriptWS("bar()"),
    BSONJavaScriptWS("lorem(0)"))

  val bsonTsFixtures = List(BSONTimestamp(0L), BSONTimestamp(1L),
    BSONTimestamp(123L), BSONTimestamp(45678L))

  val bsonLongFixtures = List(BSONLong(-1L), BSONLong(0), BSONLong(12345L))

  val bsonDecimalFixtures = List(
    BSONDecimal.PositiveZero,
    BSONDecimal.NegativeZero,
    BSONDecimal.PositiveInf,
    BSONDecimal.NegativeInf,
    BSONDecimal.NaN,
    BSONDecimal(0x3040000000000000L, 0x0000000000000001L),
    BSONDecimal(-5746593124524752896L, -9223372036854775808L),
    BSONDecimal(3476778912330022912L, 9223372036854775807L),
    BSONDecimal(0x3040000000000000L, 0x000000e67a93c822L),
    BSONDecimal(0x3036000000000000L, 0x0000000000003039L),
    BSONDecimal(0x3032000000000000L, 0x0000000000003039L))

  val bsonConstFixtures = List(BSONUndefined, BSONNull, BSONMinKey, BSONMaxKey)

  lazy val bsonValueFixtures = bsonDoubleFixtures ++ bsonStrFixtures ++ bsonIntFixtures ++ bsonArrayFixtures ++ bsonDocFixtures ++ bsonBinFixtures ++ bsonOidFixtures ++ bsonBoolFixtures ++ bsonDateTimeFixtures ++ bsonRegexFixtures ++ bsonDBPFixtures ++ bsonJSFixtures ++ bsonSymFixtures ++ bsonJSWsFixtures ++ bsonTsFixtures ++ bsonLongFixtures ++ bsonDecimalFixtures

  lazy val elementProducerFixtures: List[ElementProducer] =
    bsonValueFixtures.map {
      case p: ElementProducer => p
      case v =>
        BSONElement(v.hashCode.toString, v)
    }
}
