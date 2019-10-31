package reactivemongo

import reactivemongo.bson.{
  BSONArray => LegacyArray,
  BSONBinary => LegacyBin,
  BSONBoolean => LegacyBoolean,
  BSONDateTime => LegacyDateTime,
  BSONDecimal => LegacyDecimal,
  BSONDocument => LegacyDocument,
  BSONDouble => LegacyDouble,
  BSONInteger => LegacyInteger,
  BSONJavaScript => LegacyJavaScript,
  BSONJavaScriptWS => LegacyJavaScriptWS,
  BSONLong => LegacyLong,
  BSONMaxKey => LegacyMaxKey,
  BSONMinKey => LegacyMinKey,
  BSONNull => LegacyNull,
  BSONObjectID => LegacyObjectID,
  BSONRegex => LegacyRegex,
  BSONString => LegacyString,
  BSONSymbol => LegacySymbol,
  BSONTimestamp => LegacyTimestamp,
  BSONUndefined => LegacyUndefined,
  BSONValue => LegacyValue
}

import reactivemongo.api.bson.{
  BSONArray,
  BSONBinary,
  BSONBoolean,
  BSONDateTime,
  BSONDecimal,
  BSONDocument,
  BSONDouble,
  BSONInteger,
  BSONJavaScript,
  BSONJavaScriptWS,
  BSONLong,
  BSONMaxKey,
  BSONMinKey,
  BSONNull,
  BSONObjectID,
  BSONRegex,
  BSONString,
  BSONSymbol,
  BSONTimestamp,
  BSONUndefined,
  BSONValue
}

trait ConverterFixtures {
  val time = System.currentTimeMillis()

  val loid = LegacyObjectID.fromTime(time, true)
  val boid = BSONObjectID.fromTime(time, true)
  val uuid = java.util.UUID.randomUUID

  val ldt = LegacyDateTime(time)
  val bdt = BSONDateTime(time)

  val lts = LegacyTimestamp(time)
  val bts = BSONTimestamp(time)

  val lre = LegacyRegex("foo[A-Z]+", "i")
  val bre = BSONRegex("foo[A-Z]+", "i")

  val larr = LegacyArray(loid, LegacyString("foo"), ldt, LegacySymbol("bar"), lts, LegacyJavaScript("lorem()"), lre, LegacyArray(LegacyInteger(1), LegacyLong(2L)), LegacyDouble(3.4D))

  val barr = BSONArray(boid, BSONString("foo"), bdt, BSONSymbol("bar"), bts, BSONJavaScript("lorem()"), bre, BSONArray(BSONInteger(1), BSONLong(2L)), BSONDouble(3.4D))

  val ldoc = LegacyDocument("oid" -> loid, "str" -> LegacyString("foo"), "dt" -> ldt, "sym" -> LegacySymbol("bar"), "ts" -> lts, "nested" -> LegacyDocument("foo" -> "bar", "lorem" -> 1L), "js" -> LegacyJavaScript("lorem()"), "re" -> lre, "array" -> larr, "double" -> LegacyDouble(3.4D))

  val bdoc = BSONDocument("oid" -> boid, "str" -> BSONString("foo"), "dt" -> bdt, "sym" -> BSONSymbol("bar"), "ts" -> bts, "nested" -> BSONDocument("foo" -> "bar", "lorem" -> 1L), "js" -> BSONJavaScript("lorem()"), "re" -> bre, "array" -> barr, "double" -> BSONDouble(3.4D))

  val fixtures = Seq[(LegacyValue, BSONValue)](
    LegacyBin(uuid) -> BSONBinary(uuid),
    LegacyBoolean(true) -> BSONBoolean(true),
    LegacyDouble(1.23D) -> BSONDouble(1.23D),
    LegacyString("Foo") -> BSONString("Foo"),
    LegacyInteger(1) -> BSONInteger(1),
    LegacyLong(1L) -> BSONLong(1L),
    loid -> boid,
    ldt -> bdt,
    lts -> bts,
    LegacyDecimal.PositiveZero -> BSONDecimal.PositiveZero,
    lre -> bre,
    LegacyJavaScript("foo()") -> BSONJavaScript("foo()"),
    LegacyJavaScriptWS("bar()") -> BSONJavaScriptWS(
      "bar()", BSONDocument.empty),
    LegacySymbol("sym") -> BSONSymbol("sym"),
    LegacyUndefined -> BSONUndefined,
    LegacyNull -> BSONNull,
    LegacyMaxKey -> BSONMaxKey,
    LegacyMinKey -> BSONMinKey,
    larr -> barr,
    ldoc -> bdoc)

}
