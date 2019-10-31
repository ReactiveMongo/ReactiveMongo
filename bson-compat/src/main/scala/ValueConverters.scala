package reactivemongo.api.bson
package compat

import scala.language.implicitConversions

import scala.util.{ Failure, Success }

import reactivemongo.bson.{
  BSONArray => LegacyArray,
  BSONBinary => LegacyBinary,
  BSONBoolean => LegacyBoolean,
  BSONDateTime => LegacyDateTime,
  BSONDecimal => LegacyDecimal,
  BSONDocument => LegacyDocument,
  BSONDouble => LegacyDouble,
  BSONElement => LegacyElement,
  BSONInteger => LegacyInteger,
  BSONJavaScript => LegacyJavaScript,
  BSONJavaScriptWS => LegacyJavaScriptWS,
  BSONLong => LegacyLong,
  BSONMaxKey => LegacyMaxKey,
  BSONMinKey => LegacyMinKey,
  BSONNull => LegacyNull,
  BSONValue => LegacyValue,
  BSONObjectID => LegacyObjectID,
  BSONRegex => LegacyRegex,
  BSONSymbol => LegacySymbol,
  BSONString => LegacyString,
  BSONTimestamp => LegacyTimestamp,
  BSONUndefined => LegacyUndefined,
  Subtype => LegacySubtype
}

/**
 * See [[compat$]] and [[ValueConverters]]
 */
object ValueConverters extends ValueConverters

/**
 * Implicit conversions for value types between
 * `reactivemongo.bson` and `reactivemongo.api.bson`.
 *
 * {{{
 * // Required import
 * import reactivemongo.api.bson.compat.ValueConverters._
 *
 * // From legacy reactivemongo.bson
 * import reactivemongo.api.bson.{ BSONDouble, BSONString, BSONValue }
 *
 * val newStr: BSONString = reactivemongo.bson.BSONString("foo")
 * val newVal: BSONValue = reactivemongo.bson.BSONInteger(2)
 *
 * // To legacy
 * val oldStr: reactivemongo.bson.BSONString = BSONString("bar")
 * val oldVal: reactivemongo.bson.BSONValue = BSONDouble(1.2D)
 * }}}
 */
trait ValueConverters extends LowPriorityConverters {
  implicit final def toArray(legacy: LegacyArray): BSONArray =
    BSONArray(legacy.values.map(toValue))

  implicit final def fromArray(array: BSONArray): LegacyArray =
    LegacyArray(array.values.map(fromValue))

  implicit final def toDocument(legacy: LegacyDocument): BSONDocument =
    BSONDocument(legacy.stream.collect {
      case Success(LegacyElement(n, v)) => n -> toValue(v)
    })

  implicit final def toElement(legacy: LegacyElement): BSONElement =
    BSONElement(legacy.name, toValue(legacy.value))

  implicit final def fromDocument(doc: BSONDocument): LegacyDocument =
    LegacyDocument(doc.elements.map(fromElement))

  implicit final def fromElement(element: BSONElement): LegacyElement =
    LegacyElement(element.name, fromValue(element.value))

  implicit final def toBinary(legacy: LegacyBinary): BSONBinary =
    BSONBinary(legacy.byteArray, toBinarySubtype(legacy.subtype))

  implicit final def fromBinary(binary: BSONBinary): LegacyBinary =
    LegacyBinary(binary.byteArray, fromBinarySubtype(binary.subtype))

  implicit final def toBinarySubtype(legacy: LegacySubtype): Subtype = legacy match {
    case _: LegacySubtype.GenericBinarySubtype.type =>
      Subtype.GenericBinarySubtype

    case _: LegacySubtype.FunctionSubtype.type =>
      Subtype.FunctionSubtype

    case _: LegacySubtype.OldBinarySubtype.type =>
      Subtype.OldBinarySubtype

    case _: LegacySubtype.OldUuidSubtype.type =>
      Subtype.OldUuidSubtype

    case _: LegacySubtype.UuidSubtype.type =>
      Subtype.UuidSubtype

    case _: LegacySubtype.Md5Subtype.type =>
      Subtype.Md5Subtype

    case _ => Subtype.UserDefinedSubtype
  }

  implicit final def fromBinarySubtype(subtype: Subtype): LegacySubtype =
    subtype match {
      case Subtype.GenericBinarySubtype =>
        LegacySubtype.GenericBinarySubtype

      case Subtype.FunctionSubtype =>
        LegacySubtype.FunctionSubtype

      case Subtype.OldBinarySubtype =>
        LegacySubtype.OldBinarySubtype

      case Subtype.OldUuidSubtype =>
        LegacySubtype.OldUuidSubtype

      case Subtype.UuidSubtype =>
        LegacySubtype.UuidSubtype

      case Subtype.Md5Subtype =>
        LegacySubtype.Md5Subtype

      case _ => LegacySubtype.UserDefinedSubtype
    }

  implicit final def toDouble(legacy: LegacyDouble): BSONDouble =
    BSONDouble(legacy.value)

  implicit final def fromDouble(double: BSONDouble): LegacyDouble =
    LegacyDouble(double.value)

  implicit final def toStr(legacy: LegacyString): BSONString =
    BSONString(legacy.value)

  implicit final def fromStr(string: BSONString): LegacyString =
    LegacyString(string.value)

  implicit final def toBoolean(legacy: LegacyBoolean): BSONBoolean =
    BSONBoolean(legacy.value)

  implicit final def fromBoolean(boolean: BSONBoolean): LegacyBoolean =
    LegacyBoolean(boolean.value)

  implicit final def toInteger(legacy: LegacyInteger): BSONInteger =
    BSONInteger(legacy.value)

  implicit final def fromInteger(integer: BSONInteger): LegacyInteger =
    LegacyInteger(integer.value)

  implicit final def toLong(legacy: LegacyLong): BSONLong =
    BSONLong(legacy.value)

  implicit final def fromLong(long: BSONLong): LegacyLong =
    LegacyLong(long.value)

  implicit final def toJavaScript(legacy: LegacyJavaScript): BSONJavaScript =
    BSONJavaScript(legacy.value)

  implicit final def fromJavaScript(javaScript: BSONJavaScript): LegacyJavaScript = LegacyJavaScript(javaScript.value)

  implicit final def toJavaScriptWS(legacy: LegacyJavaScriptWS): BSONJavaScriptWS = BSONJavaScriptWS(legacy.value, BSONDocument.empty)

  implicit final def fromJavaScriptWS(javaScript: BSONJavaScriptWS): LegacyJavaScriptWS = LegacyJavaScriptWS(javaScript.value)

  implicit final def toRegex(legacy: LegacyRegex): BSONRegex =
    BSONRegex(legacy.value, legacy.flags)

  implicit final def fromRegex(regex: BSONRegex): LegacyRegex =
    LegacyRegex(regex.value, regex.flags)

  implicit final def toSymbol(legacy: LegacySymbol): BSONSymbol =
    BSONSymbol(legacy.value)

  implicit final def fromSymbol(symbol: BSONSymbol): LegacySymbol =
    LegacySymbol(symbol.value)

  implicit final def toObjectID(legacy: LegacyObjectID): BSONObjectID =
    BSONObjectID.parse(legacy.valueAsArray) match {
      case Success(oid) => oid
      case Failure(err) => throw err
    }

  implicit final def fromObjectID(objectID: BSONObjectID): LegacyObjectID =
    LegacyObjectID(objectID.byteArray)

  implicit final def toDateTime(legacy: LegacyDateTime): BSONDateTime =
    BSONDateTime(legacy.value)

  implicit final def fromDateTime(dateTime: BSONDateTime): LegacyDateTime =
    LegacyDateTime(dateTime.value)

  implicit final def toTimestamp(legacy: LegacyTimestamp): BSONTimestamp =
    BSONTimestamp(legacy.value)

  implicit final def fromTimestamp(timestamp: BSONTimestamp): LegacyTimestamp =
    LegacyTimestamp(timestamp.value)

  implicit final def toDecimal(legacy: LegacyDecimal): BSONDecimal =
    BSONDecimal(legacy.high, legacy.low)

  implicit final def fromDecimal(decimal: BSONDecimal): LegacyDecimal =
    LegacyDecimal(decimal.high, decimal.low)

  implicit val toUndefined: LegacyUndefined.type => BSONUndefined =
    _ => BSONUndefined

  implicit val fromUndefined: BSONUndefined => LegacyUndefined.type =
    _ => LegacyUndefined

  implicit val toNull: LegacyNull.type => BSONNull = _ => BSONNull

  implicit val fromNull: BSONNull => LegacyNull.type = _ => LegacyNull

  implicit val toMinKey: LegacyMinKey.type => BSONMinKey = _ => BSONMinKey

  implicit val fromMinKey: BSONMinKey => LegacyMinKey.type =
    _ => LegacyMinKey

  implicit val toMaxKey: LegacyMaxKey.type => BSONMaxKey = _ => BSONMaxKey

  implicit val fromMaxKey: BSONMaxKey => LegacyMaxKey.type =
    _ => LegacyMaxKey
}

private[bson] sealed trait LowPriorityConverters { _: ValueConverters =>
  implicit final def toValue(legacy: LegacyValue): BSONValue = legacy match {
    case arr: LegacyArray        => toArray(arr)
    case dtm: LegacyDateTime     => toDateTime(dtm)
    case doc: LegacyDocument     => toDocument(doc)
    case bin: LegacyBinary       => toBinary(bin)
    case dlb: LegacyDouble       => toDouble(dlb)
    case str: LegacyString       => toStr(str)
    case bol: LegacyBoolean      => toBoolean(bol)
    case int: LegacyInteger      => toInteger(int)
    case lng: LegacyLong         => toLong(lng)
    case js: LegacyJavaScript    => toJavaScript(js)
    case jsW: LegacyJavaScriptWS => toJavaScriptWS(jsW)
    case reg: LegacyRegex        => toRegex(reg)
    case sym: LegacySymbol       => toSymbol(sym)
    case tsp: LegacyTimestamp    => toTimestamp(tsp)
    case oid: LegacyObjectID     => toObjectID(oid)
    case dec: LegacyDecimal      => toDecimal(dec)

    case _: LegacyNull.type      => BSONNull
    case _: LegacyMaxKey.type    => BSONMaxKey
    case _: LegacyMinKey.type    => BSONMinKey

    case _                       => BSONUndefined
  }

  implicit final def fromValue(bson: BSONValue): LegacyValue = bson match {
    case arr: BSONArray        => fromArray(arr)
    case dtm: BSONDateTime     => fromDateTime(dtm)
    case doc: BSONDocument     => fromDocument(doc)
    case bin: BSONBinary       => fromBinary(bin)
    case dlb: BSONDouble       => fromDouble(dlb)
    case str: BSONString       => fromStr(str)
    case bol: BSONBoolean      => fromBoolean(bol)
    case int: BSONInteger      => fromInteger(int)
    case lng: BSONLong         => fromLong(lng)
    case js: BSONJavaScript    => fromJavaScript(js)
    case jsW: BSONJavaScriptWS => fromJavaScriptWS(jsW)
    case reg: BSONRegex        => fromRegex(reg)
    case sym: BSONSymbol       => fromSymbol(sym)
    case tsp: BSONTimestamp    => fromTimestamp(tsp)
    case oid: BSONObjectID     => fromObjectID(oid)
    case dec: BSONDecimal      => fromDecimal(dec)

    case BSONNull              => LegacyNull
    case BSONMaxKey            => LegacyMaxKey
    case BSONMinKey            => LegacyMinKey

    case _                     => LegacyUndefined
  }
}
