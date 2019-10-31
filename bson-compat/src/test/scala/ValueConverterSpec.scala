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
  BSONValue => LegacyValue,
  Subtype => LegacySubtype
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
  BSONValue,
  Subtype
}

import org.specs2.specification.core.Fragment

final class ValueConverterSpec
  extends org.specs2.mutable.Specification with ConverterFixtures {

  "Value converters" title

  import reactivemongo.api.bson.compat._

  "Scalar value converters" should {
    "support binary subtype" >> {
      Fragment.foreach(Seq[(LegacySubtype, Subtype)](
        LegacySubtype.GenericBinarySubtype -> Subtype.GenericBinarySubtype,
        LegacySubtype.FunctionSubtype -> Subtype.FunctionSubtype,
        LegacySubtype.OldBinarySubtype -> Subtype.OldBinarySubtype,
        LegacySubtype.OldUuidSubtype -> Subtype.OldUuidSubtype,
        LegacySubtype.UuidSubtype -> Subtype.UuidSubtype,
        LegacySubtype.Md5Subtype -> Subtype.Md5Subtype,
        LegacySubtype.UserDefinedSubtype -> Subtype.UserDefinedSubtype)) {
        case (l, n) =>
          s"from legacy $l" in {
            implicitly[Subtype](l) must_=== n
          }

          s"to legacy $n" in {
            implicitly[LegacySubtype](n) must_=== l
          }
      }
    }

    "support binary" >> {
      val bytes = "Test".getBytes("UTF-8")

      Fragment.foreach(Seq[(LegacyBin, BSONBinary)](
        LegacyBin(uuid) -> BSONBinary(uuid),
        LegacyBin(bytes, LegacySubtype.GenericBinarySubtype) -> BSONBinary(
          bytes, Subtype.GenericBinarySubtype))) {
        case (l, n) =>
          s"from legacy $l" in {
            implicitly[BSONBinary](l) must_=== n
          }

          s"to legacy $n" in {
            implicitly[LegacyBin](n) must_=== l
          }
      }
    }

    "support boolean" >> {
      "from legacy" in {
        implicitly[BSONBoolean](LegacyBoolean(true)) must_=== BSONBoolean(true)
      }

      "to legacy" in {
        implicitly[LegacyBoolean](BSONBoolean(true)) must_=== LegacyBoolean(true)
      }
    }

    "support date/time" >> {
      "from legacy" in {
        implicitly[BSONDateTime](ldt) must_=== bdt
      }

      "to legacy" in {
        implicitly[LegacyDateTime](bdt) must_=== ldt
      }
    }

    "support decimal" >> {
      "from legacy" in {
        implicitly[BSONDecimal](
          LegacyDecimal.PositiveInf) must_=== BSONDecimal.PositiveInf
      }

      "to legacy" in {
        implicitly[LegacyDecimal](
          BSONDecimal.PositiveInf) must_=== LegacyDecimal.PositiveInf
      }
    }

    "support double" >> {
      val raw = 1.23D

      "from legacy" in {
        implicitly[BSONDouble](LegacyDouble(raw)) must_=== BSONDouble(raw)
      }

      "to legacy" in {
        implicitly[LegacyDouble](BSONDouble(raw)) must_=== LegacyDouble(raw)
      }
    }

    "support integer" >> {
      "from legacy" in {
        implicitly[BSONInteger](LegacyInteger(1)) must_=== BSONInteger(1)
      }

      "to legacy" in {
        implicitly[LegacyInteger](BSONInteger(2)) must_=== LegacyInteger(2)
      }
    }

    "support JavaScript" >> {
      val raw = "foo()"

      "from legacy" in {
        implicitly[BSONJavaScript](
          LegacyJavaScript(raw)) must_=== BSONJavaScript(raw)
      }

      "to legacy" in {
        implicitly[LegacyJavaScript](
          BSONJavaScript(raw)) must_=== LegacyJavaScript(raw)
      }
    }

    "support JavaScript/WS" >> {
      val raw = "bar('lorem')"

      "from legacy" in {
        implicitly[BSONJavaScriptWS](
          LegacyJavaScriptWS(raw)) must_=== BSONJavaScriptWS(
            raw, BSONDocument.empty)
      }

      "to legacy" in {
        implicitly[LegacyJavaScriptWS](BSONJavaScriptWS(
          raw, BSONDocument.empty)) must_=== LegacyJavaScriptWS(raw)
      }
    }

    "support long" >> {
      "from legacy" in {
        implicitly[BSONLong](LegacyLong(1L)) must_=== BSONLong(1L)
      }

      "to legacy" in {
        implicitly[LegacyLong](BSONLong(2L)) must_=== LegacyLong(2L)
      }
    }

    "support null" >> {
      "from legacy" in {
        implicitly[BSONNull](LegacyNull) must_=== BSONNull
      }

      "to legacy" in {
        implicitly[LegacyNull.type](BSONNull) must_=== LegacyNull
      }
    }

    "support maxKey" >> {
      "from legacy" in {
        implicitly[BSONMaxKey](LegacyMaxKey) must_=== BSONMaxKey
      }

      "to legacy" in {
        implicitly[LegacyMaxKey.type](BSONMaxKey) must_=== LegacyMaxKey
      }
    }

    "support minKey" >> {
      "from legacy" in {
        implicitly[BSONMinKey](LegacyMinKey) must_=== BSONMinKey
      }

      "to legacy" in {
        implicitly[LegacyMinKey.type](BSONMinKey) must_=== LegacyMinKey
      }
    }

    "support object ID" >> {
      "from legacy" in {
        implicitly[BSONObjectID](loid) must_=== boid
      }

      "to legacy" in {
        implicitly[LegacyObjectID](boid) must_=== loid
      }
    }

    "support string" >> {
      val raw = "Foo"

      "from legacy" in {
        implicitly[BSONString](LegacyString(raw)) must_=== BSONString(raw)
      }

      "to legacy" in {
        implicitly[LegacyString](BSONString(raw)) must_=== LegacyString(raw)
      }
    }

    "support symbol" >> {
      val raw = "Foo"

      "from legacy" in {
        implicitly[BSONSymbol](LegacySymbol(raw)) must_=== BSONSymbol(raw)
      }

      "to legacy" in {
        implicitly[LegacySymbol](BSONSymbol(raw)) must_=== LegacySymbol(raw)
      }
    }

    "support timestamp" >> {
      "from legacy" in {
        implicitly[BSONTimestamp](lts) must_=== bts
      }

      "to legacy" in {
        implicitly[LegacyTimestamp](bts) must_=== lts
      }
    }

    "support regexp" >> {
      "from legacy" in {
        implicitly[BSONRegex](lre) must_=== bre
      }

      "to legacy" in {
        implicitly[LegacyRegex](bre) must_=== lre
      }
    }

    "support undefined" >> {
      "from legacy" in {
        implicitly[BSONUndefined](LegacyUndefined) must_=== BSONUndefined
      }

      "to legacy" in {
        implicitly[LegacyUndefined.type](BSONUndefined) must_=== LegacyUndefined
      }
    }
  }

  "Non-scalar value converters" should {
    "support array" >> {
      "from legacy" in {
        implicitly[BSONArray](larr) must_=== barr
      }

      "to legacy" in {
        implicitly[LegacyArray](barr) must_=== larr
      }
    }

    "support document" >> {
      "from legacy" in {
        implicitly[BSONDocument](ldoc) must_=== bdoc
      }

      "to legacy" in {
        implicitly[LegacyDocument](bdoc) must_=== ldoc
      }
    }
  }

  "Opaque values" should {
    Fragment.foreach(fixtures) {
      case (legacy, bson) =>
        s"from legacy $legacy" in {
          implicitly[BSONValue](legacy) must_=== bson
        }

        s"$bson to legacy" in {
          implicitly[LegacyValue](bson) must_=== legacy
        }
    }
  }
}
