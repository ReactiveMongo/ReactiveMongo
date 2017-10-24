package reactivemongo.bson

import java.math.{ BigDecimal => JBigDec }

import scala.util.{ Success, Try }

import org.specs2.specification.core.Fragments

class BSONDecimalSpec extends org.specs2.mutable.Specification {
  "BSON decimal (128bits)" title

  "BSONDecimal" should {
    {
      def fixtures =
        Seq[( /*result: */ BSONDecimal, /*expected: */ BSONDecimal)](
          BSONDecimal(
            0x3040000000000000L, 0x0000000000000000L) -> BSONDecimal.PositiveZero,
          BSONDecimal(
            0xb040000000000000L, 0x0000000000000000L) -> BSONDecimal.NegativeZero,
          BSONDecimal(0x7800000000000000L, 0x0000000000000000L) -> BSONDecimal.
            PositiveInf,
          BSONDecimal(0xf800000000000000L, 0x0000000000000000L) -> BSONDecimal.
            NegativeInf,
          BSONDecimal(
            0x7c00000000000000L, 0x0000000000000000L) -> BSONDecimal.NaN)

      Fragments.foreach(fixtures) {
        case (result, expected) =>
          s"be $expected" in {
            result must_== expected
          }
      }
    }

    "be initialized from high/low values" in {
      BSONDecimal(0x3040000000000000L, 0x0000000000000001L).
        aka("decimal") must beLike[BSONDecimal] {
          case d @ BSONDecimal(0x3040000000000000L, 0x0000000000000001L) =>
            Decimal128.toBigDecimal(d) must beSuccessfulTry(JBigDec.ONE)
        }
    }

    "be initialized from Java BigDecimal" >> {
      Fragments.foreach(Seq(
        JBigDec.ONE -> BSONDecimal(3476778912330022912L, 1L),
        new JBigDec(Long.MinValue) -> BSONDecimal(
          -5746593124524752896L, -9223372036854775808L),
        new JBigDec(Long.MaxValue) -> BSONDecimal(
          3476778912330022912L, 9223372036854775807L))) {
        case (big, dec) => big.toString in {
          BSONDecimal.fromBigDecimal(big) must beSuccessfulTry(dec)
        }
      }
    }

    "be initialized from a single (high) long" >> {
      Fragments.foreach(
        Seq(
          1L -> BSONDecimal.fromBigDecimal(BigDecimal("1")),
          Long.MinValue -> BSONDecimal.fromBigDecimal(
            BigDecimal(Long.MinValue)),
          Long.MaxValue -> BSONDecimal.fromBigDecimal(
            BigDecimal(Long.MaxValue)))) {
          case (l, expected) => l.toString in {
            BSONDecimal.fromLong(l) must_== expected
          }
        }
    }

    "be parsed from string representation" >> {
      Fragments.foreach(Seq(
        "0" -> BSONDecimal.PositiveZero,
        "neg0" -> BSONDecimal.NegativeZero,
        "1" -> BSONDecimal(0x3040000000000000L, 0x0000000000000001L),
        "neg1" -> BSONDecimal(0xb040000000000000L, 0x0000000000000001L),
        "12345678901234567" -> BSONDecimal(
          0x3040000000000000L, 0x002bdc545d6b4b87L),
        "989898983458" -> BSONDecimal(0x3040000000000000L, 0x000000e67a93c822L),
        "-12345678901234567" -> BSONDecimal(
          0xb040000000000000L, 0x002bdc545d6b4b87L),
        "0.12345" -> BSONDecimal(0x3036000000000000L, 0x0000000000003039L),
        "0.0012345" -> BSONDecimal(0x3032000000000000L, 0x0000000000003039L),
        "00012345678901234567" -> BSONDecimal(
          0x3040000000000000L, 0x002bdc545d6b4b87L))) {
        case (repr, expected) => repr in {
          Decimal128.parse(repr.replace("neg", "-")).
            aka("parsed") must beSuccessfulTry(expected)
        }
      }
    }

    "be rounded exactly (ignore 0 at end)" >> {
      Fragments.foreach(Seq(
        "1.234567890123456789012345678901234" -> "1.234567890123456789012345678901234",
        "1.2345678901234567890123456789012340" -> "1.234567890123456789012345678901234",
        "1.23456789012345678901234567890123400" -> "1.234567890123456789012345678901234",
        "1.234567890123456789012345678901234000" -> "1.234567890123456789012345678901234")) {
        case (input, rounded) => input in {
          Decimal128.parse(input) must_== Decimal128.parse(rounded)
        }
      }
    }

    "clamp positive exponents" >> {
      Fragments.foreach(Seq(
        "1E6112" -> "10E6111",
        "1E6113" -> "100E6111",
        "1E6143" -> "100000000000000000000000000000000E+6111",
        "1E6144" -> "1000000000000000000000000000000000E+6111",
        "11E6143" -> "1100000000000000000000000000000000E+6111",
        "0E8000" -> "0E6111",
        "0E2147483647" -> "0E6111",
        "-1E6112" -> "-10E6111",
        "-1E6113" -> "-100E6111",
        "-1E6143" -> "-100000000000000000000000000000000E+6111",
        "-1E6144" -> "-1000000000000000000000000000000000E+6111",
        "-11E6143" -> "-1100000000000000000000000000000000E+6111",
        "-0E8000" -> "-0E6111",
        "-0E2147483647" -> "-0E6111")) {
        case (input, clamped) => input in {
          Decimal128.parse(input) must_== Decimal128.parse(clamped)
        }
      }
    }

    "clamp negative exponents" >> {
      Fragments.foreach(Seq(
        "0E-8000" -> "0E-6176",
        "0E-2147483647" -> "0E-6176",
        "10E-6177" -> "1E-6176",
        "100E-6178" -> "1E-6176",
        "110E-6177" -> "11E-6176",
        "-0E-8000" -> "-0E-6176",
        "-0E-2147483647" -> "-0E-6176",
        "-10E-6177" -> "-1E-6176",
        "-100E-6178" -> "-1E-6176",
        "-110E-6177" -> "-11E-6176")) {
        case (input, clamped) => input in {
          Decimal128.parse(input) must_== Decimal128.parse(clamped)
        }
      }
    }

    "construct from large BigDecimal" >> {
      Fragments.foreach(Seq(
        "12345689012345789012345" -> BSONDecimal(
          0x304000000000029dL, 0x42da3a76f9e0d979L),
        "1234567890123456789012345678901234" -> BSONDecimal(
          0x30403cde6fff9732L, 0xde825cd07e96aff2L),
        "9.999999999999999999999999999999999E+6144" -> BSONDecimal(
          0x5fffed09bead87c0L, 0x378d8e63ffffffffL),
        "9.999999999999999999999999999999999E-6143" -> BSONDecimal(
          0x0001ed09bead87c0L, 0x378d8e63ffffffffL),
        "5.192296858534827628530496329220095E+33" -> BSONDecimal(
          0x3040ffffffffffffL, 0xffffffffffffffffL))) {
        case (input, expected) => input in {
          Decimal128.parse(input) must beSuccessfulTry(expected)
        }
      }
    }

    "convert to BigDecimal" >> {
      Fragments.foreach(Seq(
        BSONDecimal(
          0x3040000000000000L, 0x0000000000000000L) -> BigDecimal("0"),
        BSONDecimal(
          0x3040000000000000L, 0x0000000000000001L) -> BigDecimal("1"),
        BSONDecimal(
          0xb040000000000000L, 0x0000000000000001L) -> BigDecimal("-1"),
        BSONDecimal(0x3040000000000000L, 0x002bdc545d6b4b87L) -> BigDecimal(
          "12345678901234567"),
        BSONDecimal(
          0x3040000000000000L, 0x000000e67a93c822L) -> BigDecimal(
            "989898983458"),
        BSONDecimal(
          0xb040000000000000L, 0x002bdc545d6b4b87L) -> BigDecimal(
            "-12345678901234567"),
        BSONDecimal(
          0x3036000000000000L, 0x0000000000003039L) -> BigDecimal("0.12345"),
        BSONDecimal(
          0x3032000000000000L, 0x0000000000003039L) -> BigDecimal("0.0012345"),
        BSONDecimal(0x3040000000000000L, 0x002bdc545d6b4b87L) -> BigDecimal(
          "00012345678901234567"))) {
        case (dec, result) => dec.toString in {
          BSONDecimal.toBigDecimal(dec) must beSuccessfulTry(result)
        }
      }
    }

    "convert to large BigDecimal" >> {
      Fragments.foreach(Seq(
        BSONDecimal(0x304000000000029dL, 0x42da3a76f9e0d979L) -> BigDecimal(
          "12345689012345789012345"),
        BSONDecimal(0x30403cde6fff9732L, 0xde825cd07e96aff2L) -> BigDecimal(
          "1234567890123456789012345678901234"),
        BSONDecimal(0x5fffed09bead87c0L, 0x378d8e63ffffffffL) -> BigDecimal(
          "9.999999999999999999999999999999999E+6144"),
        BSONDecimal(0x0001ed09bead87c0L, 0x378d8e63ffffffffL) -> BigDecimal(
          "9.999999999999999999999999999999999E-6143"),
        BSONDecimal(0x3040ffffffffffffL, 0xffffffffffffffffL) -> BigDecimal(
          "5.192296858534827628530496329220095E+33"))) {
        case (dec, result) => dec.toString in {
          BSONDecimal.toBigDecimal(dec) must beSuccessfulTry(result)
        }
      }
    }

    "convert invalid representations of 0 as BigDecimal 0" >> {
      Fragments.foreach(Seq(
        BSONDecimal(0x6C10000000000000L, 0x0L) -> BigDecimal("0"),
        BSONDecimal(0x6C11FFFFFFFFFFFFL, 0xffffffffffffffffL) -> BigDecimal(
          "0E+3"))) {
        case (dec, result) => dec.toString in {
          BSONDecimal.toBigDecimal(dec) must beSuccessfulTry(result)
        }
      }
    }

    "be infinite" >> {
      Fragments.foreach(Seq(
        BSONDecimal.PositiveInf,
        BSONDecimal.NegativeInf)) { dec =>
        dec.toString in {
          dec.isInfinite must beTrue
        }
      }
    }

    "be finite" >> {
      Fragments.foreach(Seq(
        "0",
        "9.999999999999999999999999999999999E+6144",
        "9.999999999999999999999999999999999E-6143")) { repr =>
        repr in {
          Decimal128.parse(repr) must beSuccessfulTry[BSONDecimal].like {
            case decimal => decimal.isInfinite must beFalse
          }
        }
      }
    }

    "not be a number (NaN)" >> {
      Fragments.foreach(Seq(
        BSONDecimal.NaN,
        BSONDecimal(0x7e00000000000000L, 0))) { dec =>
        dec.toString in {
          dec.isNaN must beTrue
        }
      }
    }

    "not be a number (NaN)" >> {
      Fragments.foreach(Seq[Try[BSONDecimal]](
        Success(BSONDecimal.PositiveInf),
        Success(BSONDecimal.NegativeInf),
        Decimal128.parse("0"),
        Decimal128.parse("9.999999999999999999999999999999999E+6144"),
        Decimal128.parse("9.999999999999999999999999999999999E-6143"))) { dec =>
        dec.toString in {
          dec.map(_.isNaN) must_== Success(false)
        }
      }
    }

    "convert NaN to string" in {
      BSONDecimal.NaN.toString must_== "NaN"
    }

    "convert NaN from string" >> {
      Fragments.foreach(Seq[(String, BSONDecimal)](
        "NaN" -> BSONDecimal.NaN,
        "nan" -> BSONDecimal.NaN,
        "nAn" -> BSONDecimal.NaN,
        "-NaN" -> BSONDecimal.NegativeNaN,
        "-nan" -> BSONDecimal.NegativeNaN,
        "-nAn" -> BSONDecimal.NegativeNaN)) {
        case (nan, expected) => nan in {
          Decimal128.parse(nan) must beSuccessfulTry(expected)
        }
      }
    }

    "not convert NaN to BigDecimal" in {
      BSONDecimal.toBigDecimal(BSONDecimal.NaN) must beFailedTry.
        withThrowable[ArithmeticException]
    }

    "convert infinity to string" >> {
      Fragments.foreach(Seq(
        BSONDecimal.PositiveInf -> "Infinity",
        BSONDecimal.NegativeInf -> "-Infinity")) {
        case (decimal, repr) =>
          val result = decimal.toString

          result in {
            result must_== repr
          }
      }
    }

    "convert infinity from string" >> {
      Fragments.foreach(Seq(
        "Inf" -> BSONDecimal.PositiveInf,
        "inf" -> BSONDecimal.PositiveInf,
        "inF" -> BSONDecimal.PositiveInf,
        "+Inf" -> BSONDecimal.PositiveInf,
        "+inf" -> BSONDecimal.PositiveInf,
        "+inF" -> BSONDecimal.PositiveInf,
        "Infinity" -> BSONDecimal.PositiveInf,
        "infinity" -> BSONDecimal.PositiveInf,
        "infiniTy" -> BSONDecimal.PositiveInf,
        "+Infinity" -> BSONDecimal.PositiveInf,
        "+infinity" -> BSONDecimal.PositiveInf,
        "+infiniTy" -> BSONDecimal.PositiveInf,
        "-Inf" -> BSONDecimal.NegativeInf,
        "-inf" -> BSONDecimal.NegativeInf,
        "-inF" -> BSONDecimal.NegativeInf,
        "-Infinity" -> BSONDecimal.NegativeInf,
        "-infinity" -> BSONDecimal.NegativeInf,
        "-infiniTy" -> BSONDecimal.NegativeInf)) {
        case (repr, decimal) => repr in {
          Decimal128.parse(repr) must beSuccessfulTry(decimal)
        }
      }
    }

    "convert finite to string" >> {
      Fragments.foreach(Seq(
        "0" -> "0",
        "-0" -> "-0",
        "0E10" -> "0E+10",
        "-0E10" -> "-0E+10",
        "1" -> "1",
        "-1" -> "-1",
        "-1.1" -> "-1.1",
        "123E-9" -> "1.23E-7",
        "123E-8" -> "0.00000123",
        "123E-7" -> "0.0000123",
        "123E-6" -> "0.000123",
        "123E-5" -> "0.00123",
        "123E-4" -> "0.0123",
        "123E-3" -> "0.123",
        "123E-2" -> "1.23",
        "123E-1" -> "12.3",
        "123E0" -> "123",
        "123E1" -> "1.23E+3",
        "1234E-7" -> "0.0001234",
        "1234E-6" -> "0.001234",
        "1E6" -> "1E+6")) {
        case (input, repr) => input in {
          Decimal128.parse(input).
            map(_.toString) must beSuccessfulTry(repr)
        }
      }
    }

    "convert invalid representations of 0 to string" >> {
      Fragments.foreach(Seq(
        BSONDecimal(0x6C10000000000000L, 0x0L) -> "0",
        BSONDecimal(0x6C11FFFFFFFFFFFFL, 0xffffffffffffffffL) -> "0E+3")) {
        case (decimal, repr) =>
          val result = decimal.toString

          result in {
            result must_== repr
          }
      }
    }

    "support equality" in {
      val d1 = BSONDecimal(0x3040000000000000L, 0x0000000000000001L)
      val d2 = BSONDecimal(0x3040000000000000L, 0x0000000000000001L)
      val d3 = BSONDecimal(0x3040000000000001L, 0x0000000000000001L)
      val d4 = BSONDecimal(0x3040000000000000L, 0x0000000000000011L)

      d1 must not(beNull) and {
        d1 must_== d1
      } and {
        d1 must_== d2
      } and {
        d1 must not(beEqualTo(d3))
      } and {
        d1 must not(beEqualTo(d4))
      } and {
        d1 must not(beEqualTo(0L))
      }
    }

    "provide hashCode" in {
      BSONDecimal(0x3040000000000000L, 0x0000000000000001L).
        hashCode must_== 809500703
    }

    "not convert infinity to BigDecimal" >> {
      Fragments.foreach(Seq(
        BSONDecimal.PositiveInf, BSONDecimal.NegativeInf)) { dec =>
        dec.toString in {
          Decimal128.toBigDecimal(dec) must beFailedTry.
            withThrowable[ArithmeticException]
        }
      }
    }

    "not convert negative zero to BigDecimal" >> {
      Fragments.foreach(Seq("-0", "-0E+1", "-0E-1")) { repr =>
        repr in {
          Decimal128.parse(repr).
            flatMap(BSONDecimal.toBigDecimal(_)) must beFailedTry.
            withThrowable[ArithmeticException]
        }
      }
    }

    "not round inexactly" >> {
      Fragments.foreach(Seq(
        "12345678901234567890123456789012345E+6111",
        "123456789012345678901234567890123456E+6111",
        "1234567890123456789012345678901234567E+6111",
        "12345678901234567890123456789012345E-6176",
        "123456789012345678901234567890123456E-6176",
        "1234567890123456789012345678901234567E-6176",
        "-12345678901234567890123456789012345E+6111",
        "-123456789012345678901234567890123456E+6111",
        "-1234567890123456789012345678901234567E+6111",
        "-12345678901234567890123456789012345E-6176",
        "-123456789012345678901234567890123456E-6176",
        "-1234567890123456789012345678901234567E-6176")) { repr =>
        repr in {
          Decimal128.parse(repr) must beFailedTry.
            withThrowable[IllegalArgumentException]
        }
      }
    }

    "not clamp large exponents if no extra precision is available" >> {
      Fragments.foreach(Seq(
        "1234567890123456789012345678901234E+6112",
        "1234567890123456789012345678901234E+6113",
        "1234567890123456789012345678901234E+6114",
        "-1234567890123456789012345678901234E+6112",
        "-1234567890123456789012345678901234E+6113",
        "-1234567890123456789012345678901234E+6114")) { repr =>
        repr in {
          Decimal128.parse(repr) must beFailedTry.
            withThrowable[IllegalArgumentException]
        }
      }
    }

    "not clamp small exponents if no extra precision can be discarded" >> {
      Fragments.foreach(Seq(
        "1234567890123456789012345678901234E-6177",
        "1234567890123456789012345678901234E-6178",
        "1234567890123456789012345678901234E-6179",
        "-1234567890123456789012345678901234E-6177",
        "-1234567890123456789012345678901234E-6178",
        "-1234567890123456789012345678901234E-6179")) { repr =>
        repr in {
          Decimal128.parse(repr) must beFailedTry.
            withThrowable[IllegalArgumentException]
        }
      }
    }

    "throw IllegalArgumentException if BigDecimal is too large" in {
      val big = new java.math.BigDecimal("12345678901234567890123456789012345")

      BSONDecimal.fromBigDecimal(big) must beFailedTry.
        withThrowable[IllegalArgumentException]
    }
  }

  "Conversions" should {
    "consider a BSONDecimal like a number" in {
      val big = BigDecimal("12.345")

      BSONDecimal.fromBigDecimal(big).map { dec =>
        implicitly[BSONNumberLike](dec).toDouble
      } must beSuccessfulTry(12.345D)
    }

    "consider a BSONDecimal like a boolean" in {
      BSONDecimal.fromLong(123L).map { dec =>
        implicitly[BSONBooleanLike](dec).toBoolean
      } must beSuccessfulTry(true)
    }
  }

  "BSON handlers" should {
    "read a BSONDecimal property" in {
      val dec = BSONDecimal.PositiveInf

      BSONDocument("prop" -> dec).
        getAsTry[BSONDecimal]("prop") must beSuccessfulTry(dec)
    }

    "support BigDecimal" >> {
      val big = BigDecimal("12.345")
      lazy val bsond = BSONDecimal.fromBigDecimal(big)

      "when reading a property" in {
        bsond must beSuccessfulTry[BSONDecimal].like {
          case dec => BSONDocument("prop" -> dec).
            getAsTry[BigDecimal]("prop") must beSuccessfulTry(big)
        }
      }

      "when writing a property" in {
        BSONDocument("prop" -> big).get("prop") must beSome[BSONValue].like {
          case dec @ BSONDecimal(_, _) => bsond must beSuccessfulTry(dec)
        }
      }
    }

    "read a BSONDecimal like a number" in {
      val big = BigDecimal("12.345")

      BSONDecimal.fromBigDecimal(big).flatMap {
        _.asTry[BSONNumberLike]
      } must beSuccessfulTry[BSONNumberLike].like {
        case number => number.toDouble must_== 12.345D
      }
    }

    "read a BSONDecimal like a boolean" in {
      BSONDecimal.fromLong(123L).flatMap {
        _.asTry[BSONBooleanLike]
      } must beSuccessfulTry[BSONBooleanLike].like {
        case number => number.toBoolean must beTrue
      }
    }
  }
}
