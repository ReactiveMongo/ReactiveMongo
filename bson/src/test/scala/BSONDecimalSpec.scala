package reactivemongo.bson

import java.math.{ BigDecimal => JBigDec }

import scala.util.{ Success, Try }

import reactivemongo.bson.buffer.ArrayBSONBuffer

import org.specs2.specification.core.Fragments

class BSONDecimalSpec extends org.specs2.mutable.Specification {
  "BSON decimal (128bits)" title

  section("unit")

  import buffer.DefaultBufferHandler.{
    BSONDecimalBufferHandler,
    BSONDocumentBufferHandler
  }

  "BSONDecimal" should {
    lazy val fixtures1 =
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

    Fragments.foreach(fixtures1) {
      case (result, expected) =>
        s"be $expected" in {
          result must_=== expected and {
            bufferTrip(result) must_=== expected
          }
        }
    }

    // ---

    "be initialized from high/low values" in {
      BSONDecimal(0x3040000000000000L, 0x0000000000000001L).
        aka("decimal") must beLike[BSONDecimal] {
          case d @ BSONDecimal(0x3040000000000000L, 0x0000000000000001L) =>
            Decimal128.toBigDecimal(d) must beSuccessfulTry(JBigDec.ONE)
        }
    }

    // ---

    lazy val fixtures2 = Seq(
      JBigDec.ONE -> BSONDecimal(3476778912330022912L, 1L),
      new JBigDec(Long.MinValue) -> BSONDecimal(
        -5746593124524752896L, -9223372036854775808L),
      new JBigDec(Long.MaxValue) -> BSONDecimal(
        3476778912330022912L, 9223372036854775807L))

    "be initialized from Java BigDecimal" >> {
      Fragments.foreach(fixtures2) {
        case (big, dec) => big.toString in {
          BSONDecimal.fromBigDecimal(big) must beSuccessfulTry(dec)
        }
      }
    }

    // ---

    "be initialized from a single (high) long" >> {
      Fragments.foreach(Seq(
        1L -> BSONDecimal.fromBigDecimal(BigDecimal("1")),
        Long.MinValue -> BSONDecimal.fromBigDecimal(
          BigDecimal(Long.MinValue)),
        Long.MaxValue -> BSONDecimal.fromBigDecimal(
          BigDecimal(Long.MaxValue)))) {
        case (l, expected) => l.toString in {
          BSONDecimal.fromLong(l) must_=== expected
        }
      }
    }

    // ---

    lazy val fixtures3 = Seq(
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
        0x3040000000000000L, 0x002bdc545d6b4b87L))

    "be parsed from string representation" >> {
      Fragments.foreach(fixtures3) {
        case (repr, expected) => repr in {
          Decimal128.parse(repr.replace("neg", "-")).
            aka("parsed") must beSuccessfulTry(expected)
        }
      }
    }

    // ---

    "be rounded exactly (ignore 0 at end)" >> {
      Fragments.foreach(Seq(
        "1.234567890123456789012345678901234" -> "1.234567890123456789012345678901234",
        "1.2345678901234567890123456789012340" -> "1.234567890123456789012345678901234",
        "1.23456789012345678901234567890123400" -> "1.234567890123456789012345678901234",
        "1.234567890123456789012345678901234000" -> "1.234567890123456789012345678901234")) {
        case (input, rounded) => input in {
          Decimal128.parse(input) must_=== Decimal128.parse(rounded)
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
          Decimal128.parse(input) must_=== Decimal128.parse(clamped)
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
          Decimal128.parse(input) must_=== Decimal128.parse(clamped)
        }
      }
    }

    // ---

    lazy val fixtures4 = Seq(
      "12345689012345789012345" -> BSONDecimal(
        0x304000000000029dL, 0x42da3a76f9e0d979L),
      "1234567890123456789012345678901234" -> BSONDecimal(
        0x30403cde6fff9732L, 0xde825cd07e96aff2L),
      "9.999999999999999999999999999999999E+6144" -> BSONDecimal(
        0x5fffed09bead87c0L, 0x378d8e63ffffffffL),
      "9.999999999999999999999999999999999E-6143" -> BSONDecimal(
        0x0001ed09bead87c0L, 0x378d8e63ffffffffL),
      "5.192296858534827628530496329220095E+33" -> BSONDecimal(
        0x3040ffffffffffffL, 0xffffffffffffffffL))

    "construct from large BigDecimal" >> {
      Fragments.foreach(fixtures4) {
        case (input, expected) => input in {
          Decimal128.parse(input) must beSuccessfulTry(expected)
        }
      }
    }

    // ---

    lazy val fixtures5 = Seq(
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
        "00012345678901234567"))

    "convert to BigDecimal" >> {
      Fragments.foreach(fixtures5) {
        case (dec, result) => dec.toString in {
          BSONDecimal.toBigDecimal(dec) must beSuccessfulTry(result)
        }
      }
    }

    // ---

    lazy val fixtures6 = Seq(
      BSONDecimal(0x304000000000029dL, 0x42da3a76f9e0d979L) -> BigDecimal(
        "12345689012345789012345"),
      BSONDecimal(0x30403cde6fff9732L, 0xde825cd07e96aff2L) -> BigDecimal(
        "1234567890123456789012345678901234"),
      BSONDecimal(0x5fffed09bead87c0L, 0x378d8e63ffffffffL) -> BigDecimal(
        "9.999999999999999999999999999999999E+6144"),
      BSONDecimal(0x0001ed09bead87c0L, 0x378d8e63ffffffffL) -> BigDecimal(
        "9.999999999999999999999999999999999E-6143"),
      BSONDecimal(0x3040ffffffffffffL, 0xffffffffffffffffL) -> BigDecimal(
        "5.192296858534827628530496329220095E+33"))

    "convert to large BigDecimal" >> {
      Fragments.foreach(fixtures6) {
        case (dec, result) => dec.toString in {
          BSONDecimal.toBigDecimal(dec) must beSuccessfulTry(result)
        }
      }
    }

    // ---

    lazy val fixtures7 = Seq(
      BSONDecimal(0x6C10000000000000L, 0x0L) -> BigDecimal("0"),
      BSONDecimal(0x6C11FFFFFFFFFFFFL, 0xffffffffffffffffL) -> BigDecimal(
        "0E+3"))

    "convert invalid representations of 0 as BigDecimal 0" >> {
      Fragments.foreach(fixtures7) {
        case (dec, result) => dec.toString in {
          BSONDecimal.toBigDecimal(dec) must beSuccessfulTry(result)
        }
      }
    }

    // ---

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

    // ---

    "not be a number (NaN)" >> {
      Fragments.foreach(Seq[Try[BSONDecimal]](
        Success(BSONDecimal.PositiveInf),
        Success(BSONDecimal.NegativeInf),
        Decimal128.parse("0"),
        Decimal128.parse("9.999999999999999999999999999999999E+6144"),
        Decimal128.parse("9.999999999999999999999999999999999E-6143"))) { dec =>
        dec.toString in {
          dec.map(_.isNaN) must_=== Success(false)
        }
      }
    }

    "convert NaN to string" in {
      BSONDecimal.NaN.toString must_=== "NaN"
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
            result must_=== repr
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

    // ---

    lazy val fixtures8 = Seq(
      BSONDecimal(0x6C10000000000000L, 0x0L) -> "0",
      BSONDecimal(0x6C11FFFFFFFFFFFFL, 0xffffffffffffffffL) -> "0E+3")

    "convert invalid representations of 0 to string" >> {
      Fragments.foreach(fixtures8) {
        case (decimal, repr) =>
          val result = decimal.toString

          result in {
            result must_=== repr
          }
      }
    }

    // ---

    "support equality" in {
      val d1 = BSONDecimal(0x3040000000000000L, 0x0000000000000001L)
      val d2 = BSONDecimal(0x3040000000000000L, 0x0000000000000001L)
      val d3 = BSONDecimal(0x3040000000000001L, 0x0000000000000001L)
      val d4 = BSONDecimal(0x3040000000000000L, 0x0000000000000011L)

      d1 must not(beNull) and {
        d1 must_=== d1
      } and {
        d1 must_=== d2
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
        hashCode must_=== 809500703
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

    "be encoded properly" >> {
      val input: Seq[BSONDecimal] = fixtures1.map(_._1) ++
        fixtures2.map(_._2) ++ fixtures3.map(_._2) ++ fixtures4.map(_._2) ++
        fixtures5.map(_._1) ++ fixtures6.map(_._1) ++ fixtures7.map(_._1) ++
        fixtures8.map(_._1)

      Fragments.foreach(input.toSet.toSeq) { dec =>
        s"for $dec" in {
          bufferTrip(dec) must_=== dec
        }
      }
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
        case number => number.toDouble must_=== 12.345D
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

  "Buffer handlers" should {
    val fixtures = Seq(
      "1800000013640000000000000000000000000000002A3000" -> Decimal128.parse("0E-11"),
      "1800000013640000000000000000000000000000002C3000" -> Decimal128.parse("0E-10"),
      "1800000013640000000000000000000000000000002E3000" -> Decimal128.parse("0E-9"),
      "1800000013640000000000000000000000000000002EB000" -> Decimal128.parse("-0E-9"),
      "180000001364000000000000000000000000000000303000" -> Decimal128.parse("0E-8"),
      "18000000136400000000000000000000000000000030B000" -> Decimal128.parse("-0E-8"),
      "180000001364000000000000000000000000000000323000" -> Decimal128.parse("0E-7"),
      "18000000136400000000000000000000000000000032B000" -> Decimal128.parse("-0E-7"),
      "180000001364000000000000000000000000000000343000" -> BSONDecimal.parse("0.000000"),
      "18000000136400000000000000000000000000000034B000" -> BSONDecimal.parse("-0.000000"),
      "180000001364000000000000000000000000000000363000" -> BSONDecimal.parse("0.00000"),
      "18000000136400000000000000000000000000000036B000" -> BSONDecimal.parse("-0.00000"),
      "180000001364000000000000000000000000000000383000" -> BSONDecimal.parse("0.0000"),
      "18000000136400000000000000000000000000000038B000" -> BSONDecimal.parse("-0.0000"),
      "1800000013640000000000000000000000000000003A3000" -> BSONDecimal.parse("0.000"),
      "1800000013640000000000000000000000000000003AB000" -> BSONDecimal.parse("-0.000"),
      "1800000013640000000000000000000000000000003C3000" -> BSONDecimal.parse("0.00"),
      "1800000013640000000000000000000000000000003CB000" -> BSONDecimal.parse("-0.00"),
      "1800000013640000000000000000000000000000003E3000" -> BSONDecimal.parse("0.0"),
      "1800000013640000000000000000000000000000003EB000" -> BSONDecimal.parse("-0.0"),
      "180000001364000000000000000000000000000000403000" -> Success(BSONDecimal.PositiveZero),
      "180000001364000000000000000000000000000000423000" -> Decimal128.parse("0E+1"),
      "180000001364000000000000000000000000000000443000" -> Decimal128.parse("0E+2"),
      "180000001364000000000000000000000000000000463000" -> Decimal128.parse("0E+3"),
      "180000001364000000000000000000000000000000483000" -> Decimal128.parse("0E+4"),
      "1800000013640000000000000000000000000000004A3000" -> Decimal128.parse("0E+5"),
      "1800000013640000000000000000000000000000004C3000" -> Decimal128.parse("0E+6"),
      "1800000013640000000000000000000000000000004E3000" -> Decimal128.parse("0E+7"),
      "180000001364000000000000000000000000000000503000" -> Decimal128.parse("0E+8"),
      "180000001364000000000000000000000000000000523000" -> Decimal128.parse("0E+9"),
      "18000000136400000000000000000000000000000052B000" -> Decimal128.parse("-0E+9"),
      "180000001364000100000000000000000000000000403000" -> Decimal128.parse("1"),
      "18000000136400010000000000000000000000000040B000" -> Decimal128.parse("-1"),
      "180000001364000100000000000000000000000000523000" -> Decimal128.parse("1E+9"),
      "180000001364000100000000000000000000000000F43000" -> Decimal128.parse("1E+90"),
      "180000001364000400000000000000000000000000523000" -> Decimal128.parse("4E+9"),
      "180000001364000500000000000000000000000000323000" -> Decimal128.parse("5E-7"),
      "180000001364000500000000000000000000000000343000" -> Decimal128.parse("0.000005"),
      "180000001364000700000000000000000000000000263000" -> Decimal128.parse("7E-13"),
      "180000001364000700000000000000000000000000283000" -> Decimal128.parse("7E-12"),
      "1800000013640007000000000000000000000000002A3000" -> Decimal128.parse("7E-11"),
      "1800000013640007000000000000000000000000002C3000" -> Decimal128.parse("7E-10"),
      "1800000013640007000000000000000000000000002E3000" -> Decimal128.parse("7E-9"),
      "180000001364000700000000000000000000000000303000" -> Decimal128.parse("7E-8"),
      "180000001364000700000000000000000000000000323000" -> Decimal128.parse("7E-7"),
      "180000001364000700000000000000000000000000343000" -> Decimal128.parse("0.000007"),
      "180000001364000700000000000000000000000000363000" -> Decimal128.parse("0.00007"),
      "180000001364000700000000000000000000000000383000" -> Decimal128.parse("0.0007"),
      "1800000013640007000000000000000000000000003A3000" -> Decimal128.parse("0.007"),
      "1800000013640007000000000000000000000000003C3000" -> Decimal128.parse("0.07"),
      "1800000013640007000000000000000000000000003E3000" -> Decimal128.parse("0.7"),
      "180000001364000700000000000000000000000000403000" -> Decimal128.parse("7"),
      "180000001364000700000000000000000000000000423000" -> Decimal128.parse("7E+1"),
      "180000001364000700000000000000000000000000443000" -> Decimal128.parse("7E+2"),
      "180000001364000700000000000000000000000000463000" -> Decimal128.parse("7E+3"),
      "180000001364000700000000000000000000000000483000" -> Decimal128.parse("7E+4"),
      "1800000013640007000000000000000000000000004A3000" -> Decimal128.parse("7E+5"),
      "1800000013640007000000000000000000000000004C3000" -> Decimal128.parse("7E+6"),
      "1800000013640007000000000000000000000000004E3000" -> Decimal128.parse("7E+7"),
      "180000001364000700000000000000000000000000503000" -> Decimal128.parse("7E+8"),
      "180000001364000700000000000000000000000000523000" -> Decimal128.parse("7E+9"),
      "180000001364000700000000000000000000000000543000" -> Decimal128.parse("7E+10"),
      "180000001364000700000000000000000000000000563000" -> Decimal128.parse("7E+11"),
      "180000001364000700000000000000000000000000583000" -> Decimal128.parse("7E+12"),
      "180000001364000A00000000000000000000000000263000" -> Decimal128.parse("1.0E-12"),
      "180000001364000A00000000000000000000000000283000" -> Decimal128.parse("1.0E-11"),
      "180000001364000A000000000000000000000000002A3000" -> Decimal128.parse("1.0E-10"),
      "180000001364000A000000000000000000000000002C3000" -> Decimal128.parse("1.0E-9"),
      "180000001364000A000000000000000000000000002E3000" -> Decimal128.parse("1.0E-8"),
      "180000001364000A00000000000000000000000000303000" -> Decimal128.parse("1.0E-7"),
      "180000001364000A00000000000000000000000000323000" -> Decimal128.parse("0.0000010"),
      "180000001364000A00000000000000000000000000343000" -> Decimal128.parse("0.000010"),
      "180000001364000A00000000000000000000000000363000" -> Decimal128.parse("0.00010"),
      "180000001364000A00000000000000000000000000383000" -> Decimal128.parse("0.0010"),
      "180000001364000A000000000000000000000000003A3000" -> Decimal128.parse("0.010"),
      "180000001364000A000000000000000000000000003C3000" -> Decimal128.parse("0.10"),
      "180000001364000A000000000000000000000000003E3000" -> Decimal128.parse("1.0"),
      "180000001364000A000000000000000000000000003EB000" -> Decimal128.parse("-1.0"),
      "180000001364000A00000000000000000000000000403000" -> Decimal128.parse("10"),
      "180000001364000A00000000000000000000000000423000" -> Decimal128.parse("1.0E+2"),
      "180000001364000A00000000000000000000000000443000" -> Decimal128.parse("1.0E+3"),
      "180000001364000A00000000000000000000000000463000" -> Decimal128.parse("1.0E+4"),
      "180000001364000A00000000000000000000000000483000" -> Decimal128.parse("1.0E+5"),
      "180000001364000A000000000000000000000000004A3000" -> Decimal128.parse("1.0E+6"),
      "180000001364000A000000000000000000000000004C3000" -> Decimal128.parse("1.0E+7"),
      "180000001364000A000000000000000000000000004E3000" -> Decimal128.parse("1.0E+8"),
      "180000001364000A00000000000000000000000000503000" -> Decimal128.parse("1.0E+9"),
      "180000001364000A00000000000000000000000000523000" -> Decimal128.parse("1.0E+10"),
      "180000001364000A00000000000000000000000000543000" -> Decimal128.parse("1.0E+11"),
      "180000001364000A00000000000000000000000000563000" -> Decimal128.parse("1.0E+12"),
      "180000001364000A00000000000000000000000000583000" -> Decimal128.parse("1.0E+13"),
      "180000001364000A00000000000000000000000000F43000" -> Decimal128.parse("1.0E+91"),
      "180000001364000C000000000000000000000000003A3000" -> Decimal128.parse("0.012"),
      "180000001364000C00000000000000000000000000403000" -> Decimal128.parse("12"),
      "180000001364000F270000000000000000000000003AB000" -> Decimal128.parse("-9.999"),
      "180000001364001100000000000000000000000000403000" -> Decimal128.parse("17"),
      "1800000013640015CD5B0700000000000000000000203000" -> Decimal128.parse("1.23456789E-8"),
      "1800000013640015CD5B0700000000000000000000223000" -> Decimal128.parse("1.23456789E-7"),
      "1800000013640015CD5B0700000000000000000000243000" -> Decimal128.parse("0.00000123456789"),
      "1800000013640015CD5B0700000000000000000000263000" -> Decimal128.parse("0.0000123456789"),
      "18000000136400185C0ACE00000000000000000000383000" -> Decimal128.parse("345678.5432"),
      "18000000136400185C0ACE0000000000000000000038B000" -> Decimal128.parse("-345678.5432"),
      "180000001364002C00000000000000000000000000403000" -> Decimal128.parse("44"),
      "180000001364002C00000000000000000000000000523000" -> Decimal128.parse("4.4E+10"),
      "180000001364003200000000000000000000000000323000" -> Decimal128.parse("0.0000050"),
      "1800000013640040AF0D8648700000000000000000343000" -> Decimal128.parse("123456789.000000"),
      "1800000013640049000000000000000000000000002E3000" -> Decimal128.parse("7.3E-8"),
      "180000001364004C0000000000000000000000000040B000" -> Decimal128.parse("-76"),
      "180000001364005B000000000000000000000000003EB000" -> Decimal128.parse("-9.1"),
      "1800000013640064000000000000000000000000003C3000" -> Decimal128.parse("1.00"),
      "1800000013640064000000000000000000000000003E3000" -> Decimal128.parse("10.0"),
      "180000001364006400000000000000000000000000523000" -> Decimal128.parse("1.00E+11"),
      "180000001364006400000000000000000000000000F43000" -> Decimal128.parse("1.00E+92"),
      "1800000013640065000000000000000000000000003E3000" -> Decimal128.parse("10.1"),
      "1800000013640068000000000000000000000000003E3000" -> Decimal128.parse("10.4"),
      "1800000013640069000000000000000000000000003E3000" -> Decimal128.parse("10.5"),
      "180000001364006A000000000000000000000000003E3000" -> Decimal128.parse("10.6"),
      "180000001364006A19562522020000000000000000343000" -> Decimal128.parse("2345678.543210"),
      "180000001364006AB9C8733A0B0000000000000000343000" -> Decimal128.parse("12345678.543210"),
      "180000001364006AF90B7C50000000000000000000343000" -> Decimal128.parse("345678.543210"),
      "180000001364006D000000000000000000000000003E3000" -> Decimal128.parse("10.9"),
      "180000001364006E000000000000000000000000003E3000" -> Decimal128.parse("11.0"),
      "1800000013640078DF0D8648700000000000000000223000" -> Decimal128.parse("0.123456789012344"),
      "1800000013640079DF0D8648700000000000000000223000" -> Decimal128.parse("0.123456789012345"),
      "180000001364007B000000000000000000000000003A3000" -> Decimal128.parse("0.123"),
      "1800000013640080910F8648700000000000000000343000" -> Decimal128.parse("123456789.123456"),
      "1800000013640080910F8648700000000000000000403000" -> Decimal128.parse("123456789123456"),
      "180000001364008F030000000000000000000000003CB000" -> Decimal128.parse("-9.11"),
      "1800000013640099761CC7B548F377DC80A131C836FEAF00" -> Decimal128.parse("-1.111111111111111111111111111112345"),
      "180000001364009F230000000000000000000000003AB000" -> Decimal128.parse("-9.119"),
      "18000000136400D2040000000000000000000000003A3000" -> Decimal128.parse("1.234"),
      "18000000136400E803000000000000000000000000403000" -> Decimal128.parse("1000"),
      "18000000136400F104000000000000000000000000103000" -> Decimal128.parse("1.265E-21"),
      "18000000136400F104000000000000000000000000123000" -> Decimal128.parse("1.265E-20"),
      "18000000136400F104000000000000000000000000143000" -> Decimal128.parse("1.265E-19"),
      "18000000136400F104000000000000000000000000163000" -> Decimal128.parse("1.265E-18"),
      "18000000136400F104000000000000000000000000183000" -> Decimal128.parse("1.265E-17"),
      "18000000136400F104000000000000000000000000283000" -> Decimal128.parse("1.265E-9"),
      "18000000136400F1040000000000000000000000002A3000" -> Decimal128.parse("1.265E-8"),
      "18000000136400F1040000000000000000000000002C3000" -> Decimal128.parse("1.265E-7"),
      "18000000136400F1040000000000000000000000002E3000" -> Decimal128.parse("0.000001265"),
      "18000000136400F104000000000000000000000000303000" -> Decimal128.parse("0.00001265"),
      "18000000136400F104000000000000000000000000323000" -> Decimal128.parse("0.0001265"),
      "18000000136400F104000000000000000000000000343000" -> Decimal128.parse("0.001265"),
      "18000000136400F104000000000000000000000000363000" -> Decimal128.parse("0.01265"),
      "18000000136400F104000000000000000000000000383000" -> Decimal128.parse("0.1265"),
      "18000000136400F1040000000000000000000000003A3000" -> Decimal128.parse("1.265"),
      "18000000136400F1040000000000000000000000003C3000" -> Decimal128.parse("12.65"),
      "18000000136400F1040000000000000000000000003E3000" -> Decimal128.parse("126.5"),
      "18000000136400F104000000000000000000000000403000" -> Decimal128.parse("1265"),
      "18000000136400F104000000000000000000000000423000" -> Decimal128.parse("1.265E+4"),
      "18000000136400F104000000000000000000000000443000" -> Decimal128.parse("1.265E+5"),
      "18000000136400F104000000000000000000000000463000" -> Decimal128.parse("1.265E+6"),
      "18000000136400F104000000000000000000000000483000" -> Decimal128.parse("1.265E+7"),
      "18000000136400F1040000000000000000000000004A3000" -> Decimal128.parse("1.265E+8"),
      "18000000136400F1040000000000000000000000004C3000" -> Decimal128.parse("1.265E+9"),
      "18000000136400F1040000000000000000000000004E3000" -> Decimal128.parse("1.265E+10"),
      "18000000136400F104000000000000000000000000503000" -> Decimal128.parse("1.265E+11"),
      "18000000136400F104000000000000000000000000603000" -> Decimal128.parse("1.265E+19"),
      "18000000136400F104000000000000000000000000623000" -> Decimal128.parse("1.265E+20"),
      "18000000136400F104000000000000000000000000643000" -> Decimal128.parse("1.265E+21"),
      "18000000136400F104000000000000000000000000663000" -> Decimal128.parse("1.265E+22"),
      "18000000136400F104000000000000000000000000683000" -> Decimal128.parse("1.265E+23"),
      "18000000136400F198670C08000000000000000000363000" -> Decimal128.parse("345678.54321"),
      "18000000136400FC040000000000000000000000003C3000" -> Decimal128.parse("12.76"))

    "properly write bytes" >> {
      Fragments.foreach(fixtures) {
        case (repr, Success(dec)) => s"for '$repr'" in {
          val buf = new ArrayBSONBuffer

          BSONDocumentBufferHandler.write(BSONDocument("d" -> dec), buf)

          buf.array.map("%02X" format _).mkString must_=== repr
        }

        case (repr, _) => s"for '$repr'" in {
          failure
        }
      }
    }
  }

  section("unit")

  // ---

  @inline private def bufferTrip(in: BSONDecimal): BSONDecimal = {
    val buf = new ArrayBSONBuffer

    BSONDecimalBufferHandler.write(in, buf)

    BSONDecimalBufferHandler.read(buf.toReadableBuffer)
  }
}
