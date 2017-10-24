package reactivemongo.bson

import java.math.{ BigDecimal, BigInteger, MathContext }

import scala.collection.immutable.Set

import scala.util.{ Failure, Success, Try }

private[bson] object Decimal128 {
  // Masks
  val InfMask: Long = 0x7800000000000000L
  val NaNMask: Long = 0x7c00000000000000L
  val SignBitMask: Long = 1L << 63

  // Factories

  def parse(repr: String): Try[BSONDecimal] = {
    val normalized = repr.toLowerCase

    if (normalized == "nan") {
      Success(BSONDecimal.NaN)
    } else if (normalized == "-nan") {
      Success(BSONDecimal.NegativeNaN)
    } else if (PositiveInfStrings contains normalized) {
      Success(BSONDecimal.PositiveInf)
    } else if (NegativeInfStrings contains normalized) {
      Success(BSONDecimal.NegativeInf)
    } else {
      fromBigDecimal(new BigDecimal(repr), repr.headOption.exists(_ == '-'))
    }
  }

  /**
   * @param negative true if value is negative (necessary to detect -0, which cannot be represented)
   */
  def fromBigDecimal(value: BigDecimal, negative: Boolean): Try[BSONDecimal] =
    clampRounded(value).flatMap { clamped =>
      val exponent = -clamped.scale

      for {
        unscaled <- {
          if (exponent < MinExponent || exponent > MaxExponent) {
            Failure(new IllegalArgumentException(
              s"Exponent is out of range: $exponent"))
          } else Success(clamped.unscaledValue)
        }
        _ <- {
          if (unscaled.bitLength > MaxBitLength) {
            Failure[Unit](new IllegalArgumentException(
              s"Unscaled clamped is out of range: $unscaled"))
          } else Success({})
        }
      } yield {
        val significand: BigInteger = unscaled.abs
        val bitLength = significand.bitLength
        val lowLimit = Math.min(64, bitLength)

        @annotation.tailrec
        def computeLow(i: Int, localLow: Long): Long = {
          if (i == lowLimit) localLow
          else {
            val upd = {
              if (significand testBit i) localLow | (1L << i)
              else localLow
            }

            computeLow(i + 1, upd)
          }
        }

        // TODO: common with computeLow
        @annotation.tailrec
        def computeHigh(i: Int, localHigh: Long): Long = {
          if (i >= bitLength) localHigh
          else {
            val upd = {
              if (significand testBit i) {
                localHigh | (1L << (i - 64))
              } else localHigh
            }

            computeHigh(i + 1, upd)
          }
        }

        val localLow = computeLow(0, 0L)
        val biasedExponent = (exponent + ExponentOffset).toLong
        val localHigh = computeHigh(64, 0L) | (biasedExponent << 49)

        if (clamped.signum == -1 || negative) {
          BSONDecimal(localHigh | SignBitMask, localLow)
        } else {
          BSONDecimal(localHigh, localLow)
        }
      }
    }

  // --- Conversions

  /**
   * Returns a BigDecimal that is equivalent to this one.
   */
  @throws[ArithmeticException](
    "if the value is NaN, Infinity, -Infinity, or -0")
  def toBigDecimal(decimal: BSONDecimal): Try[BigDecimal] = {
    if (decimal.isNaN) {
      Failure[BigDecimal](new ArithmeticException(
        "NaN can not be converted to a BigDecimal"))

    } else if (decimal.isInfinite) {
      Failure[BigDecimal](new ArithmeticException(
        "Infinity can not be converted to a BigDecimal"))

    } else {
      val bigDecimal: BigDecimal = noNegativeZero(decimal)

      // If the BigDecimal is 0, but the Decimal128 is negative,
      // that means we have -0.
      if (decimal.isNegative && bigDecimal.signum == 0) {
        Failure[BigDecimal](new ArithmeticException(
          "Negative zero can not be converted to a BigDecimal"))
      } else {
        Success(bigDecimal)
      }
    }
  }

  @inline private def highest(decimal: BSONDecimal): Boolean =
    (decimal.high & 3L << 61) == 3L << 61

  def noNegativeZero(decimal: BSONDecimal): BigDecimal = {
    val hest = highest(decimal)
    val exponent: Long = {
      if (hest) {
        ((decimal.high & 0x1FFFE00000000000L)
          >>> 47) - Decimal128.ExponentOffset
      } else {
        ((decimal.high & 0x7FFF800000000000L)
          >>> 49) - Decimal128.ExponentOffset
      }
    }

    val scale = -exponent.toInt

    if (hest) {
      BigDecimal.valueOf(0, scale)
    } else {
      val signum = if (decimal.isNegative) -1 else 1
      new BigDecimal(new BigInteger(signum, toBytes(decimal)), scale)
    }
  }

  /** Returns the binary representation (byte array).*/
  private def toBytes(decimal: BSONDecimal): Array[Byte] = {
    val bytes = Array.ofDim[Byte](15)

    @annotation.tailrec
    def lowBytes(i: Int, mask: Long): Unit = if (i >= 7) {
      bytes(i) = ((decimal.low & mask) >>> ((14 - i) << 3)).toByte
      lowBytes(i - 1, mask << 8)
    }

    lowBytes(14, 0x00000000000000FF)

    highBytes(6, 0x00000000000000FF)

    @annotation.tailrec
    def highBytes(i: Int, mask: Long): Unit = if (i >= 1) {
      bytes(i) = ((decimal.high & mask) >>> ((6 - i) << 3)).toByte
      highBytes(i - 1, mask << 8)
    }

    bytes(0) = ((decimal.high & 0x0001000000000000L) >>> 48).toByte

    bytes;
  }

  def toString(decimal: BSONDecimal): String = {
    if (decimal.isNaN) {
      "NaN"
    } else if (decimal.isInfinite) {
      if (decimal.isNegative) "-Infinity"
      else "Infinity"
    } else {
      bigDecimalString(decimal)
    }
  }

  private def bigDecimalString(decimal: BSONDecimal): String = {
    val buffer = StringBuilder.newBuilder
    val value = noNegativeZero(decimal)
    val significand = value.unscaledValue.abs.toString

    if (decimal.isNegative) {
      buffer += '-'
    }

    val exponent = -value.scale
    val normalizedExpo = exponent + (significand.length - 1)

    if (exponent <= 0 && normalizedExpo >= -6) {
      if (exponent == 0) {
        buffer ++= significand
      } else {
        val pad = -exponent - significand.length

        if (pad >= 0) {
          buffer ++= "0."

          (0 until pad).foreach { i =>
            buffer += '0'
          }

          buffer.appendAll(significand.toArray, 0, significand.length)
        } else {
          val sarray = significand.toArray

          buffer.appendAll(sarray, 0, -pad)
          buffer += '.'
          buffer.appendAll(sarray, -pad, exponent.abs) //-pad - exponent)
        }
      }
    } else {
      buffer ++= significand.take(1)

      if (significand.length > 1) {
        buffer += '.'
        buffer ++= significand.drop(1)
      }

      buffer += 'E'

      if (normalizedExpo > 0) {
        buffer += '+'
      }

      buffer ++= normalizedExpo.toString
    }

    buffer.result()
  }

  // ---

  private def clampRounded(value: BigDecimal): Try[BigDecimal] = {
    val scale = value.scale

    if (-scale > MaxExponent) {
      lazy val diff = -scale - MaxExponent
      val unscaled = value.unscaledValue

      if (unscaled equals BigInteger.ZERO) {
        Success(new BigDecimal(unscaled, -MaxExponent))
      } else if (diff + value.precision > 34) {
        Failure(new NumberFormatException(
          s"Exponent is out of range: ${value}"))
      } else {
        Try(BigInteger.TEN pow diff).map { multiplier =>
          new BigDecimal(unscaled.multiply(multiplier), scale + diff)
        }
      }
    } else if (-scale < MinExponent) {
      /*
       Increasing a very negative exponent may require decreasing precision,
       which is rounding.

       Only round exactly (by removing precision that is all zeroes).
       An exception is thrown if the rounding would be inexact:

       - Exact:   .000..0011000 => 11000E-6177 => 1100E-6176 => .000001100
       - Inexact: .000..0011001 => 11001E-6177 => 1100E-6176 => .000001100
       */
      val diff = scale + MinExponent
      val undiscardedPrecision = exactRounding(value, diff)

      val divisor: Try[BigInteger] = undiscardedPrecision.map {
        case 0 => BigInteger.ONE
        case _ => BigInteger.TEN pow diff
      }

      divisor.map { d =>
        new BigDecimal(value.unscaledValue.divide(d), scale - diff)
      }
    } else {
      Try(value round MathContext.DECIMAL128).flatMap { rounded =>
        val extraPrecision = value.precision - rounded.precision

        if (extraPrecision > 0) {
          // Again, only round exactly
          exactRounding(value, extraPrecision).map(_ => rounded)
        } else {
          Success(rounded)
        }
      }
    }
  }

  private def exactRounding(value: BigDecimal, precision: Int): Try[Int] = {
    val significand = value.unscaledValue.abs.toString()
    val undiscardedPrecision = Math.max(0, significand.length - precision)

    val limit = significand.length

    @annotation.tailrec
    def ensure(i: Int): Try[Int] = {
      if (i == limit) {
        Success(undiscardedPrecision)
      } else if (!significand.drop(i).headOption.exists(_ == '0')) {
        Failure[Int](new NumberFormatException(s"Inexact rounding of $value"))
      } else {
        ensure(i + 1)
      }
    }

    ensure(undiscardedPrecision)
  }

  // ---

  // Exponent range
  val MinExponent: Int = -6176
  val MaxExponent: Int = 6111

  // Sizing
  val ExponentOffset: Int = 6176
  val MaxBitLength: Int = 113

  // Infinity string representations
  val PositiveInfStrings = Set(
    "inf", "+inf", "infinity", "+infinity")

  val NegativeInfStrings = Set("-inf", "-infinity")
}
