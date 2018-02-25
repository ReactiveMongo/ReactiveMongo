/*
 * Copyright 2013 Stephane Godbillon (@sgodbillon)
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
package reactivemongo.bson

import scala.math.Numeric
import scala.util.{ Success, Try }

sealed trait BSONNumberLikeClass[B <: BSONValue] extends BSONNumberLike

/**
 * A BSON value that can be seen as a number.
 *
 * Conversions:
 *   - [[BSONDateTime]]
 *   - [[BSONDecimal]]
 *   - [[BSONDouble]]
 *   - [[BSONInteger]]
 *   - [[BSONLong]]
 *   - [[BSONTimestamp]]
 */
sealed trait BSONNumberLike { self: BSONNumberLikeClass[_] =>
  private[bson] def underlying: BSONValue

  /** Converts this number into an `Int`. */
  def toInt: Int

  /** Converts this number into a `Long`. */
  def toLong: Long

  /** Converts this number into a `Float`. */
  def toFloat: Float

  /** Converts this number into a `Double`. */
  def toDouble: Double
}

private[bson] sealed trait HasNumeric[A] {
  private[bson] def number: ExtendedNumeric[A]
}

private[bson] sealed trait IsNumeric[A] extends HasNumeric[A] {
  def toInt = number.numeric.toInt(number.value)
  def toLong = number.numeric.toLong(number.value)
  def toFloat = number.numeric.toFloat(number.value)
  def toDouble = number.numeric.toDouble(number.value)
}

private[bson] sealed trait IsBooleanLike[A] extends HasNumeric[A] {
  def toBoolean: Boolean = number.numeric.compare(
    number.value, number.numeric.zero) != 0
}

private[bson] class ExtendedNumeric[A: Numeric](provider: Try[A]) {
  @deprecated("Use the safe `provider`", "0.13.0")
  final lazy val value: A = provider.get

  def numeric = implicitly[Numeric[A]]
}

private[bson] object ExtendedNumeric {
  @deprecated("Use the constructor with a `provider`", "0.13.0")
  def apply[A: Numeric](pure: A): ExtendedNumeric[A] =
    new ExtendedNumeric[A](Success(pure))
}

object BSONNumberLike {
  implicit class BSONDateTimeNumberLike(
    private[bson] val underlying: BSONDateTime)
    extends BSONNumberLikeClass[BSONDateTime] with IsNumeric[Long] {
    private[bson] lazy val number = ExtendedNumeric(underlying.value)
  }

  implicit final class BSONDecimalNumberLike(
    private[bson] val underlying: BSONDecimal)
    extends BSONNumberLikeClass[BSONDecimal] with IsNumeric[BigDecimal] {
    private[bson] lazy val number = new ExtendedNumeric(
      BSONDecimal.toBigDecimal(underlying))
  }

  implicit class BSONDoubleNumberLike(
    private[bson] val underlying: BSONDouble)
    extends BSONNumberLikeClass[BSONDouble] with IsNumeric[Double] {
    private[bson] lazy val number = ExtendedNumeric(underlying.value)
  }

  implicit class BSONIntegerNumberLike(
    private[bson] val underlying: BSONInteger)
    extends BSONNumberLikeClass[BSONInteger] with IsNumeric[Int] {
    private[bson] lazy val number = ExtendedNumeric(underlying.value)
  }

  implicit class BSONLongNumberLike(
    private[bson] val underlying: BSONLong)
    extends BSONNumberLikeClass[BSONLong] with IsNumeric[Long] {
    private[bson] lazy val number = ExtendedNumeric(underlying.value)
  }

  implicit class BSONTimestampNumberLike(
    private[bson] val underlying: BSONTimestamp)
    extends BSONNumberLikeClass[BSONTimestamp] with IsNumeric[Long] {
    private[bson] lazy val number = ExtendedNumeric(underlying.value * 1000L)
  }
}

/**
 * A BSON value that can be seen as a boolean.
 *
 * Conversions:
 *   - `number = 0 ~> false`
 *   - `number != 0 ~> true`
 *   - `boolean`
 *   - `undefined ~> false`
 *   - `null ~> false`
 */
sealed trait BSONBooleanLike { _: BSONBooleanLikeClass[_] =>
  private[bson] def underlying: BSONValue

  /** Returns the boolean equivalent value */
  def toBoolean: Boolean
}

sealed trait BSONBooleanLikeClass[B <: BSONValue] extends BSONBooleanLike

object BSONBooleanLike {
  implicit class BSONBooleanBooleanLike(
    private[bson] val underlying: BSONBoolean)
    extends BSONBooleanLikeClass[BSONBoolean] {
    def toBoolean = underlying.value
    override def toString = s"BSONBooleanBooleanLike($underlying)"
  }

  implicit final class BSONDecimalBooleanLike(
    private[bson] val underlying: BSONDecimal)
    extends BSONBooleanLikeClass[BSONDecimal]
    with IsBooleanLike[BigDecimal] {
    private[bson] lazy val number = new ExtendedNumeric(
      BSONDecimal.toBigDecimal(underlying))
    override def toString = s"BSONDecimalBooleanLike($underlying)"
  }

  implicit class BSONDoubleBooleanLike(
    private[bson] val underlying: BSONDouble)
    extends BSONBooleanLikeClass[BSONDouble] with IsBooleanLike[Double] {
    private[bson] lazy val number = ExtendedNumeric(underlying.value)
    override def toString = s"BSONDoubleBooleanLike($underlying)"
  }

  implicit class BSONNullBooleanLike(
    private[bson] val underlying: BSONNull.type)
    extends BSONBooleanLikeClass[BSONNull.type] {
    val toBoolean = false
    override def toString = "BSONNullBooleanLike"
  }

  implicit class BSONUndefinedBooleanLike(
    private[bson] val underlying: BSONUndefined.type)
    extends BSONBooleanLikeClass[BSONUndefined.type] {
    val toBoolean = false
    override def toString = "BSONUndefinedBooleanLike"
  }

  implicit class BSONIntegerBooleanLike(
    private[bson] val underlying: BSONInteger)
    extends BSONBooleanLikeClass[BSONInteger] with IsBooleanLike[Int] {
    private[bson] lazy val number = ExtendedNumeric(underlying.value)
    override def toString = s"BSONIntegerBooleanLike($underlying)"
  }

  implicit class BSONLongBooleanLike(
    private[bson] val underlying: BSONLong)
    extends BSONBooleanLikeClass[BSONDouble] with IsBooleanLike[Long] {
    private[bson] lazy val number = ExtendedNumeric(underlying.value)
    override def toString = s"BSONLongBooleanLike($underlying)"
  }
}
