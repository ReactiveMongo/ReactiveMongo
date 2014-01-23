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

sealed trait BSONNumberLikeClass[B <: BSONValue] extends BSONNumberLike

/**
 * A BSON value that can be seen as a number.
 *
 * Conversions:
 *   - [[BSONLong]]
 *   - [[BSONInteger]]
 *   - [[BSONDouble]]
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
  def toBoolean: Boolean = number.numeric.compare(number.value, number.numeric.zero) != 0
}

private[bson] case class ExtendedNumeric[A](value: A)(implicit val numeric: Numeric[A]) {}

object BSONNumberLike {
  implicit class BSONIntegerNumberLike(private[bson] val underlying: BSONInteger) extends BSONNumberLikeClass[BSONInteger] with IsNumeric[Int] {
    private[bson] lazy val number = ExtendedNumeric(underlying.value)
  }
  implicit class BSONDoubleNumberLike(private[bson] val underlying: BSONDouble) extends BSONNumberLikeClass[BSONDouble] with IsNumeric[Double] {
    private[bson] lazy val number = ExtendedNumeric(underlying.value)
  }
  implicit class BSONLongNumberLike(private[bson] val underlying: BSONLong) extends BSONNumberLikeClass[BSONLong] with IsNumeric[Long] {
    private[bson] lazy val number = ExtendedNumeric(underlying.value)
  }
  implicit class BSONDateTimeNumberLike(private[bson] val underlying: BSONDateTime) extends BSONNumberLikeClass[BSONDateTime] with IsNumeric[Long] {
    private[bson] lazy val number = ExtendedNumeric(underlying.value)
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
sealed trait BSONBooleanLike { self: BSONBooleanLikeClass[_] =>
  private[bson] def underlying: BSONValue
  /** Returns the boolean equivalent value */
  def toBoolean: Boolean
}
sealed trait BSONBooleanLikeClass[B <: BSONValue] extends BSONBooleanLike

object BSONBooleanLike {
  implicit class BSONBooleanBooleanLike(private[bson] val underlying: BSONBoolean) extends BSONBooleanLikeClass[BSONBoolean] {
    def toBoolean = underlying.value
  }
  implicit class BSONNullBooleanLike(private[bson] val underlying: BSONNull.type) extends BSONBooleanLikeClass[BSONNull.type] {
    val toBoolean = false
  }
  implicit class BSONUndefinedBooleanLike(private[bson] val underlying: BSONUndefined.type) extends BSONBooleanLikeClass[BSONUndefined.type] {
    val toBoolean = false
  }
  implicit class BSONIntegerBooleanLike(private[bson] val underlying: BSONInteger) extends BSONBooleanLikeClass[BSONInteger] with IsBooleanLike[Int] {
    private[bson] lazy val number = ExtendedNumeric(underlying.value)
  }
  implicit class BSONDoubleBooleanLike(private[bson] val underlying: BSONDouble) extends BSONBooleanLikeClass[BSONDouble] with IsBooleanLike[Double] {
    private[bson] lazy val number = ExtendedNumeric(underlying.value)
  }
  implicit class BSONLongBooleanLike(private[bson] val underlying: BSONLong) extends BSONBooleanLikeClass[BSONDouble] with IsBooleanLike[Long] {
    private[bson] lazy val number = ExtendedNumeric(underlying.value)
  }
}