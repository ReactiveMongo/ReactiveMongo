package reactivemongo.bson

import scala.math.Numeric

sealed trait BSONNumberLikeClass[B <: BSONValue] extends BSONNumberLike

sealed trait BSONNumberLike { self: BSONNumberLikeClass[_] =>
  private[bson] def underlying: BSONValue
  def toInt: Int
  def toLong: Long
  def toFloat: Float
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
}

sealed trait BSONBooleanLike { self: BSONBooleanLikeClass[_] =>
  private[bson] def underlying: BSONValue
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