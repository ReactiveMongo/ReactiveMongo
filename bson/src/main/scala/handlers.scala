package reactivemongo.bson

import scala.collection.generic.CanBuildFrom
import scala.util.{ Failure, Success, Try }

trait BSONReader[B <: BSONValue, T] {
  def read(bson: B): T = readTry(bson).get
  def readOpt(bson: B): Option[T] = readTry(bson).toOption
  def readTry(bson: B): Try[T]
}

trait BSONWriter[T, B <: BSONValue] {
  def write(t: T): B
}

trait VariantBSONReader[-B <: BSONValue, +T] {
  def read(bson: B): T = readTry(bson).get
  def readOpt(bson: B): Option[T] = readTry(bson).toOption
  def readTry(bson: B): Try[T]
}

trait VariantBSONWriter[-T, +B <: BSONValue] {
  def write(t: T): B
}

class VariantBSONWriterWrapper[T, B <: BSONValue](writer: VariantBSONWriter[T, B]) extends BSONWriter[T, B] {
  def write(t: T) = writer.write(t)
}

class VariantBSONReaderWrapper[B <: BSONValue, T](reader: VariantBSONReader[B, T]) extends BSONReader[B, T] {
  def readTry(b: B) = reader.readTry(b)
}

trait BSONHandler[B <: BSONValue, T] extends BSONReader[B, T] with BSONWriter[T, B]

trait DefaultBSONHandlers {
  implicit object BSONIntegerHandler extends BSONHandler[BSONInteger, Int] {
    def readTry(int: BSONInteger) = Try(int.value)
    def write(int: Int) = BSONInteger(int)
  }
  implicit object BSONLongHandler extends BSONHandler[BSONLong, Long] {
    def readTry(long: BSONLong) = Try(long.value)
    def write(long: Long) = BSONLong(long)
  }
  implicit object BSONDoubleHandler extends BSONHandler[BSONDouble, Double] {
    def readTry(double: BSONDouble) = Try(double.value)
    def write(double: Double) = BSONDouble(double)
  }

  implicit object BSONStringHandler extends BSONHandler[BSONString, String] {
    def readTry(string: BSONString) = Try(string.value)
    def write(string: String) = BSONString(string)
  }
  implicit object BSONBooleanHandler extends BSONHandler[BSONBoolean, Boolean] {
    def readTry(boolean: BSONBoolean) = Try(boolean.value)
    def write(boolean: Boolean) = BSONBoolean(boolean)
  }

  // Typeclasses Handlers
  import BSONNumberLike._
  import BSONBooleanLike._

  class BSONNumberLikeReader[B <: BSONValue] extends BSONReader[B, BSONNumberLike] {
    def readTry(bson: B) = Try(bson match {
      case int: BSONInteger => BSONIntegerNumberLike(int)
      case long: BSONLong => BSONLongNumberLike(long)
      case double: BSONDouble => BSONDoubleNumberLike(double)
      case _ => throw new UnsupportedOperationException()
    })
  }

  implicit object BSONNumberLikeWriter extends VariantBSONWriter[BSONNumberLike, BSONValue] {
    def write(number: BSONNumberLike) = number.underlying
  }

  implicit def bsonNumberLikeReader[B <: BSONValue] = new BSONNumberLikeReader[B]

  
  
  class BSONBooleanLikeReader[B <: BSONValue] extends BSONReader[B, BSONBooleanLike] {
    def readTry(bson: B) = Try(bson match {
      case int: BSONInteger => BSONIntegerBooleanLike(int)
      case double: BSONDouble => BSONDoubleBooleanLike(double)
      case long: BSONLong => BSONLongBooleanLike(long)
      case boolean: BSONBoolean => BSONBooleanBooleanLike(boolean)
      case nll: BSONNull.type => BSONNullBooleanLike(BSONNull)
      case udf: BSONUndefined.type => BSONUndefinedBooleanLike(BSONUndefined)
      case _ => throw new UnsupportedOperationException()
    })
  }

  implicit object BSONBooleanLikeWriter extends VariantBSONWriter[BSONBooleanLike, BSONValue] {
    def write(number: BSONBooleanLike) = number.underlying
  }

  implicit def bsonBooleanLikeReader[B <: BSONValue] = new BSONBooleanLikeReader[B]

  // Collections Handlers
  class BSONArrayCollectionReader[M[_], T](implicit cbf: CanBuildFrom[M[_], T, M[T]], reader: BSONReader[_ <: BSONValue, T]) extends BSONReader[BSONArray, M[T]] {
    def readTry(array: BSONArray) = Try {
      array.stream.filter(_.isSuccess).map { v =>
        reader.asInstanceOf[BSONReader[BSONValue, T]].read(v.get)
      }.to[M]
    }
  }

  class BSONArrayCollectionWriter[T, Repr <% Traversable[T]](implicit writer: BSONWriter[T, _ <: BSONValue]) extends VariantBSONWriter[Repr, BSONArray] {
    def write(repr: Repr) = {
      new BSONArray(repr.map(s => Try(writer.write(s))).to[Stream])
    }
  }

  implicit def collectionToBSONArrayCollectionWriter[T, Repr <% Traversable[T]](implicit writer: BSONWriter[T, _ <: BSONValue]): VariantBSONWriter[Repr, BSONArray] = {
    new BSONArrayCollectionWriter[T, Repr]
  }

  implicit def bsonArrayToCollectionReader[M[_], T](implicit cbf: CanBuildFrom[M[_], T, M[T]], reader: BSONReader[_ <: BSONValue, T]): BSONReader[BSONArray, M[T]] = {
    new BSONArrayCollectionReader
  }

  // Generic Handlers
  class BSONValueIdentity[B <: BSONValue] extends VariantBSONWriter[B, B] with VariantBSONReader[B, B] {
    def write(b: B): B = b
    def readTry(b: B): Try[B] = Try(b)
  }

  implicit def findIdentityWriter[B <: BSONValue]: BSONWriter[B, B] = new VariantBSONWriterWrapper(new BSONValueIdentity[B])

  implicit def findIdentityReader[B <: BSONValue]: BSONReader[B, B] = new VariantBSONReaderWrapper(new BSONValueIdentity[B])

  implicit def findWriter[T](implicit writer: VariantBSONWriter[T, _ <: BSONValue]): BSONWriter[T, _ <: BSONValue] =
    new VariantBSONWriterWrapper(writer)

  implicit def findReader[T](implicit reader: VariantBSONReader[_ <: BSONValue, T]): BSONReader[_ <: BSONValue, T] =
    new VariantBSONReaderWrapper(reader)
}

object DefaultBSONHandlers extends DefaultBSONHandlers