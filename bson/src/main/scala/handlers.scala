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

  implicit object BSONStringIdentity extends BSONReader[BSONString, BSONString] with BSONWriter[BSONString, BSONString] {
    def readTry(b: BSONString) = Try(b)
    def write(b: BSONString) = b
  }

  implicit object BSONIntegerIdentity extends BSONReader[BSONInteger, BSONInteger] with BSONWriter[BSONInteger, BSONInteger] {
    def readTry(b: BSONInteger) = Try(b)
    def write(b: BSONInteger) = b
  }

  implicit object BSONArrayIdentity extends BSONReader[BSONArray, BSONArray] with BSONWriter[BSONArray, BSONArray] {
    def readTry(b: BSONArray) = Try(b)
    def write(b: BSONArray) = b
  }

  implicit object BSONDocumentIdentity extends BSONReader[BSONDocument, BSONDocument] with BSONWriter[BSONDocument, BSONDocument] {
    def readTry(b: BSONDocument) = Try(b)
    def write(b: BSONDocument) = b
  }

  implicit object BSONBooleanIdentity extends BSONReader[BSONBoolean, BSONBoolean] with BSONWriter[BSONBoolean, BSONBoolean] {
    def readTry(b: BSONBoolean) = Try(b)
    def write(b: BSONBoolean) = b
  }

  implicit object BSONLongIdentity extends BSONReader[BSONLong, BSONLong] with BSONWriter[BSONLong, BSONLong] {
    def readTry(b: BSONLong) = Try(b)
    def write(b: BSONLong) = b
  }

  implicit object BSONDoubleIdentity extends BSONReader[BSONDouble, BSONDouble] with BSONWriter[BSONDouble, BSONDouble] {
    def readTry(b: BSONDouble) = Try(b)
    def write(b: BSONDouble) = b
  }

  implicit object BSONValueIdentity extends BSONReader[BSONValue, BSONValue] with BSONWriter[BSONValue, BSONValue] {
    def readTry(b: BSONValue) = Try(b)
    def write(b: BSONValue) = b
  }

  implicit object BSONObjectIDIdentity extends BSONReader[BSONObjectID, BSONObjectID] with BSONWriter[BSONObjectID, BSONObjectID] {
    def readTry(b: BSONObjectID) = Try(b)
    def write(b: BSONObjectID) = b
  }

  implicit object BSONBinaryIdentity extends BSONReader[BSONBinary, BSONBinary] with BSONWriter[BSONBinary, BSONBinary] {
    def readTry(b: BSONBinary) = Try(b)
    def write(b: BSONBinary) = b
  }

  implicit object BSONDateTimeIdentity extends BSONReader[BSONDateTime, BSONDateTime] with BSONWriter[BSONDateTime, BSONDateTime] {
    def readTry(b: BSONDateTime) = Try(b)
    def write(b: BSONDateTime) = b
  }

  implicit object BSONNullIdentity extends BSONReader[BSONNull.type, BSONNull.type] with BSONWriter[BSONNull.type, BSONNull.type] {
    def readTry(b: BSONNull.type) = Try(b)
    def write(b: BSONNull.type) = b
  }

  implicit object BSONUndefinedIdentity extends BSONReader[BSONUndefined.type, BSONUndefined.type] with BSONWriter[BSONUndefined.type, BSONUndefined.type] {
    def readTry(b: BSONUndefined.type) = Try(b)
    def write(b: BSONUndefined.type) = b
  }

  implicit object BSONRegexIdentity extends BSONReader[BSONRegex, BSONRegex] with BSONWriter[BSONRegex, BSONRegex] {
    def readTry(b: BSONRegex) = Try(b)
    def write(b: BSONRegex) = b
  }

  /*// Generic Handlers
  class BSONValueIdentity[B <: BSONValue] extends BSONWriter[B, B] with BSONReader[B, B] {
    def write(b: B): B = b
    def readTry(b: B): Try[B] = Try(b.asInstanceOf[B])
  }

  implicit def findIdentityWriter[B <: BSONValue]: BSONWriter[B, B] = /*new VariantBSONWriterWrapper(*/new BSONValueIdentity[B]//)

  implicit def findIdentityReader[B <: BSONValue]: BSONReader[B, B] = /*new VariantBSONReaderWrapper(*/new BSONValueIdentity[B]//)*/

  implicit def findWriter[T](implicit writer: VariantBSONWriter[T, _ <: BSONValue]): BSONWriter[T, _ <: BSONValue] =
    new VariantBSONWriterWrapper(writer)

  implicit def findReader[T](implicit reader: VariantBSONReader[_ <: BSONValue, T]): BSONReader[_ <: BSONValue, T] =
    new VariantBSONReaderWrapper(reader)
}

object DefaultBSONHandlers extends DefaultBSONHandlers