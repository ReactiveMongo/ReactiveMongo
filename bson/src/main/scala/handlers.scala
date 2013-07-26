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

import reactivemongo.bson.exceptions.{ValueIsNull, DeserializationException}
import scala.collection.generic.CanBuildFrom
import scala.util.{ Failure, Try }

/**
 * A reader that produces an instance of `T` from a subtype of [[BSONValue]].
 */
trait BSONReader[B <: BSONValue, T] {
  /**
   * Reads a BSON value and produce an instance of `T`.
   *
   * This method may throw exceptions at runtime.
   * If used outside a reader, one should consider `readTry(bson: B): Try[T]` or `readOpt(bson: B): Option[T]`.
   */
  def read(bson: B): T
  /** Tries to produce an instance of `T` from the `bson` value, returns `None` if an error occurred. */
  def readOpt(bson: B): Option[T] = readTry(bson).toOption
  /** Tries to produce an instance of `T` from the `bson` value. */
  def readTry(bson: B): Try[T] = Try(read(bson))
}

/**
 * A writer that produces a subtype of [[BSONValue]] fron an instance of `T`.
 */
trait BSONWriter[T, B <: BSONValue] {
  /**
   * Writes an instance of `T` as a BSON value.
   *
   * This method may throw exceptions at runtime.
   * If used outside a reader, one should consider `writeTry(bson: B): Try[T]` or `writeOpt(bson: B): Option[T]`.
   */
  def write(t: T): B
  /** Tries to produce a BSON value from an instance of `T`, returns `None` if an error occurred. */
  def writeOpt(t: T): Option[B] = writeTry(t).toOption
  /** Tries to produce a BSON value from an instance of `T`. */
  def writeTry(t: T): Try[B] = Try(write(t))
}

/**
 * A reader that produces an instance of `T` from a subtype of [[BSONValue]].
 */
trait VariantBSONReader[-B <: BSONValue, +T] {
  /**
   * Reads a BSON value and produce an instance of `T`.
   *
   * This method may throw exceptions at runtime.
   * If used outside a reader, one should consider `readTry(bson: B): Try[T]` or `readOpt(bson: B): Option[T]`.
   */
  def read(bson: B): T
  /** Tries to produce an instance of `T` from the `bson` value, returns `None` if an error occurred. */
  def readOpt(bson: B): Option[T] = readTry(bson).toOption
  /** Tries to produce an instance of `T` from the `bson` value. */
  def readTry(bson: B): Try[T] = Try(read(bson))
}

/**
 * A writer that produces a subtype of [[BSONValue]] fron an instance of `T`.
 */
trait VariantBSONWriter[-T, +B <: BSONValue] {
  /**
   * Writes an instance of `T` as a BSON value.
   *
   * This method may throw exceptions at runtime.
   * If used outside a reader, one should consider `writeTry(bson: B): Try[T]` or `writeOpt(bson: B): Option[T]`.
   */
  def write(t: T): B
  /** Tries to produce a BSON value from an instance of `T`, returns `None` if an error occurred. */
  def writeOpt(t: T): Option[B] = writeTry(t).toOption
  /** Tries to produce a BSON value from an instance of `T`. */
  def writeTry(t: T): Try[B] = Try(write(t))
}

trait BSONDocumentReader[T] extends BSONReader[BSONDocument, T]
trait BSONDocumentWriter[T] extends BSONWriter[T, BSONDocument]
trait VariantBSONDocumentReader[+T] extends VariantBSONReader[BSONDocument, T]
trait VariantBSONDocumentWriter[-T] extends VariantBSONWriter[T, BSONDocument]

class VariantBSONWriterWrapper[T, B <: BSONValue](writer: VariantBSONWriter[T, B]) extends BSONWriter[T, B] {
  def write(t: T) = writer.write(t)
}

class VariantBSONReaderWrapper[B <: BSONValue, T](reader: VariantBSONReader[B, T]) extends BSONReader[B, T] {
  def read(b: B) = reader.read(b)
}

trait BSONHandler[B <: BSONValue, T] extends BSONReader[B, T] with BSONWriter[T, B]

trait AllowNullable[T] extends BSONReader[BSONValue, T] {
  override def readTry(bson: BSONValue): Try[T] = bson match {
    case BSONNull | BSONUndefined => Failure(ValueIsNull)
    case x => super.readTry(x)
  }
}

trait DefaultBSONHandlers {
  import DefaultBSONHandlers._

  implicit object BSONIntegerHandler extends BSONHandler[BSONValue, Int] with AllowNullable[Int] {
    def read(bson: BSONValue) = bson match {
      case BSONInteger(value) => value
      case x                  => deserializationError(s"Expected BSONInteger, but got $x")
    }
    def write(int: Int) = BSONInteger(int)
  }
  implicit object BSONLongHandler extends BSONHandler[BSONValue, Long] with AllowNullable[Long] {
    def read(bson: BSONValue) = bson match {
      case BSONLong(value) => value
      case x               => deserializationError(s"Expected BSONLong, but got $x")
    }
    def write(long: Long) = BSONLong(long)
  }
  implicit object BSONDoubleHandler extends BSONHandler[BSONValue, Double] with AllowNullable[Double] {
    def read(bson: BSONValue) = bson match {
      case BSONDouble(value) => value
      case x                 => deserializationError(s"Expected BSONDouble, but got $x")
    }
    def write(double: Double) = BSONDouble(double)
  }

  implicit object BSONStringHandler extends BSONHandler[BSONValue, String] with AllowNullable[String] {
    def read(bson: BSONValue) = bson match {
      case BSONString(value) => value
      case x                 => deserializationError(s"Expected BSONString, but got $x")
    }
    def write(string: String) = BSONString(string)
  }
  implicit object BSONBooleanHandler extends BSONHandler[BSONValue, Boolean] with AllowNullable[Boolean] {
    def read(bson: BSONValue) = bson match {
      case BSONBoolean(value) => value
      case x                  => deserializationError(s"Expected BSONBoolean, but got $x")
    }
    def write(boolean: Boolean) = BSONBoolean(boolean)
  }

  // Typeclasses Handlers
  import BSONNumberLike._
  import BSONBooleanLike._

  implicit object BSONNumberLikeReader extends BSONReader[BSONValue, BSONNumberLike] with AllowNullable[BSONNumberLike] {
    def read(bson: BSONValue) = bson match {
      case int: BSONInteger => BSONIntegerNumberLike(int)
      case long: BSONLong => BSONLongNumberLike(long)
      case double: BSONDouble => BSONDoubleNumberLike(double)
      case _ => throw new UnsupportedOperationException()
    }
  }

  implicit object BSONNumberLikeWriter extends VariantBSONWriter[BSONNumberLike, BSONValue] {
    def write(number: BSONNumberLike) = number.underlying
  }

  implicit object BSONBooleanLikeReader extends BSONReader[BSONValue, BSONBooleanLike] {
    def read(bson: BSONValue) = bson match {
      case int: BSONInteger => BSONIntegerBooleanLike(int)
      case double: BSONDouble => BSONDoubleBooleanLike(double)
      case long: BSONLong => BSONLongBooleanLike(long)
      case boolean: BSONBoolean => BSONBooleanBooleanLike(boolean)
      case nll: BSONNull.type => BSONNullBooleanLike(BSONNull)
      case udf: BSONUndefined.type => BSONUndefinedBooleanLike(BSONUndefined)
      case _ => throw new UnsupportedOperationException()
    }
  }

  implicit object BSONBooleanLikeWriter extends VariantBSONWriter[BSONBooleanLike, BSONValue] {
    def write(number: BSONBooleanLike) = number.underlying
  }

  // Collections Handlers
  class BSONArrayCollectionReader[M[_], T](implicit cbf: CanBuildFrom[M[_], T, M[T]], reader: BSONReader[_ <: BSONValue, T]) extends BSONReader[BSONArray, M[T]] {
    def read(array: BSONArray) =
      array.stream.filter(_.isSuccess).map { v =>
        reader.asInstanceOf[BSONReader[BSONValue, T]].read(v.get)
      }.to[M]
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

  implicit object BSONStringIdentity extends BSONReader[BSONValue, BSONString] with BSONWriter[BSONString, BSONValue] with AllowNullable[BSONString] {
    def read(bson: BSONValue) = bson match {
      case x: BSONString => x
      case x             => deserializationError(s"Expected BSONString, but got: $x")
    }
    def write(b: BSONString) = b
  }

  implicit object BSONIntegerIdentity extends BSONReader[BSONValue, BSONInteger] with BSONWriter[BSONInteger, BSONValue] with AllowNullable[BSONInteger] {
    def read(bson: BSONValue) = bson match {
      case x: BSONInteger => x
      case x              => deserializationError(s"Expected BSONInteger, but got: $x")
    }
    def write(b: BSONInteger) = b
  }

  implicit object BSONArrayIdentity extends BSONReader[BSONValue, BSONArray] with BSONWriter[BSONArray, BSONValue] with AllowNullable[BSONArray] {
    def read(bson: BSONValue) = bson match {
      case x: BSONArray => x
      case x            => deserializationError(s"Expected BSONArray, but got: $x")
    }
    def write(b: BSONArray) = b
  }

  implicit object BSONDocumentIdentity extends BSONReader[BSONDocument, BSONDocument] with BSONWriter[BSONDocument, BSONDocument] with BSONDocumentReader[BSONDocument] with BSONDocumentWriter[BSONDocument] {
    def read(b: BSONDocument) = b
    def write(b: BSONDocument) = b
  }

  implicit object BSONBooleanIdentity extends BSONReader[BSONValue, BSONBoolean] with BSONWriter[BSONBoolean, BSONValue] with AllowNullable[BSONBoolean] {
    def read(bson: BSONValue) = bson match {
      case x: BSONBoolean => x
      case x              => deserializationError(s"Expected BSONBoolean, but got: $x")
    }
    def write(b: BSONBoolean) = b
  }

  implicit object BSONLongIdentity extends BSONReader[BSONValue, BSONLong] with BSONWriter[BSONLong, BSONValue] with AllowNullable[BSONLong] {
    def read(bson: BSONValue) = bson match {
      case x: BSONLong => x
      case x           => deserializationError(s"Expected BSONLong, but got: $x")
    }
    def write(b: BSONLong) = b
  }

  implicit object BSONDoubleIdentity extends BSONReader[BSONValue, BSONDouble] with BSONWriter[BSONDouble, BSONValue] with AllowNullable[BSONDouble] {
    def read(bson: BSONValue) = bson match {
      case x: BSONDouble => x
      case x             => deserializationError(s"Expected BSONDouble, but got: $x")
    }
    def write(b: BSONDouble) = b
  }

  implicit object BSONValueIdentity extends BSONReader[BSONValue, BSONValue] with BSONWriter[BSONValue, BSONValue] {
    def read(b: BSONValue) = b
    def write(b: BSONValue) = b
  }

  implicit object BSONObjectIDIdentity extends BSONReader[BSONValue, BSONObjectID] with BSONWriter[BSONObjectID, BSONValue] with AllowNullable[BSONObjectID] {
    def read(bson: BSONValue) = bson match {
      case x: BSONObjectID => x
      case x               => deserializationError(s"Expected BSONObjectID, but got: $x")
    }
    def write(b: BSONObjectID) = b
  }

  implicit object BSONBinaryIdentity extends BSONReader[BSONValue, BSONBinary] with BSONWriter[BSONBinary, BSONValue] with AllowNullable[BSONBinary] {
    def read(bson: BSONValue) = bson match {
      case x: BSONBinary => x
      case x             => deserializationError(s"Expected BSONBinary, but got: $x")
    }
    def write(b: BSONBinary) = b
  }

  implicit object BSONDateTimeIdentity extends BSONReader[BSONValue, BSONDateTime] with BSONWriter[BSONDateTime, BSONValue] with AllowNullable[BSONDateTime] {
    def read(bson: BSONValue) = bson match {
      case x: BSONDateTime => x
      case x               => deserializationError(s"Expected BSONDateTime, but got: $x")
    }
    def write(b: BSONDateTime) = b
  }

  implicit object BSONNullIdentity extends BSONReader[BSONValue, BSONNull.type] with BSONWriter[BSONNull.type, BSONValue] {
    def read(bson: BSONValue) = bson match {
      case BSONNull => BSONNull
      case x        => deserializationError(s"Expected BSONNull, but got: $x")
    }
    def write(b: BSONNull.type) = b
  }

  implicit object BSONUndefinedIdentity extends BSONReader[BSONValue, BSONUndefined.type] with BSONWriter[BSONUndefined.type, BSONValue] {
    def read(bson: BSONValue) = bson match {
      case BSONUndefined => BSONUndefined
      case x             => deserializationError(s"Expected BSONUndefined, but got: $x")
    }
    def write(b: BSONUndefined.type) = b
  }

  implicit object BSONRegexIdentity extends BSONReader[BSONValue, BSONRegex] with BSONWriter[BSONRegex, BSONValue] with AllowNullable[BSONRegex] {
    def read(bson: BSONValue) = bson match {
      case x: BSONRegex => x
      case x            => deserializationError(s"Expected BSONRegex, but got: $x")
    }
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

object DefaultBSONHandlers extends DefaultBSONHandlers {
  def deserializationError(msg: String) = throw new DeserializationException(msg)
}