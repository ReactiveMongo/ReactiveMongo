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

import scala.collection.generic.CanBuildFrom
import scala.util.{ Failure, Success, Try }

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

trait DefaultBSONHandlers {
  implicit object BSONIntegerHandler extends BSONHandler[BSONInteger, Int] {
    def read(int: BSONInteger) = int.value
    def write(int: Int) = BSONInteger(int)
  }
  implicit object BSONLongHandler extends BSONHandler[BSONLong, Long] {
    def read(long: BSONLong) = long.value
    def write(long: Long) = BSONLong(long)
  }
  implicit object BSONDoubleHandler extends BSONHandler[BSONDouble, Double] {
    def read(double: BSONDouble) = double.value
    def write(double: Double) = BSONDouble(double)
  }

  implicit object BSONStringHandler extends BSONHandler[BSONString, String] {
    def read(string: BSONString) = string.value
    def write(string: String) = BSONString(string)
  }
  implicit object BSONBooleanHandler extends BSONHandler[BSONBoolean, Boolean] {
    def read(boolean: BSONBoolean) = boolean.value
    def write(boolean: Boolean) = BSONBoolean(boolean)
  }

  // Typeclasses Handlers
  import BSONNumberLike._
  import BSONBooleanLike._

  class BSONNumberLikeReader[B <: BSONValue] extends BSONReader[B, BSONNumberLike] {
    def read(bson: B) = bson match {
      case int: BSONInteger => BSONIntegerNumberLike(int)
      case long: BSONLong => BSONLongNumberLike(long)
      case double: BSONDouble => BSONDoubleNumberLike(double)
      case _ => throw new UnsupportedOperationException()
    }
  }

  implicit object BSONNumberLikeWriter extends VariantBSONWriter[BSONNumberLike, BSONValue] {
    def write(number: BSONNumberLike) = number.underlying
  }

  implicit def bsonNumberLikeReader[B <: BSONValue] = new BSONNumberLikeReader[B]

  class BSONBooleanLikeReader[B <: BSONValue] extends BSONReader[B, BSONBooleanLike] {
    def read(bson: B) = bson match {
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

  implicit def bsonBooleanLikeReader[B <: BSONValue] = new BSONBooleanLikeReader[B]

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

  implicit object BSONStringIdentity extends BSONReader[BSONString, BSONString] with BSONWriter[BSONString, BSONString] {
    def read(b: BSONString) = b
    def write(b: BSONString) = b
  }

  implicit object BSONIntegerIdentity extends BSONReader[BSONInteger, BSONInteger] with BSONWriter[BSONInteger, BSONInteger] {
    def read(b: BSONInteger) = b
    def write(b: BSONInteger) = b
  }

  implicit object BSONArrayIdentity extends BSONReader[BSONArray, BSONArray] with BSONWriter[BSONArray, BSONArray] {
    def read(b: BSONArray) = b
    def write(b: BSONArray) = b
  }

  implicit object BSONDocumentIdentity extends BSONReader[BSONDocument, BSONDocument] with BSONWriter[BSONDocument, BSONDocument] with BSONDocumentReader[BSONDocument] with BSONDocumentWriter[BSONDocument] {
    def read(b: BSONDocument) = b
    def write(b: BSONDocument) = b
  }

  implicit object BSONBooleanIdentity extends BSONReader[BSONBoolean, BSONBoolean] with BSONWriter[BSONBoolean, BSONBoolean] {
    def read(b: BSONBoolean) = b
    def write(b: BSONBoolean) = b
  }

  implicit object BSONLongIdentity extends BSONReader[BSONLong, BSONLong] with BSONWriter[BSONLong, BSONLong] {
    def read(b: BSONLong) = b
    def write(b: BSONLong) = b
  }

  implicit object BSONDoubleIdentity extends BSONReader[BSONDouble, BSONDouble] with BSONWriter[BSONDouble, BSONDouble] {
    def read(b: BSONDouble) = b
    def write(b: BSONDouble) = b
  }

  implicit object BSONValueIdentity extends BSONReader[BSONValue, BSONValue] with BSONWriter[BSONValue, BSONValue] {
    def read(b: BSONValue) = b
    def write(b: BSONValue) = b
  }

  implicit object BSONObjectIDIdentity extends BSONReader[BSONObjectID, BSONObjectID] with BSONWriter[BSONObjectID, BSONObjectID] {
    def read(b: BSONObjectID) = b
    def write(b: BSONObjectID) = b
  }

  implicit object BSONBinaryIdentity extends BSONReader[BSONBinary, BSONBinary] with BSONWriter[BSONBinary, BSONBinary] {
    def read(b: BSONBinary) = b
    def write(b: BSONBinary) = b
  }

  implicit object BSONDateTimeIdentity extends BSONReader[BSONDateTime, BSONDateTime] with BSONWriter[BSONDateTime, BSONDateTime] {
    def read(b: BSONDateTime) = b
    def write(b: BSONDateTime) = b
  }

  implicit object BSONNullIdentity extends BSONReader[BSONNull.type, BSONNull.type] with BSONWriter[BSONNull.type, BSONNull.type] {
    def read(b: BSONNull.type) = b
    def write(b: BSONNull.type) = b
  }

  implicit object BSONUndefinedIdentity extends BSONReader[BSONUndefined.type, BSONUndefined.type] with BSONWriter[BSONUndefined.type, BSONUndefined.type] {
    def read(b: BSONUndefined.type) = b
    def write(b: BSONUndefined.type) = b
  }

  implicit object BSONRegexIdentity extends BSONReader[BSONRegex, BSONRegex] with BSONWriter[BSONRegex, BSONRegex] {
    def read(b: BSONRegex) = b
    def write(b: BSONRegex) = b
  }

  implicit object BSONJavaScriptIdentity extends BSONReader[BSONJavaScript, BSONJavaScript] with BSONWriter[BSONJavaScript, BSONJavaScript] {
    def read(b: BSONJavaScript) = b
    def write(b: BSONJavaScript) = b
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