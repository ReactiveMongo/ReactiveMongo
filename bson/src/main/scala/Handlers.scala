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

sealed trait UnsafeBSONReader[T] {
  def readTry(value: BSONValue): Try[T]
}

/**
 * A reader that produces an instance of `T` from a subtype of [[BSONValue]].
 */
trait BSONReader[B <: BSONValue, T] { self =>

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

  /**
   * Returns a BSON reader that returns the result of applying `f`
   * on the result of this reader.
   *
   * @param f the function to apply
   */
  final def afterRead[U](f: T => U): BSONReader[B, U] =
    BSONReader[B, U]((read _) andThen f)

  private[reactivemongo] def widenReader[U >: T]: UnsafeBSONReader[U] =
    new UnsafeBSONReader[U] {
      def readTry(value: BSONValue): Try[U] =
        Try(value.asInstanceOf[B]) match {
          case Failure(_) => Failure(exceptions.TypeDoesNotMatch(
            s"Cannot convert $value: ${value.getClass} with ${self.getClass}"
          ))

          case Success(bson) => self.readTry(bson)
        }
    }
}

object BSONReader {
  private class Default[B <: BSONValue, T](
      _read: B => T
  ) extends BSONReader[B, T] {
    def read(bson: B): T = _read(bson)
  }

  def apply[B <: BSONValue, T](read: B => T): BSONReader[B, T] =
    new Default[B, T](read)
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

  /**
   * Returns a BSON writer that returns the result of applying `f`
   * on the BSON value from this writer.
   *
   * @param f the function to apply
   */
  final def afterWrite[U <: BSONValue](f: B => U): BSONWriter[T, U] =
    BSONWriter[T, U]((write _) andThen f)

  final def beforeWrite[U](f: U => T): BSONWriter[U, B] =
    BSONWriter[U, B](f andThen (write _))
}

object BSONWriter {
  private class Default[T, B <: BSONValue](
      _write: T => B
  ) extends BSONWriter[T, B] {
    def write(value: T): B = _write(value)
  }

  def apply[T, B <: BSONValue](write: T => B): BSONWriter[T, B] =
    new Default[T, B](write)
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

object BSONDocumentReader {
  private class Default[T](
      _read: BSONDocument => T
  ) extends BSONDocumentReader[T] {

    def read(value: BSONDocument): T = _read(value)
  }

  def apply[T](read: BSONDocument => T): BSONDocumentReader[T] =
    new Default[T](read)
}

trait BSONDocumentWriter[T] extends BSONWriter[T, BSONDocument]

object BSONDocumentWriter {
  private class Default[T](
      _write: T => BSONDocument
  ) extends BSONDocumentWriter[T] {

    def write(value: T): BSONDocument = _write(value)
  }

  def apply[T](write: T => BSONDocument): BSONDocumentWriter[T] =
    new Default[T](write)
}

trait VariantBSONDocumentReader[+T] extends VariantBSONReader[BSONDocument, T]
trait VariantBSONDocumentWriter[-T] extends VariantBSONWriter[T, BSONDocument]

class VariantBSONWriterWrapper[T, B <: BSONValue](writer: VariantBSONWriter[T, B]) extends BSONWriter[T, B] {
  def write(t: T) = writer.write(t)
}

class VariantBSONReaderWrapper[B <: BSONValue, T](reader: VariantBSONReader[B, T]) extends BSONReader[B, T] {
  def read(b: B) = reader.read(b)
}

trait BSONHandler[B <: BSONValue, T] extends BSONReader[B, T] with BSONWriter[T, B] {
  def as[R](to: T => R, from: R => T): BSONHandler[B, R] =
    new BSONHandler.MappedHandler(this, to, from)
}

object BSONHandler {
  private[bson] class MappedHandler[B <: BSONValue, T, U](
      parent: BSONHandler[B, T],
      to: T => U,
      from: U => T
  ) extends BSONHandler[B, U] {
    def write(u: U) = parent.write(from(u))
    def read(b: B) = to(parent.read(b))
  }

  private[bson] class DefaultHandler[B <: BSONValue, T](r: B => T, w: T => B)
      extends BSONHandler[B, T] {
    def read(x: B): T = r(x)
    def write(x: T): B = w(x)
  }

  /**
   * Handler factory.
   *
   * {{{
   * import reactivemongo.bson.{ BSONHandler, BSONString }
   *
   * case class Foo(value: String)
   *
   * val foo: BSONHandler[BSONString, Foo] = BSONHandler(
   *   { read: BSONString => Foo(read.value) },
   *   { write: Foo => BSONString(write.value)
   * )
   * }}}
   */
  def apply[B <: BSONValue, T](read: B => T, write: T => B): BSONHandler[B, T] =
    new DefaultHandler(read, write)

  /**
   * Returns a BSON handler for a type `T`, provided there are
   * a writer and a reader for it, both using the same kind of `BSONValue`.
   */
  implicit def provided[B <: BSONValue, T](implicit writer: BSONWriter[T, B], reader: BSONReader[B, T]): BSONHandler[B, T] = BSONHandler(reader.read _, writer.write _)
}

trait DefaultBSONHandlers {
  import scala.language.higherKinds

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

  implicit object BSONBinaryHandler extends BSONHandler[BSONBinary, Array[Byte]] {
    def read(bin: BSONBinary) = bin.value.duplicate().readArray(bin.value.size)
    def write(xs: Array[Byte]) = BSONBinary(xs, Subtype.GenericBinarySubtype)
  }

  import java.util.Date

  implicit object BSONDateTimeHandler extends BSONHandler[BSONDateTime, Date] {
    def read(bson: BSONDateTime) = new Date(bson.value)
    def write(date: Date) = BSONDateTime(date.getTime)
  }

  // Typeclasses Handlers
  import BSONNumberLike._
  import BSONBooleanLike._

  class BSONNumberLikeReader[B <: BSONValue] extends BSONReader[B, BSONNumberLike] {
    def read(bson: B) = bson match {
      case int: BSONInteger   => BSONIntegerNumberLike(int)
      case long: BSONLong     => BSONLongNumberLike(long)
      case double: BSONDouble => BSONDoubleNumberLike(double)
      case dt: BSONDateTime   => BSONDateTimeNumberLike(dt)
      case ts: BSONTimestamp  => BSONTimestampNumberLike(ts)
      case _                  => throw new UnsupportedOperationException()
    }
  }

  implicit object BSONNumberLikeWriter extends VariantBSONWriter[BSONNumberLike, BSONValue] {
    def write(number: BSONNumberLike) = number.underlying
  }

  implicit def bsonNumberLikeReader[B <: BSONValue] = new BSONNumberLikeReader[B]

  class BSONBooleanLikeReader[B <: BSONValue] extends BSONReader[B, BSONBooleanLike] {
    def read(bson: B) = bson match {
      case int: BSONInteger        => BSONIntegerBooleanLike(int)
      case double: BSONDouble      => BSONDoubleBooleanLike(double)
      case long: BSONLong          => BSONLongBooleanLike(long)
      case boolean: BSONBoolean    => BSONBooleanBooleanLike(boolean)
      case nll: BSONNull.type      => BSONNullBooleanLike(BSONNull)
      case udf: BSONUndefined.type => BSONUndefinedBooleanLike(BSONUndefined)
      case _                       => throw new UnsupportedOperationException()
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

  implicit def collectionToBSONArrayCollectionWriter[T, Repr <% Traversable[T]](implicit writer: BSONWriter[T, _ <: BSONValue]): VariantBSONWriter[Repr, BSONArray] = new BSONArrayCollectionWriter[T, Repr]

  implicit def bsonArrayToCollectionReader[M[_], T](implicit cbf: CanBuildFrom[M[_], T, M[T]], reader: BSONReader[_ <: BSONValue, T]): BSONReader[BSONArray, M[T]] = new BSONArrayCollectionReader

  abstract class IdentityBSONConverter[T <: BSONValue](implicit m: Manifest[T]) extends BSONReader[T, T] with BSONWriter[T, T] {
    override def write(t: T): T = m.runtimeClass.cast(t).asInstanceOf[T]
    override def writeOpt(t: T): Option[T] = if (m.runtimeClass.isInstance(t)) Some(t.asInstanceOf[T]) else None
    override def read(bson: T): T = m.runtimeClass.cast(bson).asInstanceOf[T]
    override def readOpt(bson: T): Option[T] = if (m.runtimeClass.isInstance(bson)) Some(bson.asInstanceOf[T]) else None
  }

  implicit object BSONStringIdentity extends IdentityBSONConverter[BSONString]

  implicit object BSONIntegerIdentity extends IdentityBSONConverter[BSONInteger]

  implicit object BSONArrayIdentity extends IdentityBSONConverter[BSONArray]

  implicit object BSONDocumentIdentity extends IdentityBSONConverter[BSONDocument] with BSONDocumentReader[BSONDocument] with BSONDocumentWriter[BSONDocument]

  implicit object BSONBooleanIdentity extends IdentityBSONConverter[BSONBoolean]

  implicit object BSONLongIdentity extends IdentityBSONConverter[BSONLong]

  implicit object BSONDoubleIdentity extends IdentityBSONConverter[BSONDouble]

  implicit object BSONValueIdentity extends IdentityBSONConverter[BSONValue]

  implicit object BSONObjectIDIdentity extends IdentityBSONConverter[BSONObjectID]

  implicit object BSONBinaryIdentity extends IdentityBSONConverter[BSONBinary]

  implicit object BSONDateTimeIdentity extends IdentityBSONConverter[BSONDateTime]

  implicit object BSONNullIdentity extends IdentityBSONConverter[BSONNull.type]

  implicit object BSONUndefinedIdentity extends IdentityBSONConverter[BSONUndefined.type]

  implicit object BSONRegexIdentity extends IdentityBSONConverter[BSONRegex]

  implicit object BSONJavaScriptIdentity extends BSONReader[BSONJavaScript, BSONJavaScript] with BSONWriter[BSONJavaScript, BSONJavaScript] {
    def read(b: BSONJavaScript) = b
    def write(b: BSONJavaScript) = b
  }

  implicit def findWriter[T](implicit writer: VariantBSONWriter[T, _ <: BSONValue]): BSONWriter[T, _ <: BSONValue] =
    new VariantBSONWriterWrapper(writer)

  implicit def findReader[T](implicit reader: VariantBSONReader[_ <: BSONValue, T]): BSONReader[_ <: BSONValue, T] =
    new VariantBSONReaderWrapper(reader)

  implicit def MapReader[K, V](implicit keyReader: BSONReader[BSONString, K], valueReader: BSONReader[_ <: BSONValue, V]): BSONDocumentReader[Map[K, V]] =
    new BSONDocumentReader[Map[K, V]] {
      def read(bson: BSONDocument): Map[K, V] = {
        val elements = bson.elements.map { element =>
          keyReader.read(BSONString(element.name)) -> element.value.seeAsTry[V].get
        }
        elements.toMap
      }
    }

  implicit def MapWriter[K, V](implicit keyWriter: BSONWriter[K, BSONString], valueWriter: BSONWriter[V, _ <: BSONValue]): BSONDocumentWriter[Map[K, V]] =
    new BSONDocumentWriter[Map[K, V]] {
      def write(inputMap: Map[K, V]): BSONDocument = {
        val elements = inputMap.map { tuple =>
          BSONElement(keyWriter.write(tuple._1).value, valueWriter.write(tuple._2))
        }
        BSONDocument(elements)
      }
    }
}

object DefaultBSONHandlers extends DefaultBSONHandlers
