package reactivemongo.bson

import java.nio.ByteBuffer
import java.util.UUID

import scala.collection.generic.CanBuildFrom
import scala.util.Try

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

trait VariantBSONDocumentReader[+T] extends VariantBSONReader[BSONDocument, T]
trait VariantBSONDocumentWriter[-T] extends VariantBSONWriter[T, BSONDocument]

class VariantBSONWriterWrapper[T, B <: BSONValue](writer: VariantBSONWriter[T, B]) extends BSONWriter[T, B] {
  def write(t: T) = writer.write(t)
}

class VariantBSONReaderWrapper[B <: BSONValue, T](reader: VariantBSONReader[B, T]) extends BSONReader[B, T] {
  def read(b: B) = reader.read(b)
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

  implicit object BSONDecimalHandler
    extends BSONHandler[BSONDecimal, BigDecimal] {

    def read(decimal: BSONDecimal) = BSONDecimal.toBigDecimal(decimal).get
    def write(value: BigDecimal) = BSONDecimal.fromBigDecimal(value).get
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

  implicit object BSONUUID extends BSONHandler[BSONBinary, UUID] {
    def read(bson: BSONBinary): UUID =
      bson match {
        case BSONBinary(value, Subtype.OldUuidSubtype) =>
          val buf = ByteBuffer.wrap(value.readArray(value.size))
          new UUID(buf.getLong(), buf.getLong())
        case BSONBinary(_, _) => throw new UnsupportedOperationException()
      }

    def write(uuid: UUID): BSONBinary = {
      val buf = ByteBuffer.allocate(16)
      buf.putLong(uuid.getMostSignificantBits)
      buf.putLong(uuid.getLeastSignificantBits)
      BSONBinary(buf.array, Subtype.OldUuidSubtype)
    }
  }

  // Typeclasses Handlers
  import BSONNumberLike._
  import BSONBooleanLike._

  class BSONNumberLikeReader[B <: BSONValue]
    extends BSONReader[B, BSONNumberLike] {
    def read(bson: B): BSONNumberLike = bson match {
      case i: BSONInteger    => BSONIntegerNumberLike(i)
      case l: BSONLong       => BSONLongNumberLike(l)
      case d: BSONDouble     => BSONDoubleNumberLike(d)
      case dt: BSONDateTime  => BSONDateTimeNumberLike(dt)
      case ts: BSONTimestamp => BSONTimestampNumberLike(ts)
      case dec: BSONDecimal  => BSONDecimalNumberLike(dec)
      case _                 => throw new UnsupportedOperationException()
    }
  }

  implicit object BSONNumberLikeWriter extends VariantBSONWriter[BSONNumberLike, BSONValue] {
    def write(number: BSONNumberLike) = number.underlying
  }

  implicit def bsonNumberLikeReader[B <: BSONValue] =
    new BSONNumberLikeReader[B]

  class BSONBooleanLikeReader[B <: BSONValue]
    extends BSONReader[B, BSONBooleanLike] {
    def read(bson: B): BSONBooleanLike = bson match {
      case int: BSONInteger      => BSONIntegerBooleanLike(int)
      case double: BSONDouble    => BSONDoubleBooleanLike(double)
      case long: BSONLong        => BSONLongBooleanLike(long)
      case boolean: BSONBoolean  => BSONBooleanBooleanLike(boolean)
      case _: BSONNull.type      => BSONNullBooleanLike(BSONNull)
      case _: BSONUndefined.type => BSONUndefinedBooleanLike(BSONUndefined)
      case dec: BSONDecimal      => BSONDecimalBooleanLike(dec)
      case _                     => throw new UnsupportedOperationException()
    }
  }

  implicit object BSONBooleanLikeWriter extends VariantBSONWriter[BSONBooleanLike, BSONValue] {
    def write(number: BSONBooleanLike) = number.underlying
  }

  implicit def bsonBooleanLikeReader[B <: BSONValue] =
    new BSONBooleanLikeReader[B]

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

  implicit object BSONDecimalIdentity
    extends IdentityBSONConverter[BSONDecimal]

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
      def read(bson: BSONDocument): Map[K, V] =
        bson.elements.map { element =>
          keyReader.read(BSONString(element.name)) -> element.value.seeAsTry[V].get
        }(scala.collection.breakOut)
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

private[bson] final class BSONDocumentHandlerImpl[T](
  r: BSONDocument => T,
  w: T => BSONDocument) extends BSONDocumentReader[T]
  with BSONDocumentWriter[T] with BSONHandler[BSONDocument, T] {

  def read(doc: BSONDocument): T = r(doc)
  def write(value: T): BSONDocument = w(value)
}

object DefaultBSONHandlers extends DefaultBSONHandlers
