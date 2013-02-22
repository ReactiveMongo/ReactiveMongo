/*
 * Copyright 2013 Stephane Godbillon
 * @sgodbillon
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

import exceptions.DocumentKeyNotFound
import scala.util.{ Failure, Success, Try }
import scala.collection.generic.CanBuildFrom
import buffer._
import utils.Converters

/** A BSON Double. */
case class BSONDouble(value: Double) extends BSONValue { val code = 0x01 }

case class BSONString(value: String) extends BSONValue { val code = 0x02 }

/**
 * A `BSONDocument` structure (BSON type `0x03`).
 *
 * A `BSONDocument` is basically a stream of tuples `(String, BSONValue)`.
 * It is completely lazy. The stream it wraps is a `Stream[Try[(String, BSONValue)]]` since
 * we cannot be sure that a not yet deserialized value will be processed without error.
 */
class BSONDocument(val stream: Stream[Try[BSONElement]]) extends BSONValue {
  val code = 0x03

  /**
   * Returns the [[BSONValue]] associated with the given `key`.
   *
   * If the key is not found or the matching value cannot be deserialized, returns `None`.
   */
  def get(key: String): Option[BSONValue] = getTry(key).toOption

  /**
   * Returns the [[BSONValue]] associated with the given `key`.
   *
   * If the key is not found or the matching value cannot be deserialized, returns a `Failure`.
   * The `Failure` holds a [[exceptions.DocumentKeyNotFound]] if the key could not be found.
   */
  def getTry(key: String): Try[BSONValue] = Try {
    stream.find {
      case Success(element) => element._1 == key
      case Failure(e) => throw e
    }.map(_.get._2).getOrElse(throw DocumentKeyNotFound(key))
  }

  /**
   * Returns the [[BSONValue]] associated with the given `key`.
   *
   * If the key could not be found, the resulting option will be `None`.
   * If the matching value could not be deserialized, returns a `Failure`.
   */
  def getUnflattenedTry(key: String): Try[Option[BSONValue]] = getTry(key) match {
    case Failure(e: DocumentKeyNotFound) => Success(None)
    case Failure(e) => Failure(e)
    case Success(e) => Success(Some(e))
  }

  /**
   * Returns the [[BSONValue]] associated with the given `key`, and converts it with the given implicit [[BSONReader]].
   *
   * If there is no matching value, or the value could not be deserialized or converted, returns a `None`.
   */
  def getAs[T](s: String)(implicit reader: BSONReader[_ <: BSONValue, T]): Option[T] = {
    getTry(s).toOption.flatMap { element =>
      Try(reader.asInstanceOf[BSONReader[BSONValue, T]].read(element)).toOption
    }
  }

  /**
   * Returns the [[BSONValue]] associated with the given `key`, and converts it with the given implicit [[BSONReader]].
   *
   * If there is no matching value, or the value could not be deserialized or converted, returns a `Failure`.
   * The `Failure` holds a [[exceptions.DocumentKeyNotFound]] if the key could not be found.
   */
  def getAsTry[T](s: String)(implicit reader: BSONReader[_ <: BSONValue, T]): Try[T] = {
    val tt = getTry(s)
    tt.flatMap { element => Try(reader.asInstanceOf[BSONReader[BSONValue, T]].read(element)) }
  }

  /**
   * Returns the [[BSONValue]] associated with the given `key`, and converts it with the given implicit [[BSONReader]].
   *
   * If there is no matching value, returns a `Success` holding `None`.
   * If the value could not be deserialized or converted, returns a `Failure`.
   */
  def getAsUnflattenedTry[T](s: String)(implicit reader: BSONReader[_ <: BSONValue, T]): Try[Option[T]] = getAsTry(s) match {
    case Failure(e: DocumentKeyNotFound) => Success(None)
    case Failure(e) => Failure(e)
    case Success(e) => Success(Some(e))
  }

  /** Creates a new [[BSONDocument]] containing all the elements of this one and the elements of the given document. */
  def add(doc: BSONDocument): BSONDocument = new BSONDocument(stream ++ doc.stream)

  /** Creates a new [[BSONDocument]] containing all the elements of this one and the given `elements`. */
  def add(elements: Producer[(String, BSONValue)]*): BSONDocument = new BSONDocument(
    stream ++ elements.flatMap { el =>
      el.produce.map(value => Seq(Try(value))).getOrElse(Seq.empty)
    }.toStream)

  /** Alias for `add(doc: BSONDocument): BSONDocument` */
  def ++(doc: BSONDocument): BSONDocument = add(doc)

  /** Alias for `add(elements: Producer[(String, BSONValue)]*): BSONDocument` */
  def ++(elements: Producer[(String, BSONValue)]*): BSONDocument = add(elements: _*)

  /** Returns a `Stream` for all the elements of this `BSONDocument`. */
  def elements: Stream[BSONElement] = stream.filter(_.isSuccess).map(_.get)
}

object BSONDocument {
  /** Creates a new [[BSONDocument]] containing all the given `elements`. */
  def apply(elements: Producer[(String, BSONValue)]*): BSONDocument = new BSONDocument(
    elements.flatMap { el =>
      el.produce.map(value => Seq(Try(value))).getOrElse(Seq.empty)
    }.toStream)

  /** Creates a new [[BSONDocument]] containing all the `elements` in the given `Stream`. */
  def apply(elements: Stream[(String, BSONValue)]): BSONDocument = {
    new BSONDocument(elements.map(Success(_)))
  }

  /** Returns a String representing the given [[BSONDocument]]. */
  def pretty(doc: BSONDocument) = BSONIterator.pretty(doc.stream.iterator)

  /** Writes the `document` into the `buffer`. */
  def write(value: BSONDocument, buffer: WritableBuffer)(implicit bufferHandler: BufferHandler = DefaultBufferHandler): WritableBuffer = {
    bufferHandler.writeDocument(value, buffer)
  }
  /**
   * Reads a `document` from the `buffer`.
   *
   * Note that the buffer's readerIndex must be set on the start of a document, or it will fail.
   */
  def read(buffer: ReadableBuffer)(implicit bufferHandler: BufferHandler = DefaultBufferHandler): BSONDocument = {
    bufferHandler.readDocument(buffer).get
  }
}

/**
 * A `BSONArray` structure (BSON type `0x04`).
 *
 * A `BSONArray` is a straightforward `BSONDocument` where keys are a sequence of positive integers.
 *
 * A `BSONArray` is basically a stream of tuples `(String, BSONValue)` where the first member is a string representation of an index.
 * It is completely lazy. The stream it wraps is a `Stream[Try[(String, BSONValue)]]` since
 * we cannot be sure that a not yet deserialized value will be processed without error.
 */
class BSONArray(val stream: Stream[Try[BSONValue]]) extends BSONValue {
  val code = 0x04

  /**
   * Returns the [[BSONValue]] at the given `index`.
   *
   * If there is no such `index` or the matching value cannot be deserialized, returns `None`.
   */
  def get(index: Int): Option[BSONValue] = getTry(index).toOption

  /**
   * Returns the [[BSONValue]] at the given `index`.
   *
   * If there is no such `index` or the matching value cannot be deserialized, returns a `Failure`.
   * The `Failure` holds a [[exceptions.DocumentKeyNotFound]] if the key could not be found.
   */
  def getTry(index: Int): Try[BSONValue] = stream.drop(index).headOption.getOrElse(Failure(DocumentKeyNotFound(index.toString)))

  /**
   * Returns the [[BSONValue]] at the given `index`.
   *
   * If there is no such `index`, the resulting option will be `None`.
   * If the matching value could not be deserialized, returns a `Failure`.
   */
  def getUnflattenedTry(index: Int): Try[Option[BSONValue]] = getTry(index) match {
    case Failure(e: DocumentKeyNotFound) => Success(None)
    case Failure(e) => Failure(e)
    case Success(e) => Success(Some(e))
  }

  /**
   * Gets the [[BSONValue]] at the given `index`, and converts it with the given implicit [[BSONReader]].
   *
   * If there is no matching value, or the value could not be deserialized or converted, returns a `None`.
   */
  def getAs[T](index: Int)(implicit reader: BSONReader[_ <: BSONValue, T]): Option[T] = {
    getTry(index).toOption.flatMap { element =>
      Try(reader.asInstanceOf[BSONReader[BSONValue, T]].read(element)).toOption
    }
  }

  /**
   * Gets the [[BSONValue]] at the given `index`, and converts it with the given implicit [[BSONReader]].
   *
   * If there is no matching value, or the value could not be deserialized or converted, returns a `Failure`.
   * The `Failure` holds a [[exceptions.DocumentKeyNotFound]] if the key could not be found.
   */
  def getAsTry[T](index: Int)(implicit reader: BSONReader[_ <: BSONValue, T]): Try[T] = {
    val tt = getTry(index)
    tt.flatMap { element => Try(reader.asInstanceOf[BSONReader[BSONValue, T]].read(element)) }
  }

  /**
   * Gets the [[BSONValue]] at the given `index`, and converts it with the given implicit [[BSONReader]].
   *
   * If there is no matching value, returns a `Success` holding `None`.
   * If the value could not be deserialized or converted, returns a `Failure`.
   */
  def getAsUnflattenedTry[T](index: Int)(implicit reader: BSONReader[_ <: BSONValue, T]): Try[Option[T]] = getAsTry(index) match {
    case Failure(e: DocumentKeyNotFound) => Success(None)
    case Failure(e) => Failure(e)
    case Success(e) => Success(Some(e))
  }

  /** Creates a new [[BSONDocument]] containing all the elements of this one and the elements of the given document. */
  def add(doc: BSONArray): BSONArray = new BSONArray(stream ++ doc.stream)

  /** Creates a new [[BSONDocument]] containing all the elements of this one and the given `elements`. */
  def add(elements: Producer[BSONValue]*): BSONArray = new BSONArray(
    stream ++ elements.flatMap { el =>
      el.produce.map(value => Seq(Try(value))).getOrElse(Seq.empty)
    }.toStream)

  /** Alias for `add(doc: BSONDocument): BSONDocument` */
  def ++(array: BSONArray): BSONArray = add(array)

  /** Alias for `add(elements: Producer[(String, BSONValue)]*): BSONDocument` */
  def ++(elements: Producer[BSONValue]*): BSONArray = add(elements: _*)

  def iterator: Iterator[Try[(String, BSONValue)]] = stream.zipWithIndex.map { vv =>
    vv._1.map(vv._2.toString -> _)
  }.toIterator

  def values: Stream[BSONValue] = stream.filter(_.isSuccess).map(_.get)

  lazy val length = stream.size
}

object BSONArray {
  /** Creates a new [[BSONArray]] containing all the given `elements`. */
  def apply(elements: Producer[BSONValue]*): BSONArray = new BSONArray(
    elements.flatMap { el =>
      el.produce.map(value => Seq(Try(value))).getOrElse(Seq.empty)
    }.toStream)

  /** Creates a new [[BSONValue]] containing all the `elements` in the given `Stream`. */
  def apply(elements: Stream[BSONValue]): BSONArray = {
    new BSONArray(elements.map(Success(_)))
  }

  /** Returns a String representing the given [[BSONArray]]. */
  def pretty(array: BSONArray) = BSONIterator.pretty(array.iterator)
}

/**
 * A BSON binary value.
 *
 * @param value The binary content.
 * @param subtype The type of the binary content.
 */
case class BSONBinary(value: ReadableBuffer, subtype: Subtype) extends BSONValue { val code = 0x05 } // TODO

object BSONBinary {
  def apply(value: Array[Byte], subtype: Subtype): BSONBinary =
    BSONBinary(ArrayReadableBuffer(value), subtype)
}

/** BSON Undefined value */
case object BSONUndefined extends BSONValue { val code = 0x06 }

/** BSON ObjectId value. */
case class BSONObjectID(value: Array[Byte]) extends BSONValue {
  val code = 0x07

  import java.util.Arrays
  import java.nio.ByteBuffer

  /** Constructs a BSON ObjectId element from a hexadecimal String representation */
  def this(value: String) = this(Converters.str2Hex(value))

  /** ObjectId hexadecimal String representation */
  lazy val stringify = Converters.hex2Str(value)

  override def toString = "BSONObjectID(\"" + stringify + "\")"

  override def equals(obj: Any): Boolean = obj match {
    case BSONObjectID(arr) => Arrays.equals(value, arr)
    case _ => false
  }

  override lazy val hashCode: Int = Arrays.hashCode(value)

  /** The time of this BSONObjectId, in milliseconds */
  def time: Long = this.timeSecond * 1000L

  /** The time of this BSONObjectId, in seconds */
  def timeSecond: Int = ByteBuffer.wrap(this.value.take(4)).getInt
}

object BSONObjectID {
  import java.net._
  private val maxCounterValue = 16777216
  private val increment = new java.util.concurrent.atomic.AtomicInteger(scala.util.Random.nextInt(maxCounterValue))

  private def counter = (increment.getAndIncrement + maxCounterValue) % maxCounterValue

  private val machineId = {
    val networkInterfacesEnum = NetworkInterface.getNetworkInterfaces
    val networkInterfaces = scala.collection.JavaConverters.enumerationAsScalaIteratorConverter(networkInterfacesEnum).asScala
    val ha = networkInterfaces.find(ha => ha.getHardwareAddress != null && ha.getHardwareAddress.length == 6)
      .map(_.getHardwareAddress)
      .getOrElse(InetAddress.getLocalHost.getHostName.getBytes)
    Converters.md5(ha).take(3)
  }

  /** Constructs a BSON ObjectId element from a hexadecimal String representation */
  def apply(id: String): BSONObjectID = new BSONObjectID(id)

  /** Generates a new BSON ObjectID. */
  def generate: BSONObjectID = {
    val timestamp = (System.currentTimeMillis / 1000).toInt

    // n of seconds since epoch. Big endian
    val id = new Array[Byte](12)
    id(0) = (timestamp >>> 24).toByte
    id(1) = (timestamp >> 16 & 0xFF).toByte
    id(2) = (timestamp >> 8 & 0xFF).toByte
    id(3) = (timestamp & 0xFF).toByte

    // machine id, 3 first bytes of md5(macadress or hostname)
    id(4) = machineId(0)
    id(5) = machineId(1)
    id(6) = machineId(2)

    // 2 bytes of the pid or thread id. Thread id in our case. Low endian
    val threadId = Thread.currentThread.getId.toInt
    id(7) = (threadId & 0xFF).toByte
    id(8) = (threadId >> 8 & 0xFF).toByte

    // 3 bytes of counter sequence, which start is randomized. Big endian
    val c = counter
    id(9) = (c >> 16 & 0xFF).toByte
    id(10) = (c >> 8 & 0xFF).toByte
    id(11) = (c & 0xFF).toByte

    BSONObjectID(id)
  }
}

/** BSON boolean value */
case class BSONBoolean(value: Boolean) extends BSONValue { val code = 0x08 }

/** BSON date time value */
case class BSONDateTime(value: Long) extends BSONValue { val code = 0x09 }

/** BSON null value */
case object BSONNull extends BSONValue { val code = 0x0A }

/**
 * BSON Regex value.
 *
 * @param flags Regex flags.
 */
case class BSONRegex(value: String, flags: String) extends BSONValue { val code = 0x0B }

/** BSON DBPointer value. TODO */
case class BSONDBPointer(value: String, id: Array[Byte]) extends BSONValue { val code = 0x0C }

/**
 * BSON JavaScript value.
 *
 * @param value The JavaScript source code.
 */
case class BSONJavaScript(value: String) extends BSONValue { val code = 0x0D }

/** BSON Symbol value. */
case class BSONSymbol(value: String) extends BSONValue { val code = 0x0E }

/**
 * BSON scoped JavaScript value.
 *
 * @param value The JavaScript source code. TODO
 */
case class BSONJavaScriptWS(value: String) extends BSONValue { val code = 0x0F }

/** BSON Integer value */
case class BSONInteger(value: Int) extends BSONValue { val code = 0x10 }

/** BSON Timestamp value. TODO */
case class BSONTimestamp(value: Long) extends BSONValue { val code = 0x11 }

/** BSON Long value */
case class BSONLong(value: Long) extends BSONValue { val code = 0x12 }

/** BSON Min key value */
object BSONMinKey extends BSONValue { val code = 0xFF }

/** BSON Max key value */
object BSONMaxKey extends BSONValue { val code = 0x7F }

/** Binary Subtype */
sealed trait Subtype {
  /** Subtype code */
  val value: Int
}

object Subtype {
  case object GenericBinarySubtype extends Subtype { val value = 0x00 }
  case object FunctionSubtype extends Subtype { val value = 0x01 }
  case object OldBinarySubtype extends Subtype { val value = 0x02 }
  case object UuidSubtype extends Subtype { val value = 0x03 }
  case object Md5Subtype extends Subtype { val value = 0x05 }
  case object UserDefinedSubtype extends Subtype { val value = 0x80 }
  def apply(code: Int) = code match {
    case 0x00 => GenericBinarySubtype
    case 0x01 => FunctionSubtype
    case 0x02 => OldBinarySubtype
    case 0x03 => UuidSubtype
    case 0x05 => Md5Subtype
    case 0x80 => UserDefinedSubtype
    case _ => throw new NoSuchElementException(s"binary type = $code")
  }
}