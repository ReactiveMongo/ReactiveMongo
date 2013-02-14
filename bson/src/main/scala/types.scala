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

import scala.util.{ Failure, Success, Try }
import scala.collection.generic.CanBuildFrom
import utils.Converters

/** A BSON Double. */
case class BSONDouble(value: Double) extends BSONValue { val code = 0x01 }

case class BSONString(value: String) extends BSONValue { val code = 0x02 }

class BSONDocument(val stream: Stream[Try[BSONElement]]) extends BSONValue {
  val code = 0x03

  def get(s: String): Option[BSONValue] = getTry(s).toOption.flatten

  def getTry(s: String): Try[Option[BSONValue]] = Try {
    stream.find {
      case Success(element) => element._1 == s
      case Failure(e) => throw e
    }.map(_.get._2)
  }

  def getFlattenedTry(s: String): Try[BSONValue] =
    getTry(s) flatMap { option =>
      Try(option.getOrElse(throw new NoSuchElementException(s)))
    }

  def getAs[T](s: String)(implicit reader: BSONReader[_ <: BSONValue, T]): Option[T] = {
    getTry(s).toOption.flatten.flatMap { element =>
      Try(reader.asInstanceOf[BSONReader[BSONValue, T]].read(element)).toOption
    }
  }

  def getAsTry[T](s: String)(implicit reader: BSONReader[_ <: BSONValue, T]): Try[Option[T]] = {
    val tt = getTry(s)
    tt.flatMap {
      case Some(element) => Try(Some(reader.asInstanceOf[BSONReader[BSONValue, T]].read(element)))
      case None => Success(None)
    }
  }

  def getAsFlattenedTry[T](s: String)(implicit reader: BSONReader[_ <: BSONValue, T]): Try[T] =
    getAsTry(s) flatMap { option =>
      Try(option.getOrElse(throw new NoSuchElementException(s)))
    }

  def ++(doc: BSONDocument): BSONDocument = new BSONDocument(stream ++ doc.stream)

  def add[T](el: (String, T))(implicit writer: BSONWriter[T, _ <: BSONValue]): BSONDocument = {
    new BSONDocument(stream :+ Try(el._1 -> writer.write(el._2)))
  }

  def elements: Stream[BSONElement] = stream.filter(_.isSuccess).map(_.get)
}

object BSONDocument {
  def apply(elements: Producer[(String, BSONValue)]*): BSONDocument = new BSONDocument(
    elements.flatMap { el =>
      el.produce.map(value => Seq(Try(value))).getOrElse(Seq.empty)
    }.toStream)

  def apply(elements: Stream[(String, BSONValue)]): BSONDocument = {
    new BSONDocument(elements.map(Success(_)))
  }

  def pretty(doc: BSONDocument) = BSONIterator.pretty(doc.stream.iterator)

  def write(value: BSONDocument, buffer: WritableBuffer)(implicit bufferHandler: BufferHandler = DefaultBufferHandler): WritableBuffer = {
    bufferHandler.writeDocument(value, buffer)
  }
  def read(buffer: ReadableBuffer)(implicit bufferHandler: BufferHandler = DefaultBufferHandler): BSONDocument = {
    bufferHandler.readDocument(buffer).get
  }
}

class BSONArray(val stream: Stream[Try[BSONValue]]) extends BSONValue {
  val code = 0x04

  def get(i: Int) = Try(stream.drop(i).head).flatten

  def getAs[T](i: Int)(implicit reader: BSONReader[_ <: BSONValue, T]) = {
    Try(reader.asInstanceOf[BSONReader[BSONValue, T]].read(get(i).get)).toOption
  }

  def iterator: Iterator[Try[(String, BSONValue)]] = stream.zipWithIndex.map { vv =>
    vv._1.map(vv._2.toString -> _)
  }.toIterator

  def values: Stream[BSONValue] = stream.filter(_.isSuccess).map(_.get)

  def ++(array: BSONArray): BSONArray = new BSONArray(stream ++ array.stream)
}

object BSONArray {
  def apply(elements: Producer[BSONValue]*): BSONArray = new BSONArray(
    elements.flatMap { el =>
      el.produce.map(value => Seq(Try(value))).getOrElse(Seq.empty)
    }.toStream)

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
  case object GenericBinarySubtype extends Subtype { override val value = 0x00 }
  case object FunctionSubtype extends Subtype { override val value = 0x01 }
  case object OldBinarySubtype extends Subtype { override val value = 0x02 }
  case object UuidSubtype extends Subtype { override val value = 0x03 }
  case object Md5Subtype extends Subtype { override val value = 0x05 }
  case object UserDefinedSubtype extends Subtype { override val value = 0x80 }
}