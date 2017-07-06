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

import exceptions.DocumentKeyNotFound
import scala.util.{ Failure, Success, Try }
import buffer._
import utils.Converters

import scala.language.implicitConversions

sealed trait Producer[T] {
  @deprecated("Use [[apply]]", "0.12.0")
  private[bson] def produce: Option[T] = generate().headOption

  private[bson] def generate(): Traversable[T]
}

object Producer {
  private[bson] def apply[T](f: => Traversable[T]): Producer[T] =
    new Producer[T] { def generate() = f }

  case class NameOptionValueProducer(
      private val element: (String, Option[BSONValue])
  ) extends Producer[BSONElement] {
    private[bson] def generate() = element._2.map(value => BSONElement(element._1, value))
  }

  case class OptionValueProducer(
      private val element: Option[BSONValue]
  ) extends Producer[BSONValue] {
    private[bson] def generate() = element
  }

  @deprecated(
    "Replaced by [[element2Producer]] + [[BSONElement.converted]]",
    "0.12.0"
  )
  def nameValue2Producer[T](element: (String, T))(implicit writer: BSONWriter[T, _ <: BSONValue]) = NameOptionValueProducer(element._1 -> Some(writer.write(element._2)))

  implicit def element2Producer[E <% BSONElement](element: E): Producer[BSONElement] = {
    val e = implicitly[BSONElement](element)
    NameOptionValueProducer(e.name -> Some(e.value))
  }

  implicit def nameOptionValue2Producer[T](element: (String, Option[T]))(implicit writer: BSONWriter[T, _ <: BSONValue]): Producer[BSONElement] = NameOptionValueProducer(element._1 -> element._2.map(value => writer.write(value)))

  implicit def noneOptionValue2Producer(element: (String, None.type)): Producer[BSONElement] = NameOptionValueProducer(element._1 -> None)

  implicit def valueProducer[T](element: T)(implicit writer: BSONWriter[T, _ <: BSONValue]): Producer[BSONValue] = OptionValueProducer(Some(writer.write(element)))

  implicit def optionValueProducer[T](element: Option[T])(implicit writer: BSONWriter[T, _ <: BSONValue]): Producer[BSONValue] = OptionValueProducer(element.map(writer.write(_)))

  implicit val noneOptionValueProducer: None.type => Producer[BSONValue] =
    _ => OptionValueProducer(None)

}

sealed trait BSONValue {
  /**
   * The code indicating the BSON type for this value
   */
  val code: Byte
}

object BSONValue {
  import scala.util.Try
  import scala.reflect.ClassTag

  implicit class ExtendedBSONValue[B <: BSONValue](val bson: B) extends AnyVal {
    def asTry[T](implicit reader: BSONReader[B, T]): Try[T] = {
      reader.readTry(bson)
    }

    def asOpt[T](implicit reader: BSONReader[B, T]): Option[T] = asTry(reader).toOption
    def as[T](implicit reader: BSONReader[B, T]): T = asTry(reader).get

    def seeAsTry[T](implicit reader: BSONReader[_ <: BSONValue, T]): Try[T] =
      Try { reader.asInstanceOf[BSONReader[BSONValue, T]].readTry(bson) }.flatten

    def seeAsOpt[T](implicit reader: BSONReader[_ <: BSONValue, T]): Option[T] =
      seeAsTry[T].toOption
  }

  final def narrow[T <: BSONValue](v: BSONValue)(implicit tag: ClassTag[T]): Option[T] = tag.unapply(v)

  /**
   * An addition operation for [[BSONValue]],
   * so that it forms an additive semigroup with the BSON value kind.
   */
  object Addition extends ((BSONValue, BSONValue) => BSONArray) {
    def apply(x: BSONValue, y: BSONValue): BSONArray = (x, y) match {
      case (a @ BSONArray(_), b @ BSONArray(_)) => a.merge(b)
      case (a @ BSONArray(_), _)                => a.add(y)
      case (_, b @ BSONArray(_))                => x +: b
      case _                                    => BSONArray(List(x, y))
    }
  }
}

/** A BSON Double. */
case class BSONDouble(value: Double) extends BSONValue {
  val code = 0x01.toByte
}

case class BSONString(value: String) extends BSONValue {
  val code = 0x02.toByte
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
case class BSONArray(stream: Stream[Try[BSONValue]])
    extends BSONValue with BSONElementSet {

  val code = 0x04.toByte

  type SetType = BSONArray

  /**
   * Returns the [[BSONValue]] at the given `index`.
   *
   * If there is no such `index` or the matching value cannot be deserialized, returns `None`.
   */
  def get(index: Int): Option[BSONValue] = getTry(index).toOption

  /**
   * Returns the [[BSONValue]] matching the given `name`,
   * provided it is a valid string representation of a valid index.
   */
  def get(name: String): Option[BSONValue] = try {
    get(name.toInt)
  } catch {
    case _: Throwable => None
  }

  /** Returns true if the given `name` corresponds to a valid index. */
  def contains(name: String): Boolean = get(name).isDefined

  def headOption: Option[BSONElement] = stream.collectFirst {
    case Success(v) => BSONElement("0", v)
  }

  /**
   * Returns the [[BSONValue]] at the given `index`.
   *
   * If there is no such `index` or the matching value cannot be deserialized, returns a `Failure`.
   * The `Failure` holds a [[exceptions.DocumentKeyNotFound]] if the key could not be found.
   */
  def getTry(index: Int): Try[BSONValue] = stream.drop(index).headOption.
    getOrElse(Failure(DocumentKeyNotFound(index.toString)))

  /**
   * Returns the [[BSONValue]] at the given `index`.
   *
   * If there is no such `index`, the resulting option will be `None`.
   * If the matching value could not be deserialized, returns a `Failure`.
   */
  def getUnflattenedTry(index: Int): Try[Option[BSONValue]] = getTry(index) match {
    case Failure(_: DocumentKeyNotFound) => Success(None)
    case Failure(e)                      => Failure(e)
    case Success(e)                      => Success(Some(e))
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
   * Gets the [[BSONValue]] at the given `index`,
   * and converts it with the given implicit [[BSONReader]].
   *
   * If there is no matching value, or the value could not be deserialized or converted, returns a `Failure`.
   * The `Failure` holds a [[exceptions.DocumentKeyNotFound]] if the key could not be found.
   */
  def getAsTry[T](index: Int)(implicit reader: BSONReader[_ <: BSONValue, T]): Try[T] = {
    val tt = getTry(index)
    tt.flatMap { element => Try(reader.asInstanceOf[BSONReader[BSONValue, T]].read(element)) }
  }

  /**
   * Gets the [[BSONValue]] at the given `index`,
   * and converts it with the given implicit [[BSONReader]].
   *
   * If there is no matching value, returns a `Success` holding `None`.
   * If the value could not be deserialized or converted, returns a `Failure`.
   */
  def getAsUnflattenedTry[T](index: Int)(implicit reader: BSONReader[_ <: BSONValue, T]): Try[Option[T]] = getAsTry(index)(reader) match {
    case Failure(_: DocumentKeyNotFound) => Success(None)
    case Failure(e)                      => Failure(e)
    case Success(e)                      => Success(Some(e))
  }

  /** Creates a new [[BSONArray]] containing all the elements of this one and the elements of the given document. */
  def merge(doc: BSONArray): BSONArray = new BSONArray(stream ++ doc.stream)

  /** Creates a new [[BSONArray]] containing all the elements of this one and the given `elements`. */
  def merge(values: Producer[BSONValue]*): BSONArray =
    new BSONArray(stream ++ values.flatMap { v =>
      v.generate().map(value => Try(value))
    }.toStream)

  /**
   * Alias for `merge`.
   */
  @deprecated("Use the corresponding `merge`", "0.12.0")
  def add(doc: BSONArray): BSONArray = merge(doc)

  /**
   * Alias for `merge`.
   */
  def add(values: Producer[BSONValue]*): BSONArray = merge(values: _*)

  /** Returns a [[BSONArray]] with the given value prepended to its elements. */
  def prepend(value: Producer[BSONValue]): BSONArray =
    new BSONArray(value.generate().map(Try(_)) ++: stream)

  /** Alias for [[BSONArray.prepend]] */
  def +:(value: Producer[BSONValue]): BSONArray = prepend(value)

  /** Alias for the corresponding `merge` */
  @inline def ++(array: BSONArray): BSONArray = merge(array)

  /** Alias for `add` */
  def ++(values: Producer[BSONValue]*): BSONArray = merge(values: _*)

  @inline private def values(elements: Producer[BSONElement]) =
    Producer[BSONValue](elements.generate().map(_.value))

  /**
   * The name of the produced elements are ignored,
   * instead the indexes are used.
   */
  def :~(elements: Producer[BSONElement]*): BSONArray =
    this ++ (elements.map(values(_)): _*)

  def ~:(elements: Producer[BSONElement]): BSONArray =
    BSONArray(values(elements)) ++ this

  @deprecated("Use [[size]]", "0.12.0")
  def length = size

  @inline def size = stream.size

  @inline def isEmpty: Boolean = stream.isEmpty

  override def toString: String = s"BSONArray(<${if (isEmpty) "empty" else "non-empty"}>)"

  /**
   * Returns an iterator for the values as elements,
   * with their indexes as names (e.g. "0" for the first).
   */
  @deprecated(message = "Use [[elements]]", "0.12.0")
  def iterator: Iterator[Try[(String, BSONValue)]] =
    stream.zipWithIndex.map { vv =>
      vv._1.map(vv._2.toString -> _)
    }.toIterator

  def elements: List[BSONElement] = stream.zipWithIndex.collect {
    case (Success(v), index) => BSONElement(index.toString, v)
  }.toList

  private[bson] def generate() = elements

  def values: Stream[BSONValue] = stream.filter(_.isSuccess).map(_.get)
}

object BSONArray {
  /** Creates a new [[BSONArray]] containing all the given `elements`. */
  def apply(elements: Producer[BSONValue]*): BSONArray =
    new BSONArray(elements.flatMap {
      _.generate().map(value => Try(value))
    }.toStream)

  /** Creates a new [[BSONArray]] containing all the `elements` in the given `Traversable`. */
  def apply(elements: Traversable[BSONValue]): BSONArray = {
    new BSONArray(elements.toStream.map(Try(_)))
  }

  /** Returns a String representing the given [[BSONArray]]. */
  def pretty(array: BSONArray) =
    BSONIterator.pretty(array.elements.map(Try(_)).iterator)

  /** An empty BSONArray. */
  val empty: BSONArray = BSONArray()
}

/**
 * A BSON binary value.
 *
 * @param value The binary content.
 * @param subtype The type of the binary content.
 */
case class BSONBinary(value: ReadableBuffer, subtype: Subtype)
    extends BSONValue {

  val code = 0x05.toByte

  /** Returns the whole binary content as array. */
  def byteArray: Array[Byte] = value.duplicate().readArray(value.size)
}

object BSONBinary {
  def apply(value: Array[Byte], subtype: Subtype): BSONBinary =
    BSONBinary(ArrayReadableBuffer(value), subtype)
}

/** BSON Undefined value */
case object BSONUndefined
    extends BSONValue {
  val code = 0x06.toByte
}

/**
 * BSON ObjectId value.
 *
 * +------------------------+------------------------+------------------------+------------------------+
 * + timestamp (in seconds) +   machine identifier   +    thread identifier   +        increment       +
 * +        (4 bytes)       +        (3 bytes)       +        (2 bytes)       +        (3 bytes)       +
 * +------------------------+------------------------+------------------------+------------------------+
 */
@SerialVersionUID(239421902L)
class BSONObjectID private (private val raw: Array[Byte])
    extends BSONValue with Serializable with Equals {

  val code = 0x07.toByte

  import java.util.Arrays
  import java.nio.ByteBuffer

  /** ObjectId hexadecimal String representation */
  lazy val stringify = Converters.hex2Str(raw)

  override def toString = s"""BSONObjectID("${stringify}")"""

  override def canEqual(that: Any): Boolean = that.isInstanceOf[BSONObjectID]

  override def equals(that: Any): Boolean = that match {
    case BSONObjectID(other) => Arrays.equals(raw, other)
    case _                   => false
  }

  override lazy val hashCode: Int = Arrays.hashCode(raw)

  /** The time of this BSONObjectId, in milliseconds */
  def time: Long = this.timeSecond * 1000L

  /** The time of this BSONObjectId, in seconds */
  def timeSecond: Int = ByteBuffer.wrap(raw.take(4)).getInt

  def valueAsArray = Arrays.copyOf(raw, 12)
}

object BSONObjectID {
  private val maxCounterValue = 16777216
  private val increment = new java.util.concurrent.atomic.AtomicInteger(scala.util.Random.nextInt(maxCounterValue))

  private def counter = (increment.getAndIncrement + maxCounterValue) % maxCounterValue

  /**
   * The following implemtation of machineId work around openjdk limitations in
   * version 6 and 7
   *
   * Openjdk fails to parse /proc/net/if_inet6 correctly to determine macaddress
   * resulting in SocketException thrown.
   *
   * Please see:
   * * https://github.com/openjdk-mirror/jdk7u-jdk/blob/feeaec0647609a1e6266f902de426f1201f77c55/src/solaris/native/java/net/NetworkInterface.c#L1130
   * * http://lxr.free-electrons.com/source/net/ipv6/addrconf.c?v=3.11#L3442
   * * http://lxr.free-electrons.com/source/include/linux/netdevice.h?v=3.11#L1130
   * * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=7078386
   *
   * and fix in openjdk8:
   * * http://hg.openjdk.java.net/jdk8/tl/jdk/rev/b1814b3ea6d3
   */

  private val machineId = {
    import java.net._
    def p(n: String) = System.getProperty(n)
    val validPlatform = Try {
      val correctVersion = p("java.version").substring(0, 3).toFloat >= 1.8
      val noIpv6 = p("java.net.preferIPv4Stack").toBoolean == true
      val isLinux = p("os.name") == "Linux"

      !isLinux || correctVersion || noIpv6
    }.getOrElse(false)

    // Check java policies
    val permitted = {
      val sec = System.getSecurityManager();
      Try { sec.checkPermission(new NetPermission("getNetworkInformation")) }.toOption.map(_ => true).getOrElse(false);
    }

    if (validPlatform && permitted) {
      val networkInterfacesEnum = NetworkInterface.getNetworkInterfaces
      val networkInterfaces = scala.collection.JavaConverters.enumerationAsScalaIteratorConverter(networkInterfacesEnum).asScala
      val ha = networkInterfaces.find(ha => Try(ha.getHardwareAddress).isSuccess && ha.getHardwareAddress != null && ha.getHardwareAddress.length == 6)
        .map(_.getHardwareAddress)
        .getOrElse(InetAddress.getLocalHost.getHostName.getBytes("UTF-8"))
      Converters.md5(ha).take(3)
    } else {
      val threadId = Thread.currentThread.getId.toInt
      val arr = new Array[Byte](3)

      arr(0) = (threadId & 0xFF).toByte
      arr(1) = (threadId >> 8 & 0xFF).toByte
      arr(2) = (threadId >> 16 & 0xFF).toByte

      arr
    }
  }

  /**
   * Constructs a BSON ObjectId element from a hexadecimal String representation.
   * Throws an exception if the given argument is not a valid ObjectID.
   */
  @deprecated("`parse(str: String): Try[BSONObjectID]` should be considered instead of this method", "0.12.0")
  def apply(id: String): BSONObjectID = parse(id).get

  def apply(array: Array[Byte]): BSONObjectID = {
    if (array.length != 12)
      throw new IllegalArgumentException(s"wrong byte array for an ObjectId (size ${array.length})")
    new BSONObjectID(java.util.Arrays.copyOf(array, 12))
  }

  def unapply(id: BSONObjectID): Option[Array[Byte]] = Some(id.valueAsArray)

  /** Tries to make a BSON ObjectId from a hexadecimal string representation. */
  def parse(id: String): Try[BSONObjectID] = {
    if (id.length != 24) Failure[BSONObjectID](
      new IllegalArgumentException(s"Wrong ObjectId (length != 24): '$id'")
    )
    else Try(new BSONObjectID(Converters str2Hex id))
  }

  /**
   * Generates a new BSON ObjectID using the current time.
   *
   * @see [[fromTime]]
   */
  def generate(): BSONObjectID = fromTime(System.currentTimeMillis, false)

  /**
   * Generates a new BSON ObjectID from the given timestamp in milliseconds.
   *
   * The included timestamp is the number of seconds since epoch, so a BSONObjectID time part has only
   * a precision up to the second. To get a reasonably unique ID, you _must_ set `onlyTimestamp` to false.
   *
   * Crafting a BSONObjectID from a timestamp with `fillOnlyTimestamp` set to true is helpful for range queries,
   * eg if you want of find documents an _id field which timestamp part is greater than or lesser than
   * the one of another id.
   *
   * If you do not intend to use the produced BSONObjectID for range queries, then you'd rather use
   * the `generate` method instead.
   *
   * @param fillOnlyTimestamp if true, the returned BSONObjectID will only have the timestamp bytes set; the other will be set to zero.
   */
  def fromTime(timeMillis: Long, fillOnlyTimestamp: Boolean = true): BSONObjectID = {
    // n of seconds since epoch. Big endian
    val timestamp = (timeMillis / 1000).toInt
    val id = new Array[Byte](12)

    id(0) = (timestamp >>> 24).toByte
    id(1) = (timestamp >> 16 & 0xFF).toByte
    id(2) = (timestamp >> 8 & 0xFF).toByte
    id(3) = (timestamp & 0xFF).toByte

    if (!fillOnlyTimestamp) {
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
    }

    BSONObjectID(id)
  }
}

/** BSON boolean value */
case class BSONBoolean(value: Boolean)
    extends BSONValue {
  val code = 0x08.toByte

}

/** BSON date time value */
case class BSONDateTime(value: Long)
    extends BSONValue {
  val code = 0x09.toByte
}

/** BSON null value */
case object BSONNull extends BSONValue {
  val code = 0x0A.toByte
}

/**
 * BSON Regex value.
 *
 * @param flags Regex flags.
 */
case class BSONRegex(value: String, flags: String)
    extends BSONValue {
  val code = 0x0B.toByte
}

/** BSON DBPointer value. */
class BSONDBPointer private[bson] (
    val value: String,
    internalId: () => Array[Byte]
) extends BSONValue with Product with Serializable with java.io.Serializable {
  val code = 0x0C.toByte

  @deprecated("", "0.12.0")
  def this(value: String, id: Array[Byte]) = this(value, { () =>
    val idCopy = Array.ofDim[Byte](id.size)
    id.copyToArray(idCopy)
    idCopy
  })

  /** The BSONObjectID representation of this reference. */
  val objectId = BSONObjectID(internalId())

  @deprecated("Do not use", "0.12.0")
  def id: Array[Byte] = {
    val bytes = internalId()
    val idCopy = Array.ofDim[Byte](bytes.size)
    bytes.copyToArray(idCopy)
    idCopy
  }

  private[bson] def withId[T](f: Array[Byte] => T): T = f(internalId())

  // ---

  def canEqual(that: Any): Boolean = that.isInstanceOf[BSONDBPointer]

  @deprecated("No longer a case class", "0.12.0")
  val productArity = 2

  @deprecated("No longer a case class", "0.12.0")
  def productElement(n: Int): Any = n match {
    case 0 => value
    case 1 => id
  }

  @deprecated("No longer a case class", "0.12.0")
  def copy(value: String = this.value, id: Array[Byte] = internalId()): BSONDBPointer = BSONDBPointer(value, id)

  override def equals(that: Any): Boolean = {
    canEqual(that) && {
      val other = that.asInstanceOf[BSONDBPointer]
      this.value.equals(other.value) && (this.objectId == other.objectId)
    }
  }

  override def hashCode: Int = (value, objectId).hashCode
}

object BSONDBPointer extends scala.runtime.AbstractFunction2[String, Array[Byte], BSONDBPointer] {

  /** Returns a new DB pointer */
  @deprecated("", "0.12.0")
  def apply(value: String, id: Array[Byte]): BSONDBPointer = {
    val idCopy = Array.ofDim[Byte](id.size)
    id.copyToArray(idCopy)

    new BSONDBPointer(value, () => idCopy)
  }

  /** Returns a new DB pointer */
  def apply(value: String, id: => Array[Byte]): BSONDBPointer =
    new BSONDBPointer(value, () => id)

  /** Extractor */
  def unapply(pointer: BSONDBPointer): Option[(String, Array[Byte])] =
    pointer.withId { id => Some(pointer.value -> id) }
}

/**
 * BSON JavaScript value.
 *
 * @param value The JavaScript source code.
 */
case class BSONJavaScript(value: String) extends BSONValue {
  val code = 0x0D.toByte
}

/** BSON Symbol value. */
case class BSONSymbol(value: String) extends BSONValue {
  val code = 0x0E.toByte
}

/**
 * BSON scoped JavaScript value.
 *
 * @param value The JavaScript source code. TODO
 */
case class BSONJavaScriptWS(value: String)
    extends BSONValue {
  val code = 0x0F.toByte
}

/** BSON Integer value */
case class BSONInteger(value: Int) extends BSONValue {
  val code = 0x10.toByte
}

/** BSON Timestamp value */
case class BSONTimestamp(value: Long) extends BSONValue {
  val code = 0x11.toByte

  /** Seconds since the Unix epoch */
  val time = value >>> 32

  /** Ordinal (with the second) */
  val ordinal = value.toInt
}

/** Timestamp companion */
object BSONTimestamp {
  /**
   * Returns the timestamp corresponding to the given `time` and `ordinal`.
   *
   * @param time the 32bits time value (seconds since the Unix epoch)
   * @param ordinal an incrementing ordinal for operations within a same second
   */
  def apply(time: Long, ordinal: Int): BSONTimestamp =
    BSONTimestamp((time << 32) ^ ordinal)
}

/** BSON Long value */
case class BSONLong(value: Long) extends BSONValue {
  val code = 0x12.toByte
}

/** BSON Min key value */
object BSONMinKey extends BSONValue {
  val code = 0xFF.toByte
}

/** BSON Max key value */
object BSONMaxKey extends BSONValue {
  val code = 0x7F.toByte
}

/** Binary Subtype */
sealed trait Subtype {
  /** Subtype code */
  val value: Byte
}

object Subtype {
  case object GenericBinarySubtype extends Subtype { val value = 0x00.toByte }
  case object FunctionSubtype extends Subtype { val value = 0x01.toByte }
  case object OldBinarySubtype extends Subtype { val value = 0x02.toByte }
  case object OldUuidSubtype extends Subtype { val value = 0x03.toByte }
  case object UuidSubtype extends Subtype { val value = 0x04.toByte }
  case object Md5Subtype extends Subtype { val value = 0x05.toByte }
  case object UserDefinedSubtype extends Subtype { val value = 0x80.toByte }

  def apply(code: Byte) = code match {
    case 0    => GenericBinarySubtype
    case 1    => FunctionSubtype
    case 2    => OldBinarySubtype
    case 3    => OldUuidSubtype
    case 4    => UuidSubtype
    case 5    => Md5Subtype
    case -128 => UserDefinedSubtype
    case _    => throw new NoSuchElementException(s"binary type = $code")
  }
}

/**
 * Operations for a [[BSONElement]] that can contain multiple nested elements.
 *
 * @define keyParam the key to be found in the document
 */
sealed trait BSONElementSet extends ElementProducer { self: BSONValue =>
  type SetType <: BSONElementSet

  /** The first/mandatory nested element, if any */
  def headOption: Option[BSONElement]

  /** Returns the values for the nested elements. */
  def values: Traversable[BSONValue]

  /**
   * Returns a list for the values as [[BSONElement]]s,
   * with their indexes as names (e.g. "0" for the first).
   */
  def elements: Traversable[BSONElement]

  /** Returns a `Map` representation for this element set. */
  def toMap: Map[String, BSONValue] = elements.map {
    case BSONElement(name, value) => name -> value
  }.toMap

  /**
   * Checks whether the given key is found in this element set.
   *
   * @param key $keyParam
   * @return true if the key is found
   */
  def contains(key: String): Boolean

  /**
   * Returns the [[BSONValue]] associated with the given `key`.
   * If the key cannot be found, returns `None`.
   *
   * @param key $keyParam
   */
  def get(key: String): Option[BSONValue]

  /** Merge the produced elements at the beginning of this set */
  def ~:(elements: Producer[BSONElement]): SetType

  /** Merge the produced elements with this set */
  def :~(elements: Producer[BSONElement]*): SetType

  /** The number of elements */
  def size: Int

  /** Indicates whether this element set is empty */
  def isEmpty: Boolean
}

object BSONElementSet {
  def unapplySeq(that: BSONElementSet): Option[List[BSONElement]] =
    Some(that.elements.toList)

  /** Automatic conversion from elements collections to [[BSONElementSet]]. */
  implicit def apply(set: Traversable[BSONElement]): BSONElementSet =
    new BSONDocument(set.map(Success(_)).toStream)

  @deprecated("Use the appropriate factory", "0.12.0")
  implicit def legacy[T <: Product](input: Traversable[T]): BSONElementSet =
    new BSONDocument(input.collect {
      case (name: String, v: BSONValue) => Success(BSONElement(name, v))
    }.toStream)
}

/**
 * A `BSONDocument` structure (BSON type `0x03`).
 *
 * A `BSONDocument` is basically a stream of tuples `(String, BSONValue)`.
 * It is completely lazy. The stream it wraps is a `Stream[Try[(String, BSONValue)]]` since
 * we cannot be sure that a not yet deserialized value will be processed without error.
 *
 * @define keyParam the key to be found in the document
 */
case class BSONDocument(stream: Stream[Try[BSONElement]])
    extends BSONValue with BSONElementSet {

  val code = 0x03.toByte

  type SetType = BSONDocument

  def contains(key: String): Boolean = elements.exists(_.name == key)

  def get(key: String): Option[BSONValue] = elements.collectFirst {
    case BSONElement(`key`, value) => value
  }

  /**
   * Returns the [[BSONValue]] associated with the given `key`.
   *
   * If the key is not found or the matching value cannot be deserialized, returns a `Failure`.
   * The `Failure` holds a [[exceptions.DocumentKeyNotFound]] if the key could not be found.
   *
   * @param key $keyParam
   */
  def getTry(key: String): Try[BSONValue] = stream.collectFirst {
    case Success(BSONElement(k, cause)) if k == key => Success(cause)
    case Failure(e)                                 => Failure(e)
  }.getOrElse(Failure(DocumentKeyNotFound(key)))

  /**
   * Returns the [[BSONValue]] associated with the given `key`.
   *
   * If the key could not be found, the resulting option will be `None`.
   * If the matching value could not be deserialized, returns a `Failure`.
   *
   * @param key $keyParam
   */
  def getUnflattenedTry(key: String): Try[Option[BSONValue]] =
    getTry(key) match {
      case Failure(DocumentKeyNotFound(_)) => Success(None)
      case Failure(e)                      => Failure(e)
      case Success(e)                      => Success(Some(e))
    }

  /**
   * Returns the [[BSONValue]] associated with the given `key`, and converts it with the given implicit [[BSONReader]].
   *
   * If there is no matching value, or the value could not be deserialized or converted, returns a `None`.
   *
   * @param key $keyParam
   *
   * @note When implementing a [[http://reactivemongo.org/releases/latest/documentation/bson/typeclasses.html custom reader]], [[getAsTry]] must be preferred.
   */
  def getAs[T](key: String)(implicit reader: BSONReader[_ <: BSONValue, T]): Option[T] = get(key).flatMap { element =>
    reader match {
      case r: BSONReader[BSONValue, T] @unchecked => r.readOpt(element)
      case _                                      => None
    }
  }

  /**
   * Returns the [[BSONValue]] associated with the given `key`, and converts it with the given implicit [[BSONReader]].
   *
   * If there is no matching value, or the value could not be deserialized or converted, returns a `Failure`.
   * The `Failure` holds a [[exceptions.DocumentKeyNotFound]] if the key could not be found.
   *
   * @param key $keyParam
   */
  def getAsTry[T](key: String)(implicit reader: BSONReader[_ <: BSONValue, T]): Try[T] = {
    val tt = getTry(key)
    tt.flatMap { element => Try(reader.asInstanceOf[BSONReader[BSONValue, T]].read(element)) }
  }

  /**
   * Returns the [[BSONValue]] associated with the given `key`, and converts it with the given implicit [[BSONReader]].
   *
   * If there is no matching value, returns a `Success` holding `None`.
   * If the value could not be deserialized or converted, returns a `Failure`.
   */
  def getAsUnflattenedTry[T](key: String)(implicit reader: BSONReader[_ <: BSONValue, T]): Try[Option[T]] = getAsTry(key)(reader) match {
    case Failure(_: DocumentKeyNotFound) => Success(None)
    case Failure(e)                      => Failure(e)
    case Success(e)                      => Success(Some(e))
  }

  /** Creates a new [[BSONDocument]] containing all the elements of this one and the elements of the given document. */
  def merge(doc: BSONDocument): BSONDocument =
    new BSONDocument(stream ++ doc.stream)

  @deprecated("Use `merge`", "0.12.0")
  def add(doc: BSONDocument): BSONDocument = merge(doc)

  /** Creates a new [[BSONDocument]] containing all the elements of this one and the given `elements`. */
  def merge(elements: Producer[BSONElement]*): BSONDocument =
    new BSONDocument(stream ++ elements.flatMap(
      _.generate().map(value => Try(value))
    ).toStream)

  @deprecated("Use `merge`", "0.12.0")
  def add(elements: Producer[BSONElement]*): BSONDocument = merge(elements: _*)

  /** Creates a new [[BSONDocument]] without the elements corresponding the given `keys`. */
  def remove(keys: String*): BSONDocument = new BSONDocument(stream.filter {
    case Success(BSONElement(key, _)) if (
      keys contains key
    ) => false

    case _ => true
  })

  /** Alias for `add(doc: BSONDocument): BSONDocument` */
  def ++(doc: BSONDocument): BSONDocument = merge(doc)

  /** Alias for `:~` or `merge` */
  def ++(elements: Producer[BSONElement]*): BSONDocument = merge(elements: _*)

  def :~(elements: Producer[BSONElement]*): BSONDocument = merge(elements: _*)

  def ~:(elements: Producer[BSONElement]): BSONDocument =
    new BSONDocument(elements.generate().map(Success(_)) ++: stream)

  /** Alias for `remove(names: String*)` */
  def --(keys: String*): BSONDocument = remove(keys: _*)

  def headOption: Option[BSONElement] = stream.collectFirst {
    case Success(first) => first
  }

  /** Returns a `Stream` for all the elements of this `BSONDocument`. */
  lazy val elements: Stream[BSONElement] = stream.filter(_.isSuccess).map(_.get)

  private[bson] def generate() = elements

  def values: Stream[BSONValue] = stream.collect {
    case Success(BSONElement(_, value)) => value
  }

  @inline def isEmpty = stream.isEmpty

  @inline def size = stream.size

  override def toString: String = "BSONDocument(<" + (if (isEmpty) "empty" else "non-empty") + ">)"
}

object BSONDocument {
  /** Creates a [[BSONDocument]] from the given elements set. */
  def apply(set: BSONElementSet): BSONDocument = set match {
    case doc @ BSONDocument(_) => doc
    case _                     => BSONDocument.empty :~ set
  }

  /** Creates a new [[BSONDocument]] containing all the given `elements`. */
  def apply(elements: Producer[BSONElement]*): BSONDocument =
    new BSONDocument(elements.flatMap(
      _.generate().map(value => Try(value))
    ).toStream)

  /**
   * Creates a new [[BSONDocument]] containing all the `elements`
   * in the given `Traversable`.
   */
  def apply(elements: Traversable[(String, BSONValue)]): BSONDocument =
    new BSONDocument(elements.toStream.map {
      case (n, v) => Success(BSONElement(n, v))
    })

  /** Returns a String representing the given [[BSONDocument]]. */
  def pretty(doc: BSONDocument) = BSONIterator.pretty(doc.stream.iterator)

  /** Writes the `document` into the `buffer`. */
  def write(value: BSONDocument, buffer: WritableBuffer)(implicit bufferHandler: BufferHandler = DefaultBufferHandler): WritableBuffer =
    bufferHandler.writeDocument(value, buffer)

  /**
   * Reads a `document` from the `buffer`.
   *
   * Note that the buffer's readerIndex must be set on the start of a document, or it will fail.
   */
  def read(buffer: ReadableBuffer)(implicit bufferHandler: BufferHandler = DefaultBufferHandler): BSONDocument = bufferHandler.readDocument(buffer).get

  /** An empty BSONDocument. */
  val empty: BSONDocument = BSONDocument()
}

case class BSONElement(
    name: String,
    value: BSONValue
) extends ElementProducer {

  @deprecated("Use [[name]]", "0.12.0")
  @inline def _1 = name

  @deprecated("Use [[value]]", "0.12.0")
  @inline def _2 = value

  def generate() = List(this)
}

object BSONElement extends BSONElementLowPriority {
  implicit def provided(pair: (String, BSONValue)): BSONElement =
    BSONElement(pair._1, pair._2)

}

sealed trait BSONElementLowPriority {
  implicit def converted[T](pair: (String, T))(implicit w: BSONWriter[T, _ <: BSONValue]): BSONElement = BSONElement(pair._1, w.write(pair._2))

}

sealed trait ElementProducer extends Producer[BSONElement]

object ElementProducer {
  /**
   * An empty instance for the [[ElementProducer]] kind.
   * Can be used as `id` with the element [[Composition]] to form
   * an additive monoid.
   */
  case object Empty extends ElementProducer {
    def generate() = List.empty[BSONElement]
  }

  /**
   * A composition operation for [[ElementProducer]],
   * so that it forms an additive monoid with the [[Empty]] instance as `id`.
   */
  object Composition
      extends ((ElementProducer, ElementProducer) => ElementProducer) {

    def apply(x: ElementProducer, y: ElementProducer): ElementProducer =
      (x, y) match {
        case (Empty, Empty) => Empty
        case (Empty, _) => y
        case (_, Empty) => x
        case (a @ BSONElementSet(_), b @ BSONElementSet(_)) => a :~ b
        case (a @ BSONElementSet(_), _) => a :~ y
        case (_, b @ BSONElementSet(_)) => x ~: b
        case _ => BSONDocument(x, y)
      }
  }
}
