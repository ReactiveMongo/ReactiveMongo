package reactivemongo.bson

import java.nio.ByteOrder
import java.nio.ByteBuffer
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import reactivemongo.utils.Converters
import reactivemongo.utils.buffers._

/*
element  ::=  "\x01" e_name double  Floating point
|  "\x02" e_name string  UTF-8 string
|  "\x03" e_name document  Embedded document
|  "\x04" e_name document  Array
|  "\x05" e_name binary  Binary data
|  "\x06" e_name  Undefined — Deprecated
|  "\x07" e_name (byte*12)  ObjectId
|  "\x08" e_name "\x00"  Boolean "false"
|  "\x08" e_name "\x01"  Boolean "true"
|  "\x09" e_name int64  UTC datetime
|  "\x0A" e_name  Null value
|  "\x0B" e_name cstring cstring  Regular expression
|  "\x0C" e_name string (byte*12)  DBPointer — Deprecated
|  "\x0D" e_name string  JavaScript code
|  "\x0E" e_name string  Symbol
|  "\x0F" e_name code_w_s  JavaScript code w/ scope
|  "\x10" e_name int32  32-bit Integer
|  "\x11" e_name int64  Timestamp
|  "\x12" e_name int64  64-bit integer
|  "\xFF" e_name  Min key
|  "\x7F" e_name  Max key
*/
/**
 * A BSON pair (name, bsonvalue).
 */
sealed trait BSONElement {
  val name: String
  val value: BSONValue

  /** Writes this element to the given [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] */
  final def write(buffer: ChannelBuffer) :ChannelBuffer = {
    buffer writeByte value.code
    buffer writeCString name
    value write buffer
  }
}

// TODO eager/lazy
case class DefaultBSONElement(name: String, value: BSONValue) extends BSONElement
case class ReadBSONElement(name: String, value: BSONValue) extends BSONElement

sealed trait BSONValueLike

sealed trait BSONBooleanLike extends BSONValueLike {
  def toBoolean :Boolean
}

sealed trait BSONNumberLike extends BSONBooleanLike {
  def toInt :Int
  def toLong :Long
  def toDouble :Double

  def toBoolean = toDouble != 0
}

/** A BSON Value type */
sealed trait BSONValue extends BSONValueLike {
  /** bson type code */
  val code: Int
  /** Writes this value in the given [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] */
  def write(buffer: ChannelBuffer) :ChannelBuffer
  /** Returns a new buffer containing this value. */
  def toBuffer :ChannelBuffer = write(ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 256))
}

/** A BSON Double. */
case class BSONDouble(value: Double) extends BSONValue with BSONNumberLike {
  val code = 0x01

  def write(buffer: ChannelBuffer) = { buffer writeDouble value; buffer }

  lazy val toInt = value.toInt
  lazy val toLong = value.toLong
  lazy val toDouble = value
}

/** A BSON String */
case class BSONString(value: String) extends BSONValue {
  val code = 0x02

  def write(buffer: ChannelBuffer) = { buffer writeString value }
}


// BSON Structure handlers ----------------------------------->
/**
 * A BSON Structure (a BSON array or document).
 */
sealed trait BSONStructure extends BSONValue {
  /**
   * Writes the content of this structure into the given ChannelBuffer.
   * If this is an appendable structure, its underlying buffer will be copied, and ended.
   * The underlying buffer is not affected, so this instance can be used again.
   */
  def write(buffer: ChannelBuffer) = { buffer.writeBytes(makeBuffer); buffer }

  /**
   * If this structure is traversable, copies the underlying buffer.
   *
   * Otherwise, if this structure is appendable, copies the underlying buffer, ends the Bson, sets the length and returns the copied buffer.
   * The underlying buffer is not affected, so this instance can be used again.
   */
  def makeBuffer :ChannelBuffer


  type Appendable <: AppendableBSONStructure[_]

  type Traversable <: TraversableBSONStructure[_]

  /**
   * Returns an appendable structure from this structure.
   * If this structure is already appendable, it may return itself.
   */
  def toAppendable :Appendable

  /**
   * Returns a traversable structure from this structure.
   * If this structure is already traversable, it may return itself.
   */
  def toTraversable :Traversable

  /** An iterator over the BSONElements this structure contains. */
  def iterator :Iterator[BSONElement]

  /** An iterator over the values this structure contains. */
  def values :Iterator[BSONValue] = iterator.map(_.value)
}

/** A BSON Document structure. */
sealed trait BSONDocument extends BSONStructure {
  val code = 0x03

  type Appendable = AppendableBSONDocument

  type Traversable = TraversableBSONDocument

  /** Makes a map of the values indexed by their names. */
  def mapped :Map[String, BSONValue] = {
    for(el <- iterator) yield (el.name, el.value)
  }.toMap
}
/** A BSON Array structure. */
sealed trait BSONArray extends BSONStructure {
  val code = 0x04

  type Appendable = AppendableBSONArray

  type Traversable = TraversableBSONArray

  /** Makes a map of the values by their index. */
  def mapped :Map[Int, BSONValue] = {
    for(el <- iterator) yield (el.name.toInt, el.value)
  }.toMap
}

/**
 * A structure builder. It will accept elements of `E` and write them into its underlying buffer.
 *
 * @tparam E The type of the elements that can be appended to this structure.
 */
sealed trait AppendableBSONStructure[E] extends BSONStructure {
  type Self <: AppendableBSONStructure[E]

  def elements :Seq[BSONElement]

  /**
   * Appends the given elements to this structure.
   */
  def append(e: E*) :Self

  /**
   * Alias for append(e: E*): appends the given elements to this structure.
   */
  def ++ (e: E*) :Self = append(e:_*)

  /** Appends the given option of elements (produced by an `Implicits.Producer[E]`), if they are defined. */
  def append(producer: Implicits.Producer[E], producers: Implicits.Producer[E]*) :Self = {
    append( (for(e <- (producer +: producers) if e.produce.isDefined) yield e.produce.get) :_*)
  }

  /** Alias for `append(producers: Implicits.Producer[E]*)`: appends the given option of elements (produced by an `Implicits.Producer[E]`), if they are defined. */
  def ++ (producer: Implicits.Producer[E], producers: Implicits.Producer[E]*) :Self = append(producer, producers :_*)

  /** Appends another structure to this one. */
  def append(other: BSONStructure) :Self

  /** Alias for `append(other: BSONStructure)`: Appends another structure to this one. */
  def ++ (other: BSONStructure) :Self = append(other)

  def iterator :Iterator[BSONElement] = elements.iterator

  def makeBuffer = {
    val result = ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 32)
    result.writeInt(0)
    for(el <- iterator)
      el.write(result)
    result.writeByte(0)
    result.setInt(0, result.writerIndex)
    result
  }

}

/**
 * A structure reader. It will give the value matching the given key of type `Key`, if it exists.
 *
 * This reader is lazy: it deserializes values only when required.
 * Moreover, it memoizes the already deserialized values, to avoid n computations.
 *
 * @tparam Key The type of the keys of this structure.
 */
sealed trait TraversableBSONStructure[Key] extends BSONStructure {
  private[bson] val buffer :ChannelBuffer
  private val rdx = buffer.readerIndex
  protected val stream = DefaultBSONIterator(buffer).toStream

  /** Gets the value matching the given key, if it exists. */
  def get(key: Key) :Option[BSONValue]

  /**
   * Gets the value matching the given key, if it exists and if it is of type `T`.
   *
   * @tparam T the type of the BSONValue to find.
   */
  def getAs[T <: BSONValueLike :Manifest](key: Key) :Option[T] = {
    val m = manifest[T]
    get(key).flatMap { e =>
      if(scala.reflect.ClassManifest.fromClass(e.getClass) <:<  m)
        Some(e.asInstanceOf[T])
      else None
    }
  }

  def makeBuffer = {
    val pos = buffer.readerIndex
    val result = buffer.copy(rdx, buffer.writerIndex)
    buffer.readerIndex(pos)
    result
  }

  /**
   * An iterator of the elements that are present in this structure.
   *
   * This iterator is produced from a stream that memoizes the already computed values (to avoid unnecessary computation).
   */
  def iterator :Iterator[BSONElement]= stream.iterator

  /** A map containing all the values of this structure. */
  def mapped :Map[Key, BSONValue]
}

/**
 * An array reader. It will give the value matching the index, if it exists.
 *
 * This reader is lazy: it deserializes values only when required.
 * Moreover, it memoizes the already deserialized values, to avoid n computations.
 */
class TraversableBSONArray(private[bson] val buffer: ChannelBuffer) extends TraversableBSONStructure[Int] with BSONArray {
  def get(i :Int) = {
    val iterator = stream.iterator
    iterator.find(_.name.toInt == i).map(_.value)
  }

  def toAppendable = new AppendableBSONArray(iterator.toVector)

  def toTraversable = this

  /**
   * Makes a list containing all the value of this array (ascending order).
   */
  def toList: List[BSONValue] = iterator.map(_.value).toList
  def toSeq: Seq[BSONValue] = toList
}

/**
 * A BSON Array builder. It will append the given values to this array.
 */
class AppendableBSONArray private[bson](val elements: Vector[BSONElement]) extends AppendableBSONStructure[BSONValue] with BSONArray {
  type Self = AppendableBSONArray

  def append(values: BSONValue*) :Self = {
    new AppendableBSONArray(elements ++ (for(
      (value, i) <- values.zipWithIndex
    ) yield DefaultBSONElement((elements.length + i).toString, value)))
  }

  def append(other: BSONStructure) :Self = append(other.iterator.map(_.value).toList :_*)

  def toAppendable = this

  def toTraversable = new TraversableBSONArray(makeBuffer)
}

/**
 * A document reader. It will give the value matching the given name, if it exists.
 *
 * This reader is lazy: it deserializes values only when required.
 * Moreover, it memoizes the already deserialized values, to avoid n computations.
 */
class TraversableBSONDocument(private[bson] val buffer: ChannelBuffer) extends TraversableBSONStructure[String] with BSONDocument {
  def get(name: String) = {
    val iterator = stream.iterator
    iterator.find(_.name == name).map(_.value)
  }

  def toAppendable = new AppendableBSONDocument(iterator.toList)

  def toTraversable = this
}

/**
 * A BSON Document builder. It will append the given values to this array.
 */
class AppendableBSONDocument private[bson](val elements: Seq[BSONElement]) extends AppendableBSONStructure[(String, BSONValue)] with BSONDocument {
  type Self = AppendableBSONDocument

  def append( els: (String, BSONValue)* ) :Self = {
    new AppendableBSONDocument(elements ++ els.map(el => DefaultBSONElement(el._1, el._2)))
  }

  def append(other: BSONStructure) = new AppendableBSONDocument(elements ++ other.iterator)

  def toAppendable = this

  def toTraversable = new TraversableBSONDocument(makeBuffer)
}

object BSONDocument {
  /** Makes an [[reactivemongo.bson.AppendableBSONDocument]] containing the given values. */
  def apply( els: (String, BSONValue)* ) :AppendableBSONDocument = new AppendableBSONDocument(List()).append(els :_*)
  /** Makes an [[reactivemongo.bson.AppendableBSONDocument]] containing the given values (produced by an `Implicits.Producer[E]`), if they are defined. */
  def apply( producer: Implicits.Producer[(String, BSONValue)], producers: Implicits.Producer[(String, BSONValue)]* ) :AppendableBSONDocument = new AppendableBSONDocument(List()).append(producer, producers :_*)
  /** Makes a [[reactivemongo.bson.TraversableBSONDocument]] from the given buffer. */
  def apply(buffer: ChannelBuffer) :TraversableBSONDocument = new TraversableBSONDocument(buffer)
  /** Returns a String representing the given BSONDocument. */
  def pretty(document: BSONDocument) = "{\n" + BSONIterator.pretty(0, document.toTraversable.iterator) + "\n}"
}

object BSONArray {
  /** Makes an [[reactivemongo.bson.AppendableBSONArray]] containing the given values. */
  def apply(values: BSONValue*) :AppendableBSONArray = new AppendableBSONArray(Vector()).append(values :_*)
  /** Makes an [[reactivemongo.bson.AppendableBSONDocument]] containing the given values (produced by an `Implicits.Producer[E]`), if they are defined. */
  def apply( producer: Implicits.Producer[BSONValue], producers: Implicits.Producer[BSONValue]* ) :AppendableBSONArray = new AppendableBSONArray(Vector()).append(producer, producers :_*)
  /** Makes a [[reactivemongo.bson.TraversableBSONArray]] from the given buffer. */
  def apply(buffer: ChannelBuffer) :TraversableBSONArray = new TraversableBSONArray(buffer)
  /** Returns a String representing the given BSONArray. */
  def pretty(array: BSONArray) = "[\n" + BSONIterator.pretty(0, array.toTraversable.iterator) + "\n]"
}
// <---------------------------- BSON Structure handlers


/**
 * A BSON binary value.
 *
 * @param value The binary content.
 * @param subtype The type of the binary content.
 */
case class BSONBinary(value: ChannelBuffer, subtype: Subtype) extends BSONValue {
  val code = 0x05

  def write(buffer: ChannelBuffer) = { buffer writeInt value.readableBytes; buffer writeByte subtype.value; buffer writeBytes value; buffer }

  def this(value: Array[Byte], subtype: Subtype) = this(ChannelBuffers.wrappedBuffer(ByteOrder.LITTLE_ENDIAN, value), subtype)
}

/** BSON Undefined value */
object BSONUndefined extends BSONValue with BSONBooleanLike {
  val code = 0x06

  def write(buffer: ChannelBuffer) = buffer

  val toBoolean = false
}

/** BSON ObjectId value. */
case class BSONObjectID(value: Array[Byte]) extends BSONValue {
  val code = 0x07

  import java.util.Arrays

  /** Constructs a BSON ObjectId element from a hexadecimal String representation */
  def this(value: String) = this(Converters.str2Hex(value))

  /** ObjectId hexadecimal String representation */
  lazy val stringify = Converters.hex2Str(value)

  override def toString = "BSONObjectID[\"" + stringify + "\"]"

  override def equals(obj: Any) :Boolean = obj match {
    case BSONObjectID(arr) => Arrays.equals(value, arr)
    case _ => false
  }

  override lazy val hashCode :Int = Arrays.hashCode(value)

  def write(buffer: ChannelBuffer) = { buffer writeBytes value; buffer }

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
  def apply(id: String) :BSONObjectID = new BSONObjectID(id)

  /** Generates a new BSON ObjectID. */
  def generate :BSONObjectID = {
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
case class BSONBoolean(value: Boolean) extends BSONValue with BSONBooleanLike {
  val code = 0x08

  def write(buffer: ChannelBuffer) = { buffer writeByte (if (value) 1 else 0); buffer }

  val toBoolean = value
}

/** BSON date time value */
case class BSONDateTime(value: Long) extends BSONValue {
  val code = 0x09

  def write(buffer: ChannelBuffer) = { buffer writeLong value; buffer }
}

/** BSON null value */
object BSONNull extends BSONValue with BSONBooleanLike {
  val code = 0x0A

  def write(buffer: ChannelBuffer) = { buffer }

  val toBoolean = false
}

/**
 * BSON Regex value.
 *
 * @param flags Regex flags.
 */
case class BSONRegex(value: String, flags: String) extends BSONValue {
  val code = 0x0B

  def write(buffer: ChannelBuffer) = { buffer writeCString value; buffer writeCString flags }
}

/** BSON DBPointer value. TODO */
case class BSONDBPointer(value: String, id: Array[Byte]) extends BSONValue {
  val code = 0x0C

  def write(buffer: ChannelBuffer) = { buffer } // todo
}

/**
 * BSON JavaScript value.
 *
 * @param value The JavaScript source code.
 */
case class BSONJavaScript(value: String) extends BSONValue {
  val code = 0x0D

  def write(buffer: ChannelBuffer) = { buffer writeString value }
}

/** BSON Symbol value. */
case class BSONSymbol(value: String) extends BSONValue {
  val code = 0x0E

  def write(buffer: ChannelBuffer) = { buffer writeString value }
}

/**
 * BSON scoped JavaScript value.
 *
 * @param value The JavaScript source code. TODO
 */
case class BSONJavaScriptWS(value: String) extends BSONValue {
  val code = 0x0F

  def write(buffer: ChannelBuffer) = { buffer writeString value } // todo: where is the ws document ?
}

/** BSON Integer value */
case class BSONInteger(value: Int) extends BSONValue with BSONNumberLike {
  val code = 0x10

  def write(buffer: ChannelBuffer) = { buffer writeInt value; buffer }

  lazy val toInt = value
  lazy val toLong = value.toLong
  lazy val toDouble = value.toDouble
}

/** BSON Timestamp value. TODO */
case class BSONTimestamp(value: Long) extends BSONValue {
  val code = 0x11

  def write(buffer: ChannelBuffer) = { buffer writeLong value; buffer }
}

/** BSON Long value */
case class BSONLong(value: Long) extends BSONValue with BSONNumberLike {
  val code = 0x12

  def write(buffer: ChannelBuffer) = { buffer writeLong value; buffer }

  lazy val toInt = value.toInt
  lazy val toLong = value
  lazy val toDouble = value.toDouble
}

/** BSON Min key value */
object BSONMinKey extends BSONValue {
  val code = 0xFF

  def write(buffer: ChannelBuffer) = { buffer }
}

/** BSON Max key value */
object BSONMaxKey extends BSONValue {
  val code = 0x7F

  def write(buffer: ChannelBuffer) = { buffer }
}

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

/**
 * A Bson iterator from a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
 *
 * Iterating over this is completely lazy.
 */
sealed trait BSONIterator extends Iterator[BSONElement] {
  import Subtype._

  val buffer :ChannelBuffer

  val startIndex = buffer.readerIndex
  val documentSize = buffer.readInt

  def next :BSONElement = buffer.readByte match {
    case 0x01 => ReadBSONElement(buffer.readCString, BSONDouble(buffer.readDouble))
    case 0x02 => ReadBSONElement(buffer.readCString, BSONString(buffer.readString))
    case 0x03 => ReadBSONElement(buffer.readCString, BSONDocument(buffer.readBytes(buffer.getInt(buffer.readerIndex))))
    case 0x04 => ReadBSONElement(buffer.readCString, BSONArray(buffer.readBytes(buffer.getInt(buffer.readerIndex))))
    case 0x05 => {
      val name = buffer.readCString
      val length = buffer.readInt
      val subtype = buffer.readByte match {
        case 0x00 => GenericBinarySubtype
        case 0x01 => FunctionSubtype
        case 0x02 => OldBinarySubtype
        case 0x03 => UuidSubtype
        case 0x05 => Md5Subtype
        case 0x80 => UserDefinedSubtype
        case _ => throw new RuntimeException("unsupported binary subtype")
      }
      ReadBSONElement(name, BSONBinary(buffer.readBytes(length), subtype)) }
    case 0x06 => ReadBSONElement(buffer.readCString, BSONUndefined)
    case 0x07 => ReadBSONElement(buffer.readCString, BSONObjectID(buffer.readArray(12)))
    case 0x08 => ReadBSONElement(buffer.readCString, BSONBoolean(buffer.readByte == 0x01))
    case 0x09 => ReadBSONElement(buffer.readCString, BSONDateTime(buffer.readLong))
    case 0x0A => ReadBSONElement(buffer.readCString, BSONNull)
    case 0x0B => ReadBSONElement(buffer.readCString, BSONRegex(buffer.readCString, buffer.readCString))
    case 0x0C => ReadBSONElement(buffer.readCString, BSONDBPointer(buffer.readCString, buffer.readArray(12)))
    case 0x0D => ReadBSONElement(buffer.readCString, BSONJavaScript( buffer.readString))
    case 0x0E => ReadBSONElement(buffer.readCString, BSONSymbol(buffer.readString))
    case 0x0F => ReadBSONElement(buffer.readCString, BSONJavaScriptWS(buffer.readString))
    case 0x10 => ReadBSONElement(buffer.readCString, BSONInteger(buffer.readInt))
    case 0x11 => ReadBSONElement(buffer.readCString, BSONTimestamp(buffer.readLong))
    case 0x12 => ReadBSONElement(buffer.readCString, BSONLong(buffer.readLong))
    case 0xFF => ReadBSONElement(buffer.readCString, BSONMinKey)
    case 0x7F => ReadBSONElement(buffer.readCString, BSONMaxKey)
  }

  def hasNext = buffer.readerIndex - startIndex + 1 < documentSize

  def mapped :Map[String, BSONElement] = {
    for(el <- this) yield (el.name, el)
  }.toMap
}

/** Concrete lazy BSON iterator from a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]]. */
case class DefaultBSONIterator(buffer: ChannelBuffer) extends BSONIterator

object BSONIterator {
  private[bson] def pretty(i: Int, it: Iterator[BSONElement]) :String = {
    val prefix = (0 to i).map {i => "\t"}.mkString("")
    (for(v <- it) yield {
      v.value match {
        case doc :TraversableBSONDocument => prefix + v.name + ": {\n" + pretty(i + 1, doc.iterator) + "\n" + prefix +" }"
        case array :TraversableBSONArray => prefix + v.name + ": [\n" + pretty(i + 1, array.iterator) + "\n" + prefix +" ]"
        case _ => prefix + v.name + ": " + v.value.toString
      }
    }).mkString(",\n")
  }
  /** Makes a pretty String representation of the given [[reactivemongo.bson.BSONIterator]]. */
  def pretty(it: Iterator[BSONElement]) :String = "{\n" + pretty(0, it) + "\n}"
}

/**
 * Implicits for appending a mix of `BSONValue`, `Option[BSONValue]` in `BSONArray`s, of `(String, BSONValue)` and `(String, Option[BSONValue])` in `BSONDocument`s.
 * Example:
{{{
case class CappedOptions(
  size: Long,
  maxDocuments: Option[Int] = None
) {
  def toDocument = BSONDocument(
    "capped" -> BSONBoolean(true),
    "size" -> BSONLong(size),
    "max" -> maxDocuments.map(max => BSONLong(max)))
}
}}}
 */
object Implicits {
  trait Producer[T] {
    def produce :Option[T]
  }

  case class NameValueProducer(element: (String, BSONValue)) extends Producer[(String, BSONValue)] {
    def produce = Some(element)
  }

  case class NameOptionValueProducer(element: (String, Option[BSONValue])) extends Producer[(String, BSONValue)] {
    def produce = element._2.map(value => element._1 -> value)
  }

  case class ValueProducer(element: BSONValue) extends Producer[BSONValue] {
    def produce =  Some(element)
  }

  case class OptionValueProducer(element: Option[BSONValue]) extends Producer[BSONValue] {
    def produce = element
  }

  implicit def nameValue2Producer(element: (String, BSONValue)) = NameValueProducer(element)

  implicit def nameOptionValue2Producer(element: (String, Option[BSONValue])) = NameOptionValueProducer(element)

  implicit def valueProducer(element: BSONValue) = ValueProducer(element)

  implicit def optionValueProducer(element: Option[BSONValue]) = OptionValueProducer(element)
}
