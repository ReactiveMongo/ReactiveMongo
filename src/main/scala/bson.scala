package org.asyncmongo.bson

import org.asyncmongo.utils.Converters
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import java.nio.ByteOrder

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
sealed trait BSONElement {
  val code :Int
  val name: String

  def write(buffer: ChannelBuffer) :ChannelBuffer = {
    buffer writeByte code
    writeCString(name, buffer)
  }

  def writeContent(buffer: ChannelBuffer) :ChannelBuffer

  protected[bson] final def writeCString(s: String, buffer: ChannelBuffer): ChannelBuffer = {
    val bytes = s.getBytes("utf-8")
    buffer writeBytes bytes
    buffer writeByte 0
    buffer
  }

  protected[bson] final def writeString(s: String, buffer: ChannelBuffer): ChannelBuffer = {
    val bytes = s.getBytes("utf-8")
    buffer writeInt (bytes.size + 1)
    buffer writeBytes bytes
    buffer writeByte 0
    buffer
  }
}
// sam apple pie

case class BSONDouble        (name: String, value: Double) extends BSONElement {
  val code = 0x01

  def writeContent(buffer: ChannelBuffer) = { buffer writeDouble value; buffer }
}

case class BSONString        (name: String, value: String) extends BSONElement {
  val code = 0x02

  def writeContent(buffer: ChannelBuffer) = { writeString(value, buffer) }
}

case class BSONDocument      (name: String, value: ChannelBuffer) extends BSONElement {
  val code = 0x03

  def writeContent(buffer: ChannelBuffer) = { buffer.writeBytes(value); buffer }
}

case class BSONArray         (name: String, value: ChannelBuffer) extends BSONElement {
  val code = 0x04

  def writeContent(buffer: ChannelBuffer) = { buffer writeBytes value; buffer }
}

case class BSONBinary        (name: String, value: ChannelBuffer, subtype: Subtype) extends BSONElement {
  val code = 0x05

  def writeContent(buffer: ChannelBuffer) = { buffer writeInt value.readableBytes; buffer writeByte subtype.value; buffer writeBytes value; buffer }
}

case class BSONUndefined     (name: String) extends BSONElement {
  val code = 0x06

  def writeContent(buffer: ChannelBuffer) = buffer
}

case class BSONObjectID      (name: String, value: Array[Byte]) extends BSONElement {
  val code = 0x07

  def this(name: String, value: String) = this(name, Converters.str2Hex(value))

  lazy val stringify = Converters.hex2Str(value)

  override def toString = "BSONObjectID[\"" + stringify + "\"]"

  def writeContent(buffer: ChannelBuffer) = { buffer writeBytes value; buffer }
}

case class BSONBoolean       (name: String, value: Boolean) extends BSONElement {
  val code = 0x08

  def writeContent(buffer: ChannelBuffer) = { buffer writeByte (if (value) 1 else 0); buffer }
}

case class BSONDateTime      (name: String, value: Long) extends BSONElement {
  val code = 0x09

  def writeContent(buffer: ChannelBuffer) = { buffer writeLong value; buffer }
}

case class BSONNull          (name: String) extends BSONElement {
  val code = 0x0A

  def writeContent(buffer: ChannelBuffer) = { buffer }
}

case class BSONRegex         (name: String, value: String, flags: String) extends BSONElement {
  val code = 0x0B

  def writeContent(buffer: ChannelBuffer) = { writeCString(value, buffer); writeCString(flags, buffer) }
}

case class BSONDBPointer     (name: String, value: String, id: Array[Byte]) extends BSONElement {
  val code = 0x0C

  def writeContent(buffer: ChannelBuffer) = { buffer } // todo
}

case class BSONJavaScript    (name: String, value: String) extends BSONElement {
  val code = 0x0D

  def writeContent(buffer: ChannelBuffer) = { writeString(value, buffer) }
}

case class BSONSymbol        (name: String, value: String) extends BSONElement {
  val code = 0x0E

  def writeContent(buffer: ChannelBuffer) = { writeString(value, buffer) }
}

case class BSONJavaScriptWS  (name: String, value: String) extends BSONElement {
  val code = 0x0F

  def writeContent(buffer: ChannelBuffer) = { writeString(value, buffer) } // todo: where is the ws document ?
}

case class BSONInteger       (name: String, value: Int) extends BSONElement {
  val code = 0x10

  def writeContent(buffer: ChannelBuffer) = { buffer writeInt value; buffer }
}

case class BSONTimestamp     (name: String, value: Long) extends BSONElement {
  val code = 0x11

  def writeContent(buffer: ChannelBuffer) = { buffer writeLong value; buffer }
}

case class BSONLong          (name: String, value: Long) extends BSONElement {
  val code = 0x12

  def writeContent(buffer: ChannelBuffer) = { buffer writeLong value; buffer }
}

case class BSONMinKey        (name: String) extends BSONElement {
  val code = 0xFF

  def writeContent(buffer: ChannelBuffer) = { buffer }
}

case class BSONMaxKey        (name: String) extends BSONElement {
  val code = 0x7F

  def writeContent(buffer: ChannelBuffer) = { buffer }
}


sealed trait Subtype {
  val value: Int
}
case object genericBinarySubtype extends Subtype { override val value = 0x00 }
case object functionSubtype extends Subtype { override val value = 0x01 }
case object oldBinarySubtype extends Subtype { override val value = 0x02 }
case object uuidSubtype extends Subtype { override val value = 0x03 }
case object md5Subtype extends Subtype { override val value = 0x05 }
case object userDefinedSubtype extends Subtype { override val value = 0x80 }

object Subtypes

class Bson(val estimatedLength: Int = 32) {
  private val buffer = ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, estimatedLength)
  buffer.writeInt(0)

  def write(el: BSONElement) :Bson = {
    el.write(buffer)
    this
  }

  lazy val getBuffer = {
    buffer.writeByte(0)
    buffer.setInt(0, buffer.writerIndex)
    buffer
  }
}


sealed trait BSONIterator extends Iterator[BSONElement] {
  import org.asyncmongo.utils.RichBuffer._

  val buffer :ChannelBuffer

  val startIndex = buffer.readerIndex
  val documentSize = buffer.readInt

  def next :BSONElement = buffer.readByte match {
    case 0x01 => BSONDouble(buffer.readCString, buffer.readDouble)
    case 0x02 => BSONString(buffer.readCString, buffer.readUTF8)
    case 0x03 => BSONDocument(buffer.readCString, buffer.readBytes(buffer.getInt(buffer.readerIndex)))
    case 0x04 => BSONArray(buffer.readCString, buffer.readBytes(buffer.getInt(buffer.readerIndex)))
    case 0x05 => {
      val length = buffer.readInt
      val subtype = buffer.readByte match {
        case 0x00 => genericBinarySubtype
        case 0x01 => functionSubtype
        case 0x02 => oldBinarySubtype
        case 0x03 => uuidSubtype
        case 0x05 => md5Subtype
        case 0x80 => userDefinedSubtype
        case _ => throw new RuntimeException("unsupported binary subtype")
      }
      BSONBinary(buffer.readCString, buffer.readBytes(length), subtype) }
    case 0x06 => BSONUndefined(buffer.readCString)
    case 0x07 => BSONObjectID(buffer.readCString, buffer.readArray(12))
    case 0x08 => BSONBoolean(buffer.readCString, buffer.readByte == 0x01)
    case 0x09 => BSONDateTime(buffer.readCString, buffer.readLong)
    case 0x0A => BSONNull(buffer.readCString)
    case 0x0B => BSONRegex(buffer.readCString, buffer.readCString, buffer.readCString)
    case 0x0C => BSONDBPointer(buffer.readCString, buffer.readCString, buffer.readArray(12))
    case 0x0D => BSONJavaScript(buffer.readCString, buffer.readUTF8)
    case 0x0E => BSONSymbol(buffer.readCString, buffer.readUTF8)
    case 0x0F => BSONJavaScriptWS(buffer.readCString, buffer.readUTF8)
    case 0x10 => BSONInteger(buffer.readCString, buffer.readInt)
    case 0x11 => BSONTimestamp(buffer.readCString, buffer.readLong)
    case 0x12 => BSONLong(buffer.readCString, buffer.readLong)
    case 0xFF => BSONMinKey(buffer.readCString)
    case 0x7F => BSONMaxKey(buffer.readCString)
  }

  def hasNext = buffer.readerIndex - startIndex + 1 < documentSize

  def mapped :Map[String, BSONElement] = {
    for(el <- this) yield (el.name, el)
  }.toMap
}

case class DefaultBSONIterator(buffer: ChannelBuffer) extends BSONIterator

object DefaultBSONIterator {
  private def pretty(i: Int, it: DefaultBSONIterator) :String = {
    val prefix = (0 to i).map {i => "\t"}.mkString("")
    (for(v <- it) yield {
      v match {
        case BSONDocument(n, b) => prefix + n + ": {\n" + pretty(i + 1, DefaultBSONIterator(b)) + "\n" + prefix +" }"
        case _ => prefix + v.name + ": " + v.toString
      }
    }).mkString(",\n")
  }
  def pretty(it: DefaultBSONIterator) :String = "{\n" + pretty(0, it) + "\n}"
}


// test purposes - to remove
object Bson {
  def test {
    import de.undercouch.bson4jackson._
    import de.undercouch.bson4jackson.io._
    import de.undercouch.bson4jackson.uuid._
    import org.jboss.netty.channel._
    import org.jboss.netty.buffer._
    import org.codehaus.jackson.map.ObjectMapper
    import java.io._
    val mapper = {
      val fac = new BsonFactory()
      fac.enable(BsonParser.Feature.HONOR_DOCUMENT_LENGTH)
      val om = new ObjectMapper(fac)
      om.registerModule(new BsonUuidModule())
      om
    }

    val factory = new BsonFactory()

    //serialize data
    val baos = new ByteArrayOutputStream();
    val gen = factory.createJsonGenerator(baos);
    gen.writeStartObject();
    /*gen.writeStringField("name", "Jack")
    gen.writeNumberField("age", 37)*/
    gen.writeNumberField("getLastError", 1)
    gen.writeEndObject()
    gen.close()

    println("awaiting: " + java.util.Arrays.toString(baos.toByteArray))

    val bson = new Bson
    bson.write(BSONString("name", "Jack"))
    bson.write(BSONInteger("age", 37))
    bson.write(BSONInteger("getLastError", 1))
    println(DefaultBSONIterator(bson.getBuffer).toList)
    //val it = DefaultBSONIterator(bson.getBuffer)
    //while(it.hasNext)
    //  println(it.next)
    /*val is = new ChannelBufferInputStream(bson.getBuffer)
    val produced = mapper.readValue(is, classOf[java.util.HashMap[Object, Object]])
    println(produced)*/
  }
}