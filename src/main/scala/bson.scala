package org.asyncmongo.bson

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import java.nio.ByteOrder

object `package` {

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

  def writeString(s: String): Bson = {
    val bytes = s.getBytes("utf-8")
    buffer writeInt (bytes.size + 1)
    buffer writeBytes bytes
    buffer writeByte 0
    this
  }

  def writeCString(s: String): Bson = {
    val bytes = s.getBytes("utf-8")
    buffer writeBytes bytes
    buffer writeByte 0
    this
  }

  def writeElement(name: String, value: Double): Bson = {
    buffer writeByte 0x01
    this writeCString name
    buffer writeDouble value
    this
  }

  def writeElement(name: String, value: String): Bson = {
    buffer writeByte 0x02
    writeCString(name)
    writeString(value)
    this
  }
  
  def writeElement(name: String, value: Bson, asArray: Boolean = false): Bson = {
    buffer writeByte (if(asArray) 0x04 else 0x03)
    writeCString(name)
    buffer.writeBytes(value.getBuffer)
    this
  }

  def writeElement(name: String, value: Array[Byte], subtype: Subtype): Bson = {
    buffer writeByte 0x05
    writeCString(name)
    writeBinary(subtype, value)
    this
  }

  def writeUndefined(name: String, value: String): Bson = {
    buffer writeByte 0x06
    writeCString(name)
    this
  }

  def writeObjectId(name: String, value: String): Bson = {
    buffer writeByte 0x07
    writeCString(name)
    value.grouped(2).foreach { s =>
      buffer writeByte (Integer.parseInt(s, 16).toByte)
    }
    this
  }

  def writeElement(name: String, value: Boolean): Bson = {
    buffer writeByte 0x08
    writeCString(name)
    buffer writeByte (if (value) 1 else 0)
    this
  }

  def writeElement(name: String, value: java.util.Date): Bson = {
    buffer writeByte 0x09
    writeCString(name)
    buffer writeLong value.getTime
    this
  }

  def writeNull(name: String, value: String): Bson = {
    buffer writeByte 0x0A
    writeCString(name)
    this
  }
  // todo
  def writeElement(name: String, value: java.util.regex.Pattern): Bson = {
    buffer writeByte 0x0B
    writeCString(name)
    writeCString(value.pattern)
    writeCString("")
    this
  }

  def writeJavascript(name: String, value: String): Bson = {
    buffer writeByte 0x0D
    writeCString(name)
    writeString(value)
    this
  }

  def writeSymbol(name: String, value: String): Bson = {
    buffer writeByte 0x0E
    writeCString(name)
    writeString(name)
    this
  }

  def writeJavascript(name: String, value: String, document: Bson): Bson = {
    //buffer writeInt 0x0F
    // todo
    this
  }

  def writeElement(name: String, value: Int): Bson = {
    buffer writeByte 0x10
    writeCString(name)
    buffer writeInt value
    this
  }

  def writeTimestamp(name: String, value: Long): Bson = {
    buffer writeByte 0x11
    writeCString(name)
    buffer writeLong value
    this
  }

  def writeElement(name: String, value: Long): Bson = {
    buffer writeByte 0x12
    writeCString(name)
    buffer writeLong value
    this
  }

  def writeMinKey(name: String): Bson = {
    buffer writeByte 0xFF
    writeCString(name)
    this
  }

  def writeMaxKey(name: String): Bson = {
    buffer writeByte 0x7F
    writeCString(name)
    this
  }

  def writeBinary(subtype: Subtype, bytes: Array[Byte]): Bson = {
    buffer writeInt bytes.size
    buffer writeInt subtype.value
    buffer writeBytes bytes
    this
  }
  def writeGenericBinary(bytes: Array[Byte]) = writeBinary(genericBinarySubtype, bytes)
  def writeFunctionBinary(bytes: Array[Byte]) = writeBinary(functionSubtype, bytes)
  def writeOldBinary(bytes: Array[Byte]) = writeBinary(oldBinarySubtype, bytes)
  def writeUUIDBinary(bytes: Array[Byte]) = writeBinary(uuidSubtype, bytes)
  def writeUserDefinedBinary(bytes: Array[Byte]) = writeBinary(userDefinedSubtype, bytes)

  lazy val getBuffer = {
    buffer.writeByte(0)
    val index = buffer.writerIndex
    println("buffer index is " + index)
    buffer.setInt(0, index)
    println(buffer)
    println("-----ing: " + java.util.Arrays.toString(buffer.toByteBuffer.array()))
    buffer
  }
}

/*
element	::=	"\x01" e_name double	Floating point
|	"\x02" e_name string	UTF-8 string
|	"\x03" e_name document	Embedded document
|	"\x04" e_name document	Array
|	"\x05" e_name binary	Binary data
|	"\x06" e_name	Undefined — Deprecated
|	"\x07" e_name (byte*12)	ObjectId
|	"\x08" e_name "\x00"	Boolean "false"
|	"\x08" e_name "\x01"	Boolean "true"
|	"\x09" e_name int64	UTC datetime
|	"\x0A" e_name	Null value
|	"\x0B" e_name cstring cstring	Regular expression
|	"\x0C" e_name string (byte*12)	DBPointer — Deprecated
|	"\x0D" e_name string	JavaScript code
|	"\x0E" e_name string	Symbol
|	"\x0F" e_name code_w_s	JavaScript code w/ scope
|	"\x10" e_name int32	32-bit Integer
|	"\x11" e_name int64	Timestamp
|	"\x12" e_name int64	64-bit integer
|	"\xFF" e_name	Min key
|	"\x7F" e_name	Max key
*/
sealed trait BSONElement {
	val code :Int
	val name: String
}
// sam apple pie

case class BSONDouble        (name: String, value: Double)                   extends BSONElement { val code = 0x01; }
case class BSONString        (name: String, value: String)                   extends BSONElement { val code = 0x02; }
case class BSONDocument      (name: String, value: ChannelBuffer)            extends BSONElement { val code = 0x03; }
case class BSONArray         (name: String, value: ChannelBuffer)            extends BSONElement { val code = 0x04; }
case class BSONBinary        (name: String, value: ChannelBuffer)            extends BSONElement { val code = 0x05; }
case class BSONUndefined     (name: String)                                  extends BSONElement { val code = 0x06; }
case class BSONObjectID      (name: String, value: Array[Byte])              extends BSONElement { val code = 0x07; }
case class BSONBoolean       (name: String, value: Boolean)                  extends BSONElement { val code = 0x08; }
case class BSONDateTime      (name: String, value: Long)                     extends BSONElement { val code = 0x09; }
case class BSONNull          (name: String)                                  extends BSONElement { val code = 0x0A; }
case class BSONRegex         (name: String, value: String, flags: String)    extends BSONElement { val code = 0x0B; }
case class BSONDBPointer     (name: String, value: String, id: Array[Byte])  extends BSONElement { val code = 0x0C; }
case class BSONJavaScript    (name: String, value: String)                   extends BSONElement { val code = 0x0D; }
case class BSONSymbol        (name: String, value: String)                   extends BSONElement { val code = 0x0E; }
case class BSONJavaScriptWS  (name: String, value: String)                   extends BSONElement { val code = 0x0F; }
case class BSONInteger       (name: String, value: Int)                      extends BSONElement { val code = 0x10; }
case class BSONTimestamp     (name: String, value: Long)                     extends BSONElement { val code = 0x11; }
case class BSONLong          (name: String, value: Long)                     extends BSONElement { val code = 0x12; }
case class BSONMinKey        (name: String)                                  extends BSONElement { val code = 0xFF; }
case class BSONMaxKey        (name: String)                                  extends BSONElement { val code = 0x7F; }

sealed trait BSONIterable extends Iterator[BSONElement] {
	import org.asyncmongo.utils.RichBuffer._

	val buffer :ChannelBuffer
	//implicit def stringToKey[Key](s: String) :Key

	val startIndex = buffer.readerIndex
	val documentSize = buffer.readInt

	def next :BSONElement = buffer.readByte match {
		case 0x01 => BSONDouble(buffer.readCString, buffer.readDouble)
		case 0x02 => BSONString(buffer.readCString, buffer.readUTF8)
		case 0x03 => BSONDocument(buffer.readCString, buffer.readBytes(buffer.getInt(buffer.readerIndex)))
		case 0x04 => BSONArray(buffer.readCString, buffer.readBytes(buffer.getInt(buffer.readerIndex)))
		case 0x05 => BSONBinary(buffer.readCString, buffer.readBytes(buffer.getInt(buffer.readerIndex)))
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

	def hasNext = {
		println("buffer hasNext :: readerIndex=" + buffer.readerIndex + ", startIndex=" + startIndex + ", documentSize=" + documentSize + ", " + (buffer.readerIndex - startIndex))
		buffer.readerIndex - startIndex + 1< documentSize
	}
}

case class DefaultBSONIterator(buffer: ChannelBuffer) extends BSONIterable

object DefaultBSONIterator {
	private def pretty(i: Int, it: DefaultBSONIterator) :String = {
		val prefix = (0 to i).map {i => "\t"}.mkString("")
		(for(v <- it) yield {
			v match {
				case BSONDocument(n, b) => prefix + n + " -> {\n" + pretty(i + 1, DefaultBSONIterator(b)) + "\n" + prefix +" }"
				case _ => prefix + v.name + " -> " + v.toString
			}
		}).mkString("\n")
	}
	def pretty(it: DefaultBSONIterator) :String = "{\n" + pretty(0, it) + "\n}"
}

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
    bson.writeElement("name", "Jack")
		bson.writeElement("age", 37)
    bson.writeElement("getLastError", 1)
    println(DefaultBSONIterator(bson.getBuffer).toList)
    //val it = DefaultBSONIterator(bson.getBuffer)
    //while(it.hasNext)
    //	println(it.next)
    /*val is = new ChannelBufferInputStream(bson.getBuffer)
    val produced = mapper.readValue(is, classOf[java.util.HashMap[Object, Object]])
    println(produced)*/
  }
}