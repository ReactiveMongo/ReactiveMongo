package org.asyncmongo.bson

import org.jboss.netty.buffer.ChannelBuffers
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

class Bson(val estimatedLength:Int = 32) {
	private val buffer = ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, estimatedLength)
	buffer.writeInt(0)

	def writeString(s: String) :Bson = {
		val bytes = s.getBytes("utf-8")
		buffer writeInt (bytes.size + 1)
		buffer writeBytes bytes
		buffer writeByte 0
		this
	}

	def writeCString(s: String) :Bson = {
		val bytes = s.getBytes("utf-8")
		buffer writeBytes bytes
		buffer writeByte 0
		this
	}

	def writeElement(name: String, value: Double) :Bson = {
		buffer writeByte 0x01
		this writeCString name
		buffer writeDouble value
		this
	}

	def writeElement(name: String, value: String) :Bson = {
		buffer writeByte 0x02
		writeCString(name)
		writeString(value)
		this		
	}

	def writeElement(name: String, value: Array[Byte], subtype: Subtype) :Bson = {
		buffer writeByte 0x05
		writeCString(name)
		writeBinary(subtype, value)
		this		
	}

	def writeUndefined(name: String, value: String) :Bson = {
		buffer writeByte 0x06
		writeCString(name)
		this		
	}

	def writeObjectId(name: String, value: String) :Bson = {
		buffer writeByte 0x07
		writeCString(name)
		value.grouped(2).foreach { s =>
			buffer writeByte (Integer.parseInt(s, 16).toByte)
		}
		this		
	}

	def writeElement(name: String, value: Boolean) :Bson = {
		buffer writeByte 0x08
		writeCString(name)
		buffer writeByte (if(value) 1 else 0)
		this		
	}

	def writeElement(name: String, value: java.util.Date) :Bson = {
		buffer writeByte 0x09
		writeCString(name)
		buffer writeLong value.getTime
		this		
	}

	def writeNull(name: String, value: String) :Bson = {
		buffer writeByte 0x0A
		writeCString(name)
		this		
	}
	// todo
	def writeElement(name: String, value: java.util.regex.Pattern) :Bson = {
		buffer writeByte 0x0B
		writeCString(name)
		writeCString(value.pattern)
		writeCString("")
		this
	}

	def writeJavascript(name: String, value: String) :Bson = {
		buffer writeByte 0x0D
		writeCString(name)
		writeString(value)
		this
	}

	def writeSymbol(name: String, value: String) :Bson = {
		buffer writeByte 0x0E
		writeCString(name)
		writeString(name)
		this
	}

	def writeJavascript(name: String, value: String, document: Bson) :Bson = {
		//buffer writeInt 0x0F
		// todo
		this
	}

	def writeElement(name: String, value: Int) :Bson = {
		buffer writeByte 0x10
		writeCString(name)
		buffer writeInt value
		this
	}

	def writeTimestamp(name: String, value: Long) :Bson = {
		buffer writeByte 0x11
		writeCString(name)
		buffer writeLong value
		this
	}

	def writeElement(name: String, value: Long) :Bson = {
		buffer writeByte 0x12
		writeCString(name)
		buffer writeLong value
		this
	}

	def writeMinKey(name: String) :Bson = {
		buffer writeByte 0xFF
		writeCString(name)
		this
	}

	def writeMaxKey(name: String) :Bson = {
		buffer writeByte 0x7F
		writeCString(name)
		this
	}

	def writeBinary(subtype: Subtype, bytes: Array[Byte]) :Bson = {
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
		/*bson.writeElement("name", "Jack")
		bson.writeElement("age", 37)*/
		bson.writeElement("getLastError", 1)
		val is = new ChannelBufferInputStream(bson.getBuffer)
		val produced = mapper.readValue(is, classOf[java.util.HashMap[Object, Object]])
		println(produced)
	}
}