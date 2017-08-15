package reactivemongo.bson.lowlevel

import reactivemongo.bson.buffer._

sealed trait Field {
  def tpe: Byte
  def name: String
}
trait ValueField[A <: AnyVal] { self: Field =>
  def value: A
}
case class BooleanField(name: String, value: Boolean) extends Field with ValueField[Boolean] {
  final val tpe = 0x08: Byte
}
case class IntField(name: String, value: Int) extends Field with ValueField[Int] {
  final val tpe = 0x10: Byte
}
case class DoubleField(name: String, value: Double) extends Field with ValueField[Double] {
  final val tpe = 0x01: Byte
}
case class NoValue(tpe: Byte, name: String) extends Field
case class LongField(tpe: Byte, name: String, value: Long) extends Field with ValueField[Long]

@SerialVersionUID(587711495L)
case class StructureField[A <: ReadableBuffer](tpe: Byte, name: String, @transient reader: LowLevelBsonDocReader[A]) extends Field

case class LazyField[A <: ReadableBuffer](tpe: Byte, name: String, buffer: A) extends Field

object LoweLevelDocumentIterator extends (ReadableBuffer => Iterator[ReadableBuffer]) {
  def apply(buf: ReadableBuffer): Iterator[ReadableBuffer] = new Iterator[ReadableBuffer] {
    private val slice = buf.slice(buf.readable)
    def hasNext = slice.readable >= 5 // length + 0x00
    def next = {
      val length = slice.readInt
      slice.index -= 4
      slice.slice(length)
    }
  }
}

class LowLevelBsonDocReader[A <: ReadableBuffer](rbuf: A) {
  private val start = rbuf.index
  private val length = {
    val res = rbuf.readInt
    rbuf.index = start
    res
  }
  private def slice = rbuf.slice(length)

  type ->[T, U] = (T, U)

  def lookup(name: String): Option[Field] =
    fieldStream.find(_.name == name)

  def fieldStream(): Stream[Field] = {
    val buf = slice
    buf.index = buf.index + 4

    def skipCString(): Unit =
      while (buf.readByte != 0x00) {}

    def stream(): Stream[Field] = {
      val tpe = buf.readByte
      val name = buf.readCString

      val field = (0xFF & tpe) match {
        case 0x01 =>
          DoubleField(name, buf.readDouble)
        case 0x09 | 0x11 | 0x12 =>
          LongField(tpe, name, buf.readLong)
        case 0x08 =>
          BooleanField(name, buf.readByte == 0x01)
        case 0x10 =>
          IntField(name, buf.readInt)
        case 0x03 | 0x04 =>
          val length = buf.readInt
          buf.index = buf.index - 4
          val res = StructureField(tpe, name, new LowLevelBsonDocReader(buf.slice(length)))
          buf.index += length
          res
        case 0x05 =>
          val length = buf.readInt + 4 + 1
          buf.index = buf.index - 4
          val res = LazyField(tpe, name, buf.slice(length))
          buf.index = buf.index + length
          res
        //buf.index = buf.index + buf.readInt + 5
        case 0x0B =>
          val now = buf.index
          skipCString(); skipCString()
          val length = buf.index - now
          buf.index = now
          val res = LazyField(tpe, name, buf.slice(length))
          buf.index = buf.index + length
          res
        case 0x02 | 0x0D | 0x0E =>
          val length = buf.readInt + 4
          buf.index = buf.index - 4
          val res = LazyField(tpe, name, buf.slice(length))
          buf.index = buf.index + length
          res
        case 0x0C =>
          val length = buf.readInt + 12 + 4
          buf.index = buf.index - 4
          val res = LazyField(tpe, name, buf.slice(length))
          buf.index = buf.index + length
          res
        case 0x07 =>
          val res = LazyField(tpe, name, buf.slice(12))
          buf.index = buf.index + 12
          res
        case 0x0F =>
          // TODO
          ???

        case 0x06 | 0x0A | 0xFF | 0x7F =>
          NoValue(tpe, name)

        case x => throw new RuntimeException(s"unexpected type $x")
      }
      if (buf.readable > 1)
        field #:: stream()
      else Stream(field)
    }

    stream
  }
}

class LowLevelBsonDocWriter[A <: WritableBuffer](buf: A) {
  private var marks = List[(Int, Int)](0 -> 0x03)

  buf.writeInt(0)

  def mark(tpe: Int): Unit = { marks = (buf.index, tpe) :: marks }

  def popMark: (Int, Int) = {
    val mark = marks.head
    marks = marks.tail
    mark
  }

  def putBytes(bytes: Array[Byte]): this.type = {
    buf.writeBytes(bytes)
    this
  }

  def putDouble(key: String, value: Double): this.type = {
    buf.writeByte(0x01)
    buf.writeCString(key)
    buf.writeDouble(value)
    this
  }
  def putString(key: String, value: String): this.type = {
    buf.writeByte(0x02)
    buf.writeCString(key)
    buf.writeString(value)
    this
  }
  def putUndefined(): this.type = {
    buf.writeByte(0x06)
    this
  }
  def putObjectId(key: String, value: Array[Byte]): this.type = {
    buf.writeByte(0x07)
    buf.writeCString(key)
    buf.writeBytes(value)
    this
  }
  def putBoolean(key: String, value: Boolean): this.type = {
    buf.writeByte(0x08)
    buf.writeCString(key)
    buf.writeByte(if (value) 1 else 0)
    this
  }
  def putDateTime(key: String, value: Long): this.type = {
    buf.writeByte(0x09)
    buf.writeCString(key)
    buf.writeLong(value)
    this
  }
  def putNull(): this.type = {
    buf.writeByte(0x0A)
    this
  }
  def putRegex(key: String, value: String, flags: String): this.type = {
    buf.writeByte(0x0B)
    buf.writeCString(key)
    buf.writeCString(value)
    buf.writeCString(flags)
    this
  }
  def putDBPointer(key: String, db: String, value: Array[Byte]): this.type = {
    buf.writeByte(0x0C)
    buf.writeCString(key)
    buf.writeString(db)
    buf.writeBytes(value)
    this
  }
  def putJavaScript(key: String, value: String): this.type = {
    buf.writeByte(0x0D)
    buf.writeCString(key)
    buf.writeString(value)
    this
  }
  def putSymbol(key: String, value: String): this.type = {
    buf.writeByte(0x0E)
    buf.writeCString(key)
    buf.writeString(value)
    this
  }
  def putInt(key: String, value: Int): this.type = {
    buf.writeByte(0x10)
    buf.writeCString(key)
    buf.writeInt(value)
    this
  }
  def putTimestamp(key: String, value: Long): this.type = {
    buf.writeByte(0x11)
    buf.writeCString(key)
    buf.writeLong(value)
    this
  }
  def putLong(key: String, value: Long): this.type = {
    buf.writeByte(0x12)
    buf.writeCString(key)
    buf.writeLong(value)
    this
  }
  def putMinKey(key: String): this.type = {
    buf.writeByte(0xFF.toByte)
    buf.writeCString(key)
    this
  }
  def putMaxKey(key: String): this.type = {
    buf.writeByte(0x7F)
    buf.writeCString(key)
    this
  }

  def openDocument(key: String): this.type = {
    buf.writeByte(0x03)
    buf.writeCString(key)
    mark(0x03)
    buf.writeInt(0) // length
    this
  }
  def openArray(key: String): this.type = {
    buf.writeByte(0x04)
    buf.writeCString(key)
    mark(0x04)
    buf.writeInt(0) // length
    this
  }
  def openBinary(key: String, tpe: Byte): this.type = {
    buf.writeByte(0x05)
    buf.writeCString(key)
    mark(0x05) // TODO
    buf.writeInt(0) // length
    buf.writeByte(tpe)
    this
  }
  def openJavaScriptWithScope(key: String, value: String): this.type = {
    buf.writeByte(0x0F)
    buf.writeCString(key)
    mark(0x0F)
    buf.writeInt(0) // length of value + doc
    buf.writeString(value)
    mark(0x03)
    buf.writeInt(0) // length of scope doc
    this
  }

  def close(): this.type = {
    val (pos, tpe) = popMark
    if (tpe == 0x05)
      buf.setInt(pos, buf.index - pos - 1) // no trailing nul, excluding binary type byte
    else {
      buf.setInt(pos, buf.index - pos + 1)
      buf.writeByte(0)
    }
    this
  }

  def result(): A = buf
}
/*
sealed trait NonStructuralBSONValue[A <: BSONValue]
object NonStructuralBSONValue {
  implicit def doubleEv: NonStructuralBSONValue[BSONDouble] = null
  implicit def stringEv: NonStructuralBSONValue[BSONString] = null
  //implicit def binaryEv: NonStructuralBSONValue[BSONBinary] = null
  implicit def undefinedEv: NonStructuralBSONValue[BSONUndefined.type] = null
  implicit def objectIdEv: NonStructuralBSONValue[BSONObjectID] = null
  implicit def booleanEv: NonStructuralBSONValue[BSONBoolean] = null
  implicit def dateTimeEv: NonStructuralBSONValue[BSONDateTime] = null
  implicit def nullEv: NonStructuralBSONValue[BSONNull.type] = null
  implicit def regexEv: NonStructuralBSONValue[BSONRegex] = null
  implicit def dbPointerEv: NonStructuralBSONValue[BSONDBPointer] = null
  implicit def jsEv: NonStructuralBSONValue[BSONJavaScript] = null
  implicit def symbolEv: NonStructuralBSONValue[BSONSymbol] = null
  implicit def jswsEv: NonStructuralBSONValue[BSONJavaScriptWS] = null
  implicit def integerEv: NonStructuralBSONValue[BSONInteger] = null
  implicit def timestampEv: NonStructuralBSONValue[BSONTimestamp] = null
  implicit def longEv: NonStructuralBSONValue[BSONLong] = null
  implicit def minKeyEv: NonStructuralBSONValue[BSONMinKey.type] = null
  implicit def maxKeyEv: NonStructuralBSONValue[BSONMaxKey.type] = null
}
*/
