package core

import akka.util.ByteStringBuilder
import reactivemongo.bson.buffer.{ReadableBuffer, WritableBuffer}

/**
 * Created by sh1ng on 14/05/15.
 */

case class AkkaByteStringWritableBuffer(builder: ByteStringBuilder = new ByteStringBuilder()) extends WritableBuffer{
  implicit val byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN

  /** Returns the current write index of this buffer. */
  override def index: Int = builder.length

  /** Writes the given `Double` into this buffer. */
  override def writeDouble(double: Double): WritableBuffer = {
    builder.putDouble(double)
    this
  }

  /** Writes the given `Int` into this buffer. */
  override def writeInt(int: Int): WritableBuffer = {
    builder.putInt(int)
    this
  }

  /** Writes the bytes stored in the given `array` into this buffer. */
  override def writeBytes(array: Array[Byte]): WritableBuffer = {
    builder.putBytes(array)
    this
  }

  /** Writes the given `Long` into this buffer. */
  override def writeLong(long: Long): WritableBuffer = {
    builder.putLong(long)
    this
  }

  /** Writes the given `Byte` into this buffer. */
  override def writeByte(byte: Byte): WritableBuffer = {
    builder.putByte(byte)
    this
  }

  /** Replaces 4 bytes at the given `index` by the given `value` */
  override def setInt(index: Int, value: Int): WritableBuffer = ???
}
