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
package reactivemongo.bson.buffer

import scala.collection.mutable.ArrayBuffer

/**
 * A writable buffer.
 *
 * The implementation '''MUST''' ensure it stores data in little endian when needed.
 */
trait WritableBuffer { self =>
  /** Returns the current write index of this buffer. */
  def index: Int

  /** Replaces 4 bytes at the given `index` by the given `value` */
  def setInt(index: Int, value: Int): WritableBuffer

  /** Writes the bytes stored in the given `array` into this buffer. */
  def writeBytes(array: Array[Byte]): self.type

  /** Writes the bytes stored in the given `buffer` into this buffer. */
  def writeBytes(buffer: ReadableBuffer): self.type = {
    @annotation.tailrec
    def write(buf: ReadableBuffer): self.type = {
      if (buf.readable > 1024) {
        writeBytes(buf.readArray(1024))
        write(buf)
      } else writeBytes(buf.readArray(buf.readable))
    }

    write(buffer.slice(buffer.readable))
  }

  /** Writes the given `Byte` into this buffer. */
  def writeByte(byte: Byte): WritableBuffer

  /** Writes the given `Int` into this buffer. */
  def writeInt(int: Int): WritableBuffer

  /** Writes the given `Long` into this buffer. */
  def writeLong(long: Long): WritableBuffer

  /** Writes the given `Double` into this buffer. */
  def writeDouble(double: Double): WritableBuffer

  def toReadableBuffer(): ReadableBuffer

  /** Write a UTF-8 encoded C-Style String. */
  def writeCString(s: String): WritableBuffer = {
    val bytes = s.getBytes("utf-8")
    writeBytes(bytes).writeByte(0)
  }

  /** Write a UTF-8 encoded String. */
  def writeString(s: String): WritableBuffer = {
    val bytes = s.getBytes("utf-8")
    writeInt(bytes.size + 1).writeBytes(bytes).writeByte(0)
  }
}

/**
 * A readable buffer.
 *
 * The implementation '''MUST''' ensure it reads data in little endian when needed.
 */
trait ReadableBuffer {
  /** Returns the current read index of this buffer. */
  def index: Int

  def index_=(i: Int): Unit

  /** Sets the read index to `index + n` (in other words, skips `n` bytes). */
  def discard(n: Int): Unit

  /** Fills the given array with the bytes read from this buffer. */
  def readBytes(bytes: Array[Byte]): Unit

  /** Reads a `Byte` from this buffer. */
  def readByte(): Byte

  /** Reads an `Int` from this buffer. */
  def readInt(): Int

  /** Reads a `Long` from this buffer. */
  def readLong(): Long

  /** Reads a `Double` from this buffer. */
  def readDouble(): Double

  /** Returns the number of readable remaining bytes of this buffer. */
  def readable(): Int

  def toWritableBuffer: WritableBuffer

  /**
   * Returns a new instance of ReadableBuffer which starts at the current index and contains `n` bytes.
   *
   * This method does not update the read index of the original buffer.
   */
  def slice(n: Int): ReadableBuffer

  /** Returns the buffer size. */
  def size: Int

  /** Reads a UTF-8 String. */
  def readString(): String = {
    val bytes = new Array[Byte](this.readInt - 1)
    this.readBytes(bytes)
    this.readByte
    new String(bytes, "UTF-8")
  }

  /**
   * Reads an array of Byte of the given length.
   *
   * @param length Length of the newly created array.
   */
  def readArray(length: Int): Array[Byte] = {
    val bytes = new Array[Byte](length)
    this.readBytes(bytes)
    bytes
  }

  /** Reads a UTF-8 C-Style String. */
  def readCString(): String = readCString(new ArrayBuffer[Byte](16))

  @scala.annotation.tailrec
  private def readCString(array: ArrayBuffer[Byte]): String = {
    val byte = this.readByte

    if (byte == 0x00) {
      new String(array.toArray, "UTF-8")
    } else readCString(array += byte)
  }

  def duplicate(): ReadableBuffer
}

trait BSONBuffer extends ReadableBuffer with WritableBuffer

import java.nio.{ ByteBuffer, ByteOrder }, ByteOrder._

/** An array-backed readable buffer. */
case class ArrayReadableBuffer private (
  bytebuffer: ByteBuffer) extends ReadableBuffer {

  bytebuffer.order(LITTLE_ENDIAN)

  def size = bytebuffer.limit()

  def index = bytebuffer.position()

  def index_=(i: Int) = { bytebuffer.position(i); () }

  def discard(n: Int) =
    { bytebuffer.position(bytebuffer.position() + n); () }

  def slice(n: Int) = {
    val nb = bytebuffer.slice()
    nb.limit(n)
    new ArrayReadableBuffer(nb)
  }

  def readBytes(array: Array[Byte]): Unit = { bytebuffer.get(array); () }

  def readByte() = bytebuffer.get()

  def readInt() = bytebuffer.getInt()

  def readLong() = bytebuffer.getLong()

  def readDouble() = bytebuffer.getDouble()

  def readable() = bytebuffer.remaining()

  def toWritableBuffer: WritableBuffer = {
    val buf = new ArrayBSONBuffer()
    buf.writeBytes(this)
  }

  def duplicate() = new ArrayReadableBuffer(bytebuffer.duplicate())
}

object ArrayReadableBuffer {
  /** Returns an [[ArrayReadableBuffer]] which source is the given `array`. */
  def apply(array: Array[Byte]) = new ArrayReadableBuffer(ByteBuffer.wrap(array))
}

/** An array-backed writable buffer. */
class ArrayBSONBuffer protected[buffer] (
  protected val buffer: ArrayBuffer[Byte]) extends WritableBuffer {
  def index = buffer.length // useless

  def bytebuffer(size: Int) = {
    val b = ByteBuffer.allocate(size)
    b.order(java.nio.ByteOrder.LITTLE_ENDIAN)
    b
  }

  def this() = this(new ArrayBuffer[Byte]())

  /** Returns an array containing all the data that were put in this buffer. */
  def array = buffer.toArray

  def setInt(index: Int, value: Int) = {
    val array = bytebuffer(4).putInt(value).array
    buffer.update(index, array(0))
    buffer.update(index + 1, array(1))
    buffer.update(index + 2, array(2))
    buffer.update(index + 3, array(3))
    this
  }

  def toReadableBuffer = ArrayReadableBuffer(array)

  def writeBytes(array: Array[Byte]): this.type = {
    buffer ++= array
    //index += array.length
    this
  }

  def writeByte(byte: Byte): WritableBuffer = {
    buffer += byte
    //index += 1
    this
  }

  def writeInt(int: Int): WritableBuffer = {
    val array = bytebuffer(4).putInt(int).array
    buffer ++= array
    //index += 4
    this
  }

  def writeLong(long: Long): WritableBuffer = {
    buffer ++= bytebuffer(8).putLong(long).array
    //index += 8
    this
  }

  def writeDouble(double: Double): WritableBuffer = {
    buffer ++= bytebuffer(8).putDouble(double).array
    //index += 8
    this
  }
}
