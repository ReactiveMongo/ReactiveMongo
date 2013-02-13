package reactivemongo.bson

import scala.collection.mutable.ArrayBuffer

trait WritableBuffer {
  def writerIndex: Int

  def setInt(index: Int, value: Int)

  def writeBytes(array: Array[Byte]): WritableBuffer

  def writeByte(byte: Byte): WritableBuffer

  def writeInt(int: Int): WritableBuffer

  def writeLong(long: Long): WritableBuffer

  def writeDouble(double: Double): WritableBuffer

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

trait ReadableBuffer {
  def index: Int

  def discard(n: Int): Unit

  def readBytes(bytes: Array[Byte]): Unit

  def readByte(): Byte

  def readInt(): Int

  def readLong(): Long

  def readDouble(): Double

  def readable(): Int

  def slice(n: Int): ReadableBuffer

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
    if (byte == 0x00)
      new String(array.toArray, "UTF-8")
    else readCString(array += byte)
  }
}

trait BSONBuffer extends ReadableBuffer with WritableBuffer

import java.nio.ByteBuffer
import java.nio.ByteOrder._

class ArrayReadableBuffer private(bytebuffer: ByteBuffer) extends ReadableBuffer {
  bytebuffer.order(LITTLE_ENDIAN)

  def size = bytebuffer.limit()

  def index = bytebuffer.position()

  def discard(n: Int) = {
    println(s"discarding $n bytes at position ${bytebuffer.position()} (capacity is ${bytebuffer.capacity()})")
    bytebuffer.position(bytebuffer.position() + n - 1)
  }

  def slice(n: Int) = {
    val nb = bytebuffer.slice()
    println(s"slice: ${bytebuffer.position()} + $n/${nb.capacity()} (${nb.limit()})")
    nb.limit(n)
    new ArrayReadableBuffer(nb)
  }

  def readBytes(array: Array[Byte]): Unit = {
    bytebuffer.get(array)
  }

  def readByte() = bytebuffer.get()

  def readInt() = bytebuffer.getInt()

  def readLong() = bytebuffer.getLong()

  def readDouble() = bytebuffer.getDouble()

  def readable() = bytebuffer.remaining()
}

object ArrayReadableBuffer {
  def apply(array: Array[Byte]) = new ArrayReadableBuffer(ByteBuffer.wrap(array))
}

class ArrayBSONBuffer extends WritableBuffer {
  var writerIndex = 0

  def bytebuffer(size: Int) = {
    val b = ByteBuffer.allocate(size)
    b.order(java.nio.ByteOrder.LITTLE_ENDIAN)
    b
  }

  val buffer = new ArrayBuffer[Byte]()
  def array = buffer.toArray

  def setInt(index: Int, value: Int) = {
    val array = bytebuffer(4).putInt(value).array
    //println("SETINT = " + java.util.Arrays.toString(array))
    buffer.update(index, array(0))
    buffer.update(index + 1, array(1))
    buffer.update(index + 2, array(2))
    buffer.update(index + 3, array(3))
  }

  def writeBytes(array: Array[Byte]): WritableBuffer = {
    buffer ++= array
    writerIndex += array.length
    this
  }

  def writeByte(byte: Byte): WritableBuffer = {
    buffer += byte
    writerIndex += 1
    this
  }

  def writeInt(int: Int): WritableBuffer = {
    val array = bytebuffer(4).putInt(int).array
    //println(java.util.Arrays.toString(array))
    buffer ++= array
    writerIndex += 4
    this
  }

  def writeLong(long: Long): WritableBuffer = {
    buffer ++= bytebuffer(8).putLong(long).array
    writerIndex += 8
    this
  }

  def writeDouble(double: Double): WritableBuffer = {
    buffer ++= bytebuffer(8).putDouble(double).array
    writerIndex += 8
    this
  }
}