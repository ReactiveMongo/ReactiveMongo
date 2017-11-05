package reactivemongo.bson.buffer

import scala.collection.mutable.ArrayBuffer

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
   * Returns a new instance of `ReadableBuffer`,
   * which starts at the current index and contains `n` bytes.
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
   * @param length the length of the newly created array.
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
