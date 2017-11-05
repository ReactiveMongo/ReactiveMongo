package reactivemongo.bson.buffer

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
