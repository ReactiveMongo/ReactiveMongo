package reactivemongo.core

import java.nio.ByteOrder

import akka.util.{ByteStringBuilder, ByteString}
import reactivemongo.bson.buffer.{WritableBuffer, ReadableBuffer}

/**
 * Created by sh1ng on 29/04/15.
 */
class AkkaReadableBuffer(buffer: ByteString) extends ReadableBuffer {

  val byteBuffer = buffer.toByteBuffer.order(ByteOrder.LITTLE_ENDIAN)


  /** Returns the current read index of this buffer. */
  override def index: Int = byteBuffer.arrayOffset()

  /** Reads a `Long` from this buffer. */
  override def readLong(): Long = byteBuffer.getLong()

  override def index_=(i: Int): Unit = byteBuffer.position(i)

  /** Reads a `Byte` from this buffer. */
  override def readByte(): Byte = byteBuffer.get()

  /** Returns the number of readable remaining bytes of this buffer. */
  override def readable(): Int = byteBuffer.remaining()

  override def size: Int = buffer.size

  /** Sets the read index to `index + n` (in other words, skips `n` bytes). */
  override def discard(n: Int): Unit = byteBuffer.position(byteBuffer.position() + n)

  /** Reads an `Int` from this buffer. */
  override def readInt(): Int = byteBuffer.getInt()

  /** Fills the given array with the bytes read from this buffer. */
  override def readBytes(bytes: Array[Byte]): Unit = byteBuffer.get(bytes)

  /**
   * Returns a new instance of ReadableBuffer which starts at the current index and contains `n` bytes.
   *
   * This method does not update the read index of the original buffer.
   */
  override def slice(n: Int): ReadableBuffer = new AkkaReadableBuffer(buffer.slice(index, index + n))

  /** Reads a `Double` from this buffer. */
  override def readDouble(): Double = byteBuffer.getDouble()
}
