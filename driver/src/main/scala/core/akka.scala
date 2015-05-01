package reactivemongo.core

import reactivemongo.bson.buffer.{WritableBuffer, ReadableBuffer}

/**
 * Created by sh1ng on 29/04/15.
 */
class AkkaReadableBuffer extends ReadableBuffer {
  /** Returns the current read index of this buffer. */
  override def index: Int = ???

  /** Reads a `Long` from this buffer. */
  override def readLong(): Long = ???

  override def index_=(i: Int): Unit = ???

  /** Reads a `Byte` from this buffer. */
  override def readByte(): Byte = ???

  /** Returns the number of readable remaining bytes of this buffer. */
  override def readable(): Int = ???

  override def size: Int = ???

  /** Sets the read index to `index + n` (in other words, skips `n` bytes). */
  override def discard(n: Int): Unit = ???

  /** Reads an `Int` from this buffer. */
  override def readInt(): Int = ???

  /** Fills the given array with the bytes read from this buffer. */
  override def readBytes(bytes: Array[Byte]): Unit = ???

  override def toWritableBuffer: WritableBuffer = ???

  /**
   * Returns a new instance of ReadableBuffer which starts at the current index and contains `n` bytes.
   *
   * This method does not update the read index of the original buffer.
   */
  override def slice(n: Int): ReadableBuffer = ???

  /** Reads a `Double` from this buffer. */
  override def readDouble(): Double = ???
}
