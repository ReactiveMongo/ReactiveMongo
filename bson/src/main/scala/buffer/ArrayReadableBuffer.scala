package reactivemongo.bson.buffer

import java.nio.{ ByteBuffer, ByteOrder }

/** An array-backed readable buffer. */
case class ArrayReadableBuffer private (
  private[reactivemongo] val bytebuffer: ByteBuffer) extends ReadableBuffer {

  bytebuffer.order(ByteOrder.LITTLE_ENDIAN)

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
  def apply(array: Array[Byte]): ArrayReadableBuffer =
    new ArrayReadableBuffer(ByteBuffer.wrap(array))
}
