package reactivemongo.core.protocol.buffer

import reactivemongo.io.netty.buffer.ByteBuf

/**
 * Something that can be written into a [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]].
 */
private[reactivemongo] trait ChannelBufferWritable {

  /** Writes this instance into the given [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]]. */
  def writeTo: ByteBuf => Unit

  /** Size of the content that would be written. */
  def size: Int
}

/**
 * A constructor of T instances from a [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]].
 *
 * @tparam T type which instances can be constructed with this.
 */
private[reactivemongo] trait ChannelBufferReadable[T] {

  /** Makes an instance of T from the data from the given buffer. */
  def readFrom(buffer: ByteBuf): T

  /** @see readFrom */
  final def apply(buffer: ByteBuf): T = readFrom(buffer)
}

/**
 * Typeclass for types that can be written into a `ByteBuffer`,
 * via writeTupleToBufferN methods.
 *
 * @tparam T type to be written via `writeTupleToBufferN(...)` methods.
 */
private[protocol] sealed trait BufferInteroperable[T] {
  def apply(buffer: ByteBuf, t: T): Unit
}

/**
 * Helper methods to work with [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]].
 *
 * @define writeTupleDescription Writes the given tuple into the given [[http://static.netty.io/4.0/api/io/netty/buffer/ByteBuf.html ByteBuf]]
 * @define buffer_interop_tparam type that have an implicit typeclass [[reactivemongo.core.protocol.buffer.BufferInteroperable]]
 */
object `package` {

  private[protocol] implicit object IntChannelInteroperable
      extends BufferInteroperable[Int] {
    def apply(buffer: ByteBuf, i: Int) = { buffer `writeIntLE` i; () }
  }

  private[protocol] implicit object LongChannelInteroperable
      extends BufferInteroperable[Long] {
    def apply(buffer: ByteBuf, l: Long) = { buffer `writeLongLE` l; () }
  }

  private[protocol] implicit object StringChannelInteroperable
      extends BufferInteroperable[String] {

    private def writeCString(buffer: ByteBuf, s: String): ByteBuf = {
      val bytes = s.getBytes("utf-8")

      buffer `writeBytes` bytes
      buffer `writeByte` 0
      buffer
    }

    def apply(buffer: ByteBuf, s: String) = { writeCString(buffer, s); () }
  }

  // ---

  /**
   * $writeTupleDescription.
   *
   * @tparam A $buffer_interop_tparam
   * @tparam B $buffer_interop_tparam
   */
  def writeTupleToBuffer2[A, B](
      t: (A, B)
    )(buffer: ByteBuf
    )(implicit
      i1: BufferInteroperable[A],
      i2: BufferInteroperable[B]
    ): Unit = {
    i1(buffer, t._1)
    i2(buffer, t._2)
  }

  /**
   * $writeTupleDescription.
   *
   * @tparam A $buffer_interop_tparam
   * @tparam B $buffer_interop_tparam
   * @tparam C $buffer_interop_tparam
   */
  private[protocol] def writeTupleToBuffer3[A, B, C](
      t: (A, B, C)
    )(buffer: ByteBuf
    )(implicit
      i1: BufferInteroperable[A],
      i2: BufferInteroperable[B],
      i3: BufferInteroperable[C]
    ): Unit = {
    i1(buffer, t._1)
    i2(buffer, t._2)
    i3(buffer, t._3)
  }

  /**
   * $writeTupleDescription.
   *
   * @tparam A $buffer_interop_tparam
   * @tparam B $buffer_interop_tparam
   * @tparam C $buffer_interop_tparam
   * @tparam D $buffer_interop_tparam
   */
  private[protocol] def writeTupleToBuffer4[A, B, C, D](
      t: (A, B, C, D)
    )(buffer: ByteBuf
    )(implicit
      i1: BufferInteroperable[A],
      i2: BufferInteroperable[B],
      i3: BufferInteroperable[C],
      i4: BufferInteroperable[D]
    ): Unit = {
    i1(buffer, t._1)
    i2(buffer, t._2)
    i3(buffer, t._3)
    i4(buffer, t._4)
  }
}
