package reactivemongo.core.protocol

import reactivemongo.io.netty.buffer.ByteBuf

/**
 * Helper methods to write tuples of supported types into a [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]].
 */
private[protocol] object BufferAccessors { // TODO: Refactor or remove
  /**
   * Typeclass for types that can be written into a `ByteBuffer`,
   * via writeTupleToBufferN methods.
   *
   * @tparam T type to be written via BufferAccessors.writeTupleToBufferN(...) methods.
   */
  sealed trait BufferInteroperable[T] {
    def apply(buffer: ByteBuf, t: T): Unit
  }

  implicit object IntChannelInteroperable extends BufferInteroperable[Int] {
    def apply(buffer: ByteBuf, i: Int) = { buffer writeIntLE i; () }
  }

  implicit object LongChannelInteroperable extends BufferInteroperable[Long] {
    def apply(buffer: ByteBuf, l: Long) = { buffer writeLongLE l; () }
  }

  implicit object StringChannelInteroperable extends BufferInteroperable[String] {
    private def writeCString(buffer: ByteBuf, s: String): ByteBuf = {
      val bytes = s.getBytes("utf-8")

      buffer writeBytes bytes
      buffer writeByte 0
      buffer
    }

    def apply(buffer: ByteBuf, s: String) = { writeCString(buffer, s); () }
  }

  /**
   * Write the given tuple into the given [[http://static.netty.io/4.0/api/io/netty/buffer/ByteBuf.html ByteBuf]].
   *
   * @tparam A type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   * @tparam B type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   */
  def writeTupleToBuffer2[A, B](t: (A, B))(buffer: ByteBuf)(implicit i1: BufferInteroperable[A], i2: BufferInteroperable[B]): Unit = {
    i1(buffer, t._1)
    i2(buffer, t._2)
  }

  /**
   * Write the given tuple into the given [[http://static.netty.io/4.0/api/io/netty/buffer/ByteBuf.html ByteBuf]].
   *
   * @tparam A type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   * @tparam B type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   * @tparam C type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   */
  def writeTupleToBuffer3[A, B, C](t: (A, B, C))(buffer: ByteBuf)(implicit i1: BufferInteroperable[A], i2: BufferInteroperable[B], i3: BufferInteroperable[C]): Unit = {
    i1(buffer, t._1)
    i2(buffer, t._2)
    i3(buffer, t._3)
  }

  /**
   * Write the given tuple into the given [[http://static.netty.io/4.0/api/io/netty/buffer/ByteBuf.html ByteBuf]].
   *
   * @tparam A type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   * @tparam B type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   * @tparam C type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   * @tparam D type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   */
  def writeTupleToBuffer4[A, B, C, D](t: (A, B, C, D))(buffer: ByteBuf)(implicit i1: BufferInteroperable[A], i2: BufferInteroperable[B], i3: BufferInteroperable[C], i4: BufferInteroperable[D]): Unit = {
    i1(buffer, t._1)
    i2(buffer, t._2)
    i3(buffer, t._3)
    i4(buffer, t._4)
  }
}
