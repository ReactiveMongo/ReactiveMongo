package reactivemongo.core.protocol

import reactivemongo.io.netty.buffer.ByteBuf

/**
 * Something that can be written into a [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]].
 */
trait ChannelBufferWritable {
  /** Write this instance into the given [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]]. */
  def writeTo: ByteBuf => Unit

  /** Size of the content that would be written. */
  def size: Int
}

/**
 * A constructor of T instances from a [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]].
 *
 * @tparam T type which instances can be constructed with this.
 */
trait ChannelBufferReadable[T] {
  /** Makes an instance of T from the data from the given buffer. */
  def readFrom(buffer: ByteBuf): T

  /** @see readFrom */
  def apply(buffer: ByteBuf): T = readFrom(buffer)
}
