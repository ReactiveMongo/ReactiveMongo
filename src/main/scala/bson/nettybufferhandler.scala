package reactivemongo.bson.netty

import java.nio.ByteOrder._
import reactivemongo.bson.{ ReadableBuffer, WritableBuffer }
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers

class ChannelBufferReadableBuffer(protected val buffer: ChannelBuffer) extends ReadableBuffer {
  def size = buffer.capacity()

  def index = buffer.readerIndex()

  def discard(n: Int) = {
    println(s"discarding $n bytes at position ${buffer.readerIndex} (capacity is ${buffer.capacity()}, wx is ${buffer.writerIndex()})")
    buffer.readerIndex(buffer.readerIndex + n - 1)
  }

  def slice(n: Int) = {
    //println(s"slice: ${bytebuffer.position()} + $n/${nb.capacity()} (${nb.limit()})")
    val b2 = buffer.slice(buffer.readerIndex(), n)
    new ChannelBufferReadableBuffer(b2)
  }

  def readBytes(array: Array[Byte]): Unit = {
    buffer.readBytes(array)
  }

  def readByte() = buffer.readByte()

  def readInt() = buffer.readInt()

  def readLong() = buffer.readLong()

  def readDouble() = buffer.readDouble()

  def readable() = buffer.readableBytes()
}

object ChannelBufferReadableBuffer {
  def apply(buffer: ChannelBuffer) = new ChannelBufferReadableBuffer(buffer)
}

class ChannelBufferWritableBuffer extends WritableBuffer {
  val buffer = ChannelBuffers.dynamicBuffer(LITTLE_ENDIAN, 32)

  def writerIndex = buffer.writerIndex

  def setInt(index: Int, value: Int) = {
    buffer.setInt(index, value)
  }

  def writeBytes(array: Array[Byte]): WritableBuffer = {
    buffer writeBytes array
    this
  }

  def writeByte(byte: Byte): WritableBuffer = {
    buffer writeByte byte
    this
  }

  def writeInt(int: Int): WritableBuffer = {
    buffer writeInt int
    this
  }

  def writeLong(long: Long): WritableBuffer = {
    buffer writeLong long
    this
  }

  def writeDouble(double: Double): WritableBuffer = {
    buffer writeDouble double
    this
  }
}

object ChannelBufferWritableBuffer {
  def apply() = new ChannelBufferWritableBuffer()
}

