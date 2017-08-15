/*
 * Copyright 2012-2013 Stephane Godbillon (@sgodbillon) and Zenexity
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivemongo.core.netty

import java.nio.ByteOrder.LITTLE_ENDIAN

import shaded.netty.buffer.{
  ChannelBuffer,
  ChannelBuffers,
  LittleEndianHeapChannelBuffer
}

import reactivemongo.bson.BSONDocument
import reactivemongo.bson.buffer.{ ReadableBuffer, WritableBuffer }

class ChannelBufferReadableBuffer(
  protected[netty] val buffer: ChannelBuffer) extends ReadableBuffer {

  def size = buffer.capacity()

  def index = buffer.readerIndex()

  def index_=(i: Int) = buffer.readerIndex(i)

  def discard(n: Int) = buffer.readerIndex(buffer.readerIndex + n)

  def slice(n: Int) =
    new ChannelBufferReadableBuffer(buffer.slice(buffer.readerIndex(), n))

  def readBytes(array: Array[Byte]): Unit = buffer.readBytes(array)

  def readByte() = buffer.readByte()

  def readInt() = buffer.readInt()

  def readLong() = buffer.readLong()

  def readDouble() = buffer.readDouble()

  def readable() = buffer.readableBytes()

  def toWritableBuffer: ChannelBufferWritableBuffer = {
    val buf = new ChannelBufferWritableBuffer
    buf.writeBytes(buffer)
  }

  def duplicate() = new ChannelBufferReadableBuffer(buffer.duplicate())
}

object ChannelBufferReadableBuffer {
  def apply(buffer: ChannelBuffer) = new ChannelBufferReadableBuffer(buffer)

  def document(buffer: ChannelBuffer): BSONDocument =
    BSONDocument.read(ChannelBufferReadableBuffer(buffer))

}

class ChannelBufferWritableBuffer(val buffer: ChannelBuffer = ChannelBuffers.dynamicBuffer(LITTLE_ENDIAN, 32)) extends WritableBuffer {
  def index = buffer.writerIndex

  def setInt(index: Int, value: Int) = {
    buffer.setInt(index, value)
    this
  }

  def toReadableBuffer =
    ChannelBufferReadableBuffer(buffer.duplicate())

  def writeBytes(array: Array[Byte]): this.type = {
    buffer writeBytes array
    this
  }

  override def writeBytes(buf: ReadableBuffer): this.type = buf match {
    case nettyBuffer: ChannelBufferReadableBuffer =>
      val readable = nettyBuffer.buffer.slice()
      buffer.writeBytes(readable)
      this

    case _ => super.writeBytes(buf)
  }

  def writeBytes(buf: ChannelBuffer) = {
    val readable = buf.slice()
    buffer.writeBytes(readable)
    this
  }

  def writeByte(byte: Byte): WritableBuffer = {
    buffer writeByte byte.toInt
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
  /** Returns a new writable channel buffer. */
  def apply() = new ChannelBufferWritableBuffer()

  /** Returns a new channel buffer with the give `document` written on. */
  private[reactivemongo] def single(document: BSONDocument): ChannelBuffer = {
    val buffer = ChannelBufferWritableBuffer()
    BSONDocument.write(document, buffer)
    buffer.buffer
  }
}

case class BufferSequence(
  private val head: ChannelBuffer,
  private val tail: ChannelBuffer*) {
  def merged: ChannelBuffer = mergedBuffer.duplicate()

  private lazy val mergedBuffer =
    ChannelBuffers.wrappedBuffer((head +: tail): _*)
}

object BufferSequence {
  /** Returns an empty buffer sequence. */
  val empty = BufferSequence(new LittleEndianHeapChannelBuffer(0))

  /** Returns a new channel buffer with the give `document` written on. */
  private[reactivemongo] def single(document: BSONDocument): BufferSequence =
    BufferSequence(ChannelBufferWritableBuffer single document)

}
