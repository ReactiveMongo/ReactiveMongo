/*
 * Copyright 2013 Stephane Godbillon (@sgodbillon)
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
package reactivemongo.bson.buffer

import reactivemongo.io.netty.buffer.{ ByteBuf, Unpooled }

import java.nio.ByteBuffer

/** An array-backed writable buffer. */
class ArrayBSONBuffer protected[buffer] (
  private[reactivemongo] val buffer: ByteBuf) extends WritableBuffer {

  def index: Int = buffer.readableBytes() // useless

  def bytebuffer(size: Int): ByteBuffer = {
    val b = ByteBuffer.allocate(size)
    b.order(java.nio.ByteOrder.LITTLE_ENDIAN)
    b
  }

  def this() = this(Unpooled.buffer(96))

  /** Returns an array containing all the data that were put in this buffer. */
  def array = {
    val bytes = Array.ofDim[Byte](index)
    buffer.getBytes(0, bytes, 0, index)
    bytes
  }

  def setInt(index: Int, value: Int): this.type = {
    buffer.setIntLE(index, value)
    this
  }

  def toReadableBuffer = ArrayReadableBuffer(array)

  def writeBytes(array: Array[Byte]): this.type = {
    buffer.writeBytes(array)
    this
  }

  override def writeBytes(buf: ReadableBuffer): this.type = buf match {
    case in: ArrayReadableBuffer => {
      buffer.writeBytes(in.bytebuffer)
      this
    }

    case _ => super.writeBytes(buf)
  }

  def writeByte(byte: Byte): WritableBuffer = {
    buffer.writeByte(byte.toInt)
    //index += 1
    this
  }

  def writeInt(int: Int): WritableBuffer = {
    buffer.writeIntLE(int)
    //index += 4
    this
  }

  def writeLong(long: Long): WritableBuffer = {
    buffer.writeLongLE(long)
    //index += 8
    this
  }

  def writeDouble(double: Double): WritableBuffer = {
    buffer.writeDoubleLE(double)
    //index += 8
    this
  }
}
