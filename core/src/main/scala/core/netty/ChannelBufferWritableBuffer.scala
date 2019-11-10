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

import reactivemongo.io.netty.buffer.{ ByteBuf, Unpooled }

import reactivemongo.bson.BSONDocument
import reactivemongo.bson.buffer.{ ReadableBuffer, WritableBuffer }

@deprecated("Internal: will be private", "0.19.1")
class ChannelBufferWritableBuffer(
  val buffer: ByteBuf = Unpooled.buffer(32)) extends WritableBuffer {

  def index = buffer.writerIndex

  def setInt(index: Int, value: Int) = {
    buffer.setIntLE(index, value)
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

  def writeBytes(buf: ByteBuf) = {
    val readable = buf.slice()
    buffer.writeBytes(readable)
    this
  }

  def writeByte(byte: Byte): WritableBuffer = {
    buffer writeByte byte.toInt
    this
  }

  def writeInt(int: Int): WritableBuffer = {
    buffer writeIntLE int
    this
  }

  def writeLong(long: Long): WritableBuffer = {
    buffer writeLongLE long
    this
  }

  def writeDouble(double: Double): WritableBuffer = {
    buffer writeDoubleLE double
    this
  }
}

object ChannelBufferWritableBuffer {
  /** Returns a new writable channel buffer. */
  def apply() = new ChannelBufferWritableBuffer()

  /** Returns a new channel buffer with the give `document` written on. */
  private[reactivemongo] def single(document: BSONDocument): ByteBuf = {
    val buffer = ChannelBufferWritableBuffer()
    BSONDocument.write(document, buffer)
    buffer.buffer
  }
}
