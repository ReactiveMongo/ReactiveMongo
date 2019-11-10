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

import reactivemongo.io.netty.buffer.ByteBuf

import reactivemongo.bson.BSONDocument
import reactivemongo.bson.buffer.ReadableBuffer

@deprecated("Internal: will be private", "0.19.1")
class ChannelBufferReadableBuffer(
  protected[netty] val buffer: ByteBuf) extends ReadableBuffer {

  def size = buffer.capacity()

  def index = buffer.readerIndex()

  def index_=(i: Int) = { buffer.readerIndex(i); () }

  def discard(n: Int) = { buffer.readerIndex(buffer.readerIndex + n); () }

  def slice(n: Int) =
    new ChannelBufferReadableBuffer(buffer.slice(buffer.readerIndex, n))

  def readBytes(array: Array[Byte]): Unit = { buffer.readBytes(array); () }

  def readByte() = buffer.readByte()

  def readInt() = buffer.readIntLE()

  def readLong() = buffer.readLongLE()

  def readDouble() = buffer.readDoubleLE()

  def readable() = buffer.readableBytes()

  def toWritableBuffer: ChannelBufferWritableBuffer = {
    val buf = new ChannelBufferWritableBuffer
    buf.writeBytes(buffer)
  }

  def duplicate() = new ChannelBufferReadableBuffer(buffer.duplicate())
}

object ChannelBufferReadableBuffer {
  def apply(buffer: ByteBuf) = new ChannelBufferReadableBuffer(buffer)

  def document(buffer: ByteBuf): BSONDocument =
    BSONDocument.read(ChannelBufferReadableBuffer(buffer))

}
