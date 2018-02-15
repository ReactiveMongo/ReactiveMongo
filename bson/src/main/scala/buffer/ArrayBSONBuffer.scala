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

import scala.collection.mutable.ArrayBuffer

@deprecated("Unused", "0.13.0")
trait BSONBuffer extends ReadableBuffer with WritableBuffer

import java.nio.ByteBuffer

/** An array-backed writable buffer. */
class ArrayBSONBuffer protected[buffer] (
  protected val buffer: ArrayBuffer[Byte]) extends WritableBuffer {
  def index = buffer.length // useless

  def bytebuffer(size: Int): ByteBuffer = {
    val b = ByteBuffer.allocate(size)
    b.order(java.nio.ByteOrder.LITTLE_ENDIAN)
    b
  }

  def this() = this(new ArrayBuffer[Byte]())

  /** Returns an array containing all the data that were put in this buffer. */
  def array = buffer.toArray

  def setInt(index: Int, value: Int): this.type = {
    val array = bytebuffer(4).putInt(value).array

    buffer.update(index, array(0))
    buffer.update(index + 1, array(1))
    buffer.update(index + 2, array(2))
    buffer.update(index + 3, array(3))

    this
  }

  def toReadableBuffer = ArrayReadableBuffer(array)

  def writeBytes(array: Array[Byte]): this.type = {
    buffer ++= array
    //index += array.length
    this
  }

  def writeByte(byte: Byte): WritableBuffer = {
    buffer += byte
    //index += 1
    this
  }

  def writeInt(int: Int): WritableBuffer = {
    val array = bytebuffer(4).putInt(int).array
    buffer ++= array
    //index += 4
    this
  }

  def writeLong(long: Long): WritableBuffer = {
    buffer ++= bytebuffer(8).putLong(long).array
    //index += 8
    this
  }

  def writeDouble(double: Double): WritableBuffer = {
    buffer ++= bytebuffer(8).putDouble(double).array
    //index += 8
    this
  }
}
