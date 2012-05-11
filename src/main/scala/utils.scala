package org.asyncmongo.utils

import org.jboss.netty.channel._
import org.jboss.netty.buffer._

import org.asyncmongo.protocol.ChannelBufferWritable

case class RichBuffer(buffer: ChannelBuffer) {
  def writeUTF8(s: String) {
    buffer writeBytes (s.getBytes("UTF-8"))
    buffer writeByte 0
  }
  def write(writable: ChannelBufferWritable) {
    writable writeTo buffer
  }
  def readUTF8 :String = {
    val bytes = new Array[Byte](buffer.readInt)
    buffer.readBytes(bytes)
    new String(bytes, "UTF-8")
  }
  def readArray(n: Int) :Array[Byte] = {
    val bytes = new Array[Byte](buffer.readInt)
    buffer.readBytes(bytes)
    bytes
  }

  import scala.collection.mutable.ArrayBuffer
  @scala.annotation.tailrec
  private def readCString(array: ArrayBuffer[Byte]) :String = {
    val byte = buffer.readByte
    if(byte == 0x00)
      new String(array.toArray, "UTF-8")
    else readCString(array += byte)
  }
  def readCString :String = readCString(new ArrayBuffer[Byte](16))
}

object RichBuffer {
  implicit def channelBufferToExtendedBuffer(buffer: ChannelBuffer) = RichBuffer(buffer)
}

object BufferAccessors {
  import RichBuffer._
  
  sealed trait BufferInteroperable[T] {
    def apply(buffer: ChannelBuffer, t: T) :Unit
  }
  
  implicit object IntChannelInteroperable extends BufferInteroperable[Int] {
    def apply(buffer: ChannelBuffer, i: Int) = buffer writeInt i
  }
  
  implicit object LongChannelInteroperable extends BufferInteroperable[Long] {
    def apply(buffer: ChannelBuffer, l: Long) = buffer writeLong l
  }
  
  implicit object StringChannelInteroperable extends BufferInteroperable[String] {
    def apply(buffer: ChannelBuffer, s: String) = buffer writeUTF8(s)
  }

  def writeTupleToBuffer2[A, B](t: (A, B))(buffer: ChannelBuffer)(implicit i1: BufferInteroperable[A], i2: BufferInteroperable[B]): Unit = {
    i1(buffer, t._1)
    i2(buffer, t._2)
  }

  def writeTupleToBuffer3[A, B, C](t: (A, B, C))(buffer: ChannelBuffer)(implicit i1: BufferInteroperable[A], i2: BufferInteroperable[B], i3: BufferInteroperable[C]): Unit = {
    i1(buffer, t._1)
    i2(buffer, t._2)
    i3(buffer, t._3)
  }

  def writeTupleToBuffer4[A, B, C, D](t: (A, B, C, D))(buffer: ChannelBuffer)(implicit i1: BufferInteroperable[A], i2: BufferInteroperable[B], i3: BufferInteroperable[C], i4: BufferInteroperable[D]): Unit = {
    i1(buffer, t._1)
    i2(buffer, t._2)
    i3(buffer, t._3)
    i4(buffer, t._4)
  }
}