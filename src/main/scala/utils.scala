package org.asyncmongo.utils

import org.jboss.netty.channel._
import org.jboss.netty.buffer._

import org.asyncmongo.protocol.ChannelBufferWritable

/** Common functions */
object Converters {
  private val HEX_CHARS :Array[Char] = "0123456789abcdef".toCharArray();

  /** Turns an array of Byte into a String representation in hexadecimal. */
  def hex2Str(bytes: Array[Byte]) :String = {
    val hex = new Array[Char](2 * bytes.length)
    var i = 0
    while(i < bytes.length) {
      hex(2 * i) = HEX_CHARS((bytes(i) & 0xF0) >>> 4)
      hex(2 * i + 1) = HEX_CHARS(bytes(i) & 0x0F)
      i = i + 1
    }
    new String(hex)
  }

  /** Turns a hexadecimal String into an array of Byte. */
  def str2Hex(str: String) :Array[Byte] = {
    val bytes = new Array[Byte](str.length / 2)
    var i = 0
    while(i < bytes.length) {
      bytes(i) = Integer.parseInt(str.substring(2*i, 2*i+2), 16).toByte
      i += 1
    }
    bytes
  }

  /** Computes the MD5 hash of the given String and turns it into a hexadecimal String representation. */
  def md5Hex(s: String) :String =
    hex2Str(java.security.MessageDigest.getInstance("MD5").digest(s.getBytes))
}

/** Extends a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] with handy functions for the Mongo Wire Protocol. */
case class RichBuffer(buffer: ChannelBuffer) {
  /** Write an UTF-8 encoded String. */
  def writeUTF8(s: String) {
    buffer writeBytes (s.getBytes("UTF-8"))
    buffer writeByte 0
  }
  /** Write the contents of the given [[org.asyncmongo.protocol.ChannelBufferWritable]]. */
  def write(writable: ChannelBufferWritable) {
    writable writeTo buffer
  }
  /** Reads a UTF-8 String. */
  def readUTF8() :String = {
    val bytes = new Array[Byte](buffer.readInt - 1)
    buffer.readBytes(bytes)
    buffer.readByte
    new String(bytes, "UTF-8")
  }
  /**
   * Reads an array of Byte of the given length.
   *
   * @param length Length of the newly created array.
   */
  def readArray(length: Int) :Array[Byte] = {
    val bytes = new Array[Byte](length)
    buffer.readBytes(bytes)
    bytes
  }

  import scala.collection.mutable.ArrayBuffer

  /** Reads a CString. */
  def readCString() :String = readCString(new ArrayBuffer[Byte](16))

  @scala.annotation.tailrec
  private def readCString(array: ArrayBuffer[Byte]) :String = {
    val byte = buffer.readByte
    if(byte == 0x00)
      new String(array.toArray, "UTF-8")
    else readCString(array += byte)
  }
}

object RichBuffer {
  /** Implicit conversion between a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] and [[org.asyncmongo.utils.RichBuffer]]. */
  implicit def channelBufferToExtendedBuffer(buffer: ChannelBuffer) = RichBuffer(buffer)
}

/**
 * Helper methods to write tuples of supported types into a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
 */
object BufferAccessors {
  import RichBuffer._

  /**
   * Typeclass for types that can be written into a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]]
   * via writeTupleToBufferN methods.
   *
   * @tparam T type to be written via BufferAccessors.writeTupleToBufferN(...) methods.
   */
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

  /**
   * Write the given tuple into the given [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
   *
   * @tparam A type that have an implicit typeclass [[org.asyncmongo.utils.BufferAccessors.BufferInteroperable]].
   * @tparam B type that have an implicit typeclass [[org.asyncmongo.utils.BufferAccessors.BufferInteroperable]].
   */
  def writeTupleToBuffer2[A, B](t: (A, B))(buffer: ChannelBuffer)(implicit i1: BufferInteroperable[A], i2: BufferInteroperable[B]): Unit = {
    i1(buffer, t._1)
    i2(buffer, t._2)
  }

  /**
   * Write the given tuple into the given [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
   *
   * @tparam A type that have an implicit typeclass [[org.asyncmongo.utils.BufferAccessors.BufferInteroperable]].
   * @tparam B type that have an implicit typeclass [[org.asyncmongo.utils.BufferAccessors.BufferInteroperable]].
   * @tparam C type that have an implicit typeclass [[org.asyncmongo.utils.BufferAccessors.BufferInteroperable]].
   */
  def writeTupleToBuffer3[A, B, C](t: (A, B, C))(buffer: ChannelBuffer)(implicit i1: BufferInteroperable[A], i2: BufferInteroperable[B], i3: BufferInteroperable[C]): Unit = {
    i1(buffer, t._1)
    i2(buffer, t._2)
    i3(buffer, t._3)
  }

  /**
   * Write the given tuple into the given [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
   *
   * @tparam A type that have an implicit typeclass [[org.asyncmongo.utils.BufferAccessors.BufferInteroperable]].
   * @tparam B type that have an implicit typeclass [[org.asyncmongo.utils.BufferAccessors.BufferInteroperable]].
   * @tparam C type that have an implicit typeclass [[org.asyncmongo.utils.BufferAccessors.BufferInteroperable]].
   * @tparam D type that have an implicit typeclass [[org.asyncmongo.utils.BufferAccessors.BufferInteroperable]].
   */
  def writeTupleToBuffer4[A, B, C, D](t: (A, B, C, D))(buffer: ChannelBuffer)(implicit i1: BufferInteroperable[A], i2: BufferInteroperable[B], i3: BufferInteroperable[C], i4: BufferInteroperable[D]): Unit = {
    i1(buffer, t._1)
    i2(buffer, t._2)
    i3(buffer, t._3)
    i4(buffer, t._4)
  }
}