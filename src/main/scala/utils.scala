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
  /** Write a UTF-8 encoded C-Style String. */
  def writeCString(s: String): ChannelBuffer = {
    val bytes = s.getBytes("utf-8")
    buffer writeBytes bytes
    buffer writeByte 0
    buffer
  }

  /** Write a UTF-8 encoded String. */
  def writeString(s: String): ChannelBuffer = {
    val bytes = s.getBytes("utf-8")
    buffer writeInt (bytes.size + 1)
    buffer writeBytes bytes
    buffer writeByte 0
    buffer
  }
  /** Write the contents of the given [[org.asyncmongo.protocol.ChannelBufferWritable]]. */
  def write(writable: ChannelBufferWritable) {
    writable writeTo buffer
  }
  /** Reads a UTF-8 String. */
  def readString() :String = {
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

  /** Reads a UTF-8 C-Style String. */
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