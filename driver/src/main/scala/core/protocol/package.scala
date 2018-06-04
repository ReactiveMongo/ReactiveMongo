package reactivemongo.core.protocol

import shaded.netty.buffer.ByteBuf

object `package` {
  @deprecated("Will be removed", "0.12.0")
  implicit class RichBuffer(val buffer: ByteBuf) extends AnyVal {
    import scala.collection.mutable.ArrayBuffer

    /** Write a UTF-8 encoded C-Style String. */
    def writeCString(s: String): ByteBuf = {
      val bytes = s.getBytes("utf-8")

      buffer writeBytes bytes
      buffer writeByte 0

      buffer
    }

    /** Write a UTF-8 encoded String. */
    def writeString(s: String): ByteBuf = {
      val bytes = s.getBytes("utf-8")

      buffer.writeIntLE(bytes.size + 1)
      buffer writeBytes bytes
      buffer writeByte 0
      buffer
    }

    /** Write the contents of the given [[reactivemongo.core.protocol.ChannelBufferWritable]]. */
    def write(writable: ChannelBufferWritable) = writable writeTo buffer

    /** Reads a UTF-8 String. */
    def readString(): String = {
      val bytes = new Array[Byte](buffer.readIntLE - 1)

      buffer.readBytes(bytes)
      buffer.readByte

      new String(bytes, "UTF-8")
    }

    /**
     * Reads an array of Byte of the given length.
     *
     * @param length Length of the newly created array.
     */
    def readArray(length: Int): Array[Byte] = {
      val bytes = new Array[Byte](length)

      buffer.readBytes(bytes)

      bytes
    }

    /** Reads a UTF-8 C-Style String. */
    def readCString(): String = {
      @annotation.tailrec
      def readCString(array: ArrayBuffer[Byte]): String = {
        val byte = buffer.readByte
        if (byte == 0x00) new String(array.toArray, "UTF-8")
        else readCString(array += byte)
      }

      readCString(new ArrayBuffer[Byte](16))
    }
  }
}
