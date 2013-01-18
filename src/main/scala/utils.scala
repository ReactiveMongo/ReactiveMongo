package reactivemongo.utils

import scala.concurrent._
import scala.concurrent.duration._

object `package` {
  /** Concats two array - fast way */
  def concat[T](a1: Array[T], a2: Array[T])(implicit m: Manifest[T]) :Array[T] = {
    var i, j = 0
    val result = new Array[T](a1.length + a2.length)
    while(i < a1.length) {
      result(i) = a1(i)
      i = i + 1
    }
    while(j < a2.length) {
      result(i + j) = a2(j)
      j = j + 1
    }
    result
  }

  /** Makes an option of the value matching the condition. */
  def option[T](cond: => Boolean, value: => T) :Option[T] = (if(cond) Some(value) else None)
}

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

  /** Computes the MD5 hash of the given String. */
  def md5(s: String) = java.security.MessageDigest.getInstance("MD5").digest(s.getBytes)

  /** Computes the MD5 hash of the given Array of Bytes. */
  def md5(array: Array[Byte]) = java.security.MessageDigest.getInstance("MD5").digest(array)

  /** Computes the MD5 hash of the given String and turns it into a hexadecimal String representation. */
  def md5Hex(s: String) :String = hex2Str(md5(s))
}

case class LazyLogger(logger: org.slf4j.Logger) {
  def trace(s: => String) { if(logger.isTraceEnabled) logger.trace(s) }
  def debug(s: => String) { if(logger.isDebugEnabled) logger.debug(s) }
  def info(s: => String) { if(logger.isInfoEnabled) logger.info(s) }
  def warn(s: => String) { if(logger.isWarnEnabled) logger.warn(s) }
  def error(s: => String) { if(logger.isErrorEnabled) logger.error(s) }
}

object LazyLogger {
  def apply(logger: String) :LazyLogger = LazyLogger(org.slf4j.LoggerFactory.getLogger("ReadFileEntry"))
}

case class EitherMappableFuture[A](future: Future[A]) {
  def mapEither[E <: Throwable, B](f: A => Either[E, B])(implicit ec: ExecutionContext) = {
    future.flatMap(
      f(_) match {
        case Left(e) => Future.failed(e)
        case Right(b) => Future.successful(b)
      }
    )
  }
}
object EitherMappableFuture {
  implicit def futureToEitherMappable[A](future: Future[A]) :EitherMappableFuture[A] = EitherMappableFuture(future)
}

object ExtendedFutures {
  import akka.actor.{ActorSystem, Scheduler}

  // better way to this?
  def DelayedFuture(millis: Long, system: ActorSystem) :Future[Unit] = {
    implicit val ec = system.dispatcher
    val promise = Promise[Unit]()
    system.scheduler.scheduleOnce(Duration.apply(millis, "millis"))(promise.success())
    promise.future
  }
}

package buffers {
  import org.jboss.netty.channel._
  import org.jboss.netty.buffer._
  import reactivemongo.core.protocol.ChannelBufferWritable
  import scala.collection.mutable.ArrayBuffer

  object `package` {
    /** Extends a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] with handy functions for the Mongo Wire Protocol. */
    implicit class RichBuffer(buffer: ChannelBuffer) {
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

      /** Write the contents of the given [[reactivemongo.core.protocol.ChannelBufferWritable]]. */
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
  }

  case class BufferSequence(private val head: ChannelBuffer, private val tail: ChannelBuffer*) {
    def merged :ChannelBuffer = mergedBuffer.duplicate()

    private lazy val mergedBuffer = {
      val bufs = (head +: tail) // .map(cb =>cb.duplicate())
      ChannelBuffers.wrappedBuffer(bufs: _*)
      // bufs.tail.foldLeft(bufs.head) { (result, buf) =>
      //   result writeBytes buf
      //   result
      // }
    }
  }

  object BufferSequence {
    val empty = BufferSequence(new LittleEndianHeapChannelBuffer(0))
  }
}