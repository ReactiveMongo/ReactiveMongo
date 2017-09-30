package reactivemongo.api

import scala.language.higherKinds

import scala.util.Try

import reactivemongo.bson.buffer.{ ReadableBuffer, WritableBuffer }

trait SerializationPack { self: Singleton =>
  type Value
  type ElementProducer
  type Document <: Value
  type Writer[A]
  type Reader[A]
  type NarrowValueReader[A]
  private[reactivemongo] type WidenValueReader[A]

  def IdentityWriter: Writer[Document]
  def IdentityReader: Reader[Document]

  def serialize[A](a: A, writer: Writer[A]): Document
  def deserialize[A](document: Document, reader: Reader[A]): A

  def writeToBuffer(buffer: WritableBuffer, document: Document): WritableBuffer
  def readFromBuffer(buffer: ReadableBuffer): Document

  def serializeAndWrite[A](buffer: WritableBuffer, document: A, writer: Writer[A]): WritableBuffer = writeToBuffer(buffer, serialize(document, writer))

  def readAndDeserialize[A](buffer: ReadableBuffer, reader: Reader[A]): A =
    deserialize(readFromBuffer(buffer), reader)

  import reactivemongo.core.protocol.Response
  import reactivemongo.core.netty.ChannelBufferReadableBuffer

  final def readAndDeserialize[A](response: Response, reader: Reader[A]): A = {
    val buf = response.documents
    val channelBuf = ChannelBufferReadableBuffer(buf.readBytes(buf.getInt(buf.readerIndex)))
    readAndDeserialize(channelBuf, reader)
  }

  def writer[A](f: A => Document): Writer[A]

  def isEmpty(document: Document): Boolean

  def widenReader[T](r: NarrowValueReader[T]): WidenValueReader[T]

  def readValue[A](value: Value, reader: WidenValueReader[A]): Try[A]

  private[reactivemongo] def bsonSize(value: Value): Int = -1
  // TODO: Remove the default value after release

  private[reactivemongo] def newBuilder: SerializationPack.Builder[self.type] = null // TODO: Remove the default value after release
}

object SerializationPack {
  /** A builder for serialization simple values (useful for the commands) */
  private[reactivemongo] trait Builder[P <: SerializationPack with Singleton] {
    protected val pack: P

    /** Returns a new document from a sequence of element producers. */
    def document(elements: Seq[pack.ElementProducer]): pack.Document

    /** Returns a new non empty array of values */
    def array(value: pack.Value, values: Seq[pack.Value]): pack.Value

    /**
     * Returns a producer of element, for the given `name` and `value`.
     *
     * @param name the element name
     * @param value the element value
     */
    def elementProducer(name: String, value: pack.Value): pack.ElementProducer

    /** Returns a boolean as a serialized value. */
    def boolean(b: Boolean): pack.Value

    /** Returns an integer as a serialized value. */
    def int(i: Int): pack.Value

    /** Returns an long as a serialized value. */
    def long(l: Long): pack.Value

    /** Returns an double as a serialized value. */
    def double(d: Double): pack.Value

    /** Returns an string as a serialized value. */
    def string(s: String): pack.Value
  }
}
