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
}

/** The default serialization pack. */
object BSONSerializationPack extends SerializationPack {
  import reactivemongo.bson._, buffer.DefaultBufferHandler

  type Value = BSONValue
  type ElementProducer = Producer[BSONElement]
  type Document = BSONDocument
  type Writer[A] = BSONDocumentWriter[A]
  type Reader[A] = BSONDocumentReader[A]
  type NarrowValueReader[A] = BSONReader[_ <: BSONValue, A]
  private[reactivemongo] type WidenValueReader[A] = UnsafeBSONReader[A]

  object IdentityReader extends Reader[Document] {
    def read(document: Document): Document = document
  }

  object IdentityWriter extends Writer[Document] {
    def write(document: Document): Document = document
  }

  def serialize[A](a: A, writer: Writer[A]): Document = writer.write(a)

  def deserialize[A](document: Document, reader: Reader[A]): A =
    reader.read(document)

  def writeToBuffer(buffer: WritableBuffer, document: Document): WritableBuffer = DefaultBufferHandler.writeDocument(document, buffer)

  def readFromBuffer(buffer: ReadableBuffer): Document =
    DefaultBufferHandler.readDocument(buffer).get

  def writer[A](f: A => Document): Writer[A] = new BSONDocumentWriter[A] {
    def write(input: A): Document = f(input)
  }

  def isEmpty(document: Document) = document.isEmpty

  def widenReader[T](r: NarrowValueReader[T]): WidenValueReader[T] =
    r.widenReader

  def readValue[A](value: Value, reader: WidenValueReader[A]): Try[A] =
    reader.readTry(value)
}
