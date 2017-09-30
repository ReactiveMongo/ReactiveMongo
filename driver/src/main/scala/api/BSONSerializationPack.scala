package reactivemongo.api

import scala.util.Try

import reactivemongo.bson.buffer.{ ReadableBuffer, WritableBuffer }

/** The default serialization pack. */
object BSONSerializationPack extends SerializationPack { self =>
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

  override private[reactivemongo] def bsonSize(value: Value): Int =
    value.byteSize

  override private[reactivemongo] def newBuilder: SerializationPack.Builder[BSONSerializationPack.type] = Builder

  // ---

  /** A builder for serialization simple values (useful for the commands) */
  private object Builder
    extends SerializationPack.Builder[BSONSerializationPack.type] {
    protected val pack = self

    def document(elements: Seq[ElementProducer]): Document =
      BSONDocument(elements: _*)

    def array(value: Value, values: Seq[Value]): Value =
      BSONArray(value +: values)

    def elementProducer(name: String, value: Value): ElementProducer =
      BSONElement(name, value)

    def boolean(b: Boolean): Value = BSONBoolean(b)

    def int(i: Int): Value = BSONInteger(i)

    def long(l: Long): Value = BSONLong(l)

    def double(d: Double): Value = BSONDouble(d)

    def string(s: String): Value = BSONString(s)
  }
}
