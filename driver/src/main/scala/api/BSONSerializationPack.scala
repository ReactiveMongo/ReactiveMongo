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

  def writer[A](f: A => Document): Writer[A] = BSONDocumentWriter[A](f)

  def isEmpty(document: Document) = document.isEmpty

  def widenReader[T](r: NarrowValueReader[T]): WidenValueReader[T] =
    r.widenReader

  def readValue[A](value: Value, reader: WidenValueReader[A]): Try[A] =
    reader.readTry(value)

  private[reactivemongo] def reader[A](f: Document => A): Reader[A] =
    BSONDocumentReader(f)

  override private[reactivemongo] def bsonSize(value: Value): Int =
    value.byteSize

  private[reactivemongo] def document(doc: BSONDocument): Document = doc

  override private[reactivemongo] val newBuilder: SerializationPack.Builder[BSONSerializationPack.type] = Builder

  override private[reactivemongo] val newDecoder: SerializationPack.Decoder[BSONSerializationPack.type] = Decoder

  override private[reactivemongo] def pretty(doc: BSONDocument): String =
    BSONDocument.pretty(doc)

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

  private object Decoder
    extends SerializationPack.Decoder[BSONSerializationPack.type] {
    protected val pack = self

    def booleanLike(document: pack.Document, name: String): Option[Boolean] =
      document.getAs[BSONBooleanLike](name).map(_.toBoolean)

    def child(document: BSONDocument, name: String): Option[BSONDocument] =
      document.getAs[BSONDocument](name)

    def children(document: BSONDocument, name: String): List[BSONDocument] = {
      document.getAs[List[BSONDocument]](name).
        getOrElse(List.empty[BSONDocument])
    }

    def double(document: BSONDocument, name: String): Option[Double] =
      document.getAs[Double](name)

    def int(document: BSONDocument, name: String): Option[Int] =
      document.getAs[Int](name)

    def string(document: BSONDocument, name: String): Option[String] =
      document.getAs[String](name)
  }
}
