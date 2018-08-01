package reactivemongo.api

import java.util.UUID

import scala.util.Try

import reactivemongo.bson.buffer.{ ReadableBuffer, WritableBuffer }

import reactivemongo.core.protocol.Response
import reactivemongo.core.netty.ChannelBufferReadableBuffer

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

  override final def readAndDeserialize[A](response: Response, reader: Reader[A]): A = response match {
    case s @ Response.Successful(_, _, docs, _) => s.first match {
      case Some(preloaded) =>
        deserialize[A](preloaded, reader) // optimization

      case _ => {
        val channelBuf = ChannelBufferReadableBuffer(docs)
        readAndDeserialize(channelBuf, reader)
      }
    }

    case _ => {
      val channelBuf = ChannelBufferReadableBuffer(response.documents)
      readAndDeserialize(channelBuf, reader)
    }
  }

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

  private[reactivemongo] def bsonValue(value: BSONValue): BSONValue = value

  override private[reactivemongo] val newBuilder: SerializationPack.Builder[BSONSerializationPack.type] = Builder

  override private[reactivemongo] val newDecoder: SerializationPack.Decoder[BSONSerializationPack.type] = Decoder

  override private[reactivemongo] def pretty(doc: BSONDocument): String =
    BSONDocument.pretty(doc)

  // ---

  /** A builder for serialization simple values (useful for the commands) */
  private object Builder
    extends SerializationPack.Builder[BSONSerializationPack.type] {
    protected[reactivemongo] val pack = self

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

    def uuid(id: UUID): Value = { // TODO: Adds handler in BSON
      val buf = java.nio.ByteBuffer.wrap(Array.ofDim[Byte](16))

      buf putLong id.getMostSignificantBits
      buf putLong id.getLeastSignificantBits

      BSONBinary(buf.array, Subtype.UuidSubtype)
    }

    /** Returns a timestamp as a serialized value. */
    def timestamp(time: Long): Value = BSONTimestamp(time)
  }

  private object Decoder
    extends SerializationPack.Decoder[BSONSerializationPack.type] {
    protected[reactivemongo] val pack = self

    def asDocument(value: BSONValue): Option[BSONDocument] = value match {
      case doc: BSONDocument => Some(doc)
      case _                 => None
    }

    def names(document: BSONDocument): Set[String] =
      document.elements.map(_.name).toSet

    def get(document: BSONDocument, name: String): Option[BSONValue] =
      document.get(name)

    def array(document: pack.Document, name: String): Option[Seq[BSONValue]] =
      document.getAs[BSONArray](name).map(_.elements.map(_.value))

    def booleanLike(document: BSONDocument, name: String): Option[Boolean] =
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

    def long(document: BSONDocument, name: String): Option[Long] =
      document.getAs[BSONNumberLike](name).map(_.toLong)

    def string(document: BSONDocument, name: String): Option[String] =
      document.getAs[String](name)

    def uuid(document: BSONDocument, name: String): Option[UUID] =
      document.getAs[BSONBinary](name).collect {
        case bin @ BSONBinary(_, Subtype.UuidSubtype) =>
          UUID.nameUUIDFromBytes(bin.byteArray)
      }
  }
}
