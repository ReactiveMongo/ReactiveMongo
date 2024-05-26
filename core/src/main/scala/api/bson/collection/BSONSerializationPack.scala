package reactivemongo.api.bson.collection

import java.util.UUID

import scala.util.{ Failure, Success, Try }

import scala.reflect.ClassTag

import reactivemongo.core.protocol.Response

import reactivemongo.api.SerializationPack
import reactivemongo.api.bson._
import reactivemongo.api.bson.buffer.{
  DefaultBufferHandler,
  ReadableBuffer,
  WritableBuffer
}

/** The default serialization pack. */
object BSONSerializationPack extends SerializationPack { self =>

  type Value = BSONValue
  type ElementProducer = reactivemongo.api.bson.ElementProducer
  type Document = BSONDocument
  type Writer[A] = BSONDocumentWriter[A]
  type Reader[A] = BSONDocumentReader[A]
  type NarrowValueReader[A] = BSONReader[A]
  private[reactivemongo] type WidenValueReader[A] = BSONReader[A]

  private[reactivemongo] val IsDocument = implicitly[ClassTag[BSONDocument]]
  private[reactivemongo] val IsValue = implicitly[ClassTag[BSONValue]]

  lazy val IdentityReader: Reader[Document] = bsonDocumentReader
  lazy val IdentityWriter: Writer[Document] = bsonDocumentWriter

  def serialize[A](a: A, writer: Writer[A]): Document =
    writer.writeTry(a) match {
      case Success(doc)   => doc
      case Failure(cause) => throw cause
    }

  def deserialize[A](
      document: Document,
      reader: Reader[A]
    ): A = reader.readTry(document) match {
    case Success(a) => a

    case Failure(cause) =>
      throw cause
  }

  private[reactivemongo] def writeToBuffer(
      buffer: WritableBuffer,
      document: Document
    ): WritableBuffer =
    DefaultBufferHandler.writeDocument(document, buffer)

  private[reactivemongo] def readFromBuffer(buffer: ReadableBuffer): Document =
    DefaultBufferHandler.readDocument(buffer)

  override def readAndDeserialize[A](response: Response, reader: Reader[A]): A =
    response match {
      case s @ Response.Successful(_, _, docs, _) =>
        s.first match {
          case Some(preloaded) => // optimization
            deserialize[A](preloaded, reader)

          case _ =>
            readAndDeserialize(ReadableBuffer(docs), reader)
        }

      case Response.CommandError(_, _, _, cause) =>
        throw cause

      case _ =>
        readAndDeserialize(ReadableBuffer(response.documents), reader)
    }

  def writer[A](f: A => Document): Writer[A] = BSONDocumentWriter[A](f)

  def isEmpty(document: Document) = document.isEmpty

  def widenReader[T](r: NarrowValueReader[T]): WidenValueReader[T] = r

  private[reactivemongo] def readValue[A](
      value: Value,
      reader: WidenValueReader[A]
    ): Try[A] = reader.readTry(value)

  private[reactivemongo] def reader[A](f: Document => A): Reader[A] =
    BSONDocumentReader(f)

  private[reactivemongo] def afterReader[A, B](
      r: Reader[A]
    )(f: A => B
    ): Reader[B] = r.afterRead(f)

  override private[reactivemongo] def bsonSize(value: Value): Int =
    value.byteSize

  private[reactivemongo] val narrowIdentityReader: NarrowValueReader[BSONValue] =
    BSONReader[BSONValue](identity)

  override private[reactivemongo] val newBuilder: SerializationPack.Builder[BSONSerializationPack.type] =
    Builder

  override private[reactivemongo] val newDecoder: SerializationPack.Decoder[BSONSerializationPack.type] =
    Decoder

  override private[reactivemongo] def pretty(doc: BSONDocument): String =
    BSONDocument.pretty(doc)

  // ---

  /** A builder for serialization simple values (useful for the commands) */
  private object Builder
      extends SerializationPack.Builder[BSONSerializationPack.type] {
    protected[reactivemongo] val pack = self

    def document(elements: Seq[ElementProducer]): Document =
      reactivemongo.api.BSONCompat.document(elements)

    @inline def elementProducer(doc: Document): ElementProducer = doc

    def array(values: Seq[Value]): Value = BSONArray(values)

    def binary(data: Array[Byte]): Value =
      BSONBinary(data, Subtype.GenericBinarySubtype)

    def elementProducer(name: String, value: Value): ElementProducer =
      BSONElement(name, value)

    def boolean(b: Boolean): Value = BSONBoolean(b)

    def int(i: Int): Value = BSONInteger(i)

    def long(l: Long): Value = BSONLong(l)

    def double(d: Double): Value = BSONDouble(d)

    def string(s: String): Value = BSONString(s)

    def uuid(id: UUID): Value = BSONBinary(id)

    def timestamp(time: Long): Value = BSONTimestamp(time)

    def dateTime(time: Long): Value = BSONDateTime(time)

    def regex(pattern: String, options: String): Value =
      BSONRegex(pattern, options)

    def generateObjectId() = BSONObjectID.generate()
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

    def binary(document: BSONDocument, name: String): Option[Array[Byte]] =
      document.get(name).collect { case bin: BSONBinary => bin.byteArray }

    def get(document: BSONDocument, name: String): Option[BSONValue] =
      document.get(name)

    def array(document: pack.Document, name: String): Option[Seq[BSONValue]] =
      document.getAsOpt[BSONArray](name).map(_.values)

    def booleanLike(document: BSONDocument, name: String): Option[Boolean] =
      document.booleanLike(name)

    def child(document: BSONDocument, name: String): Option[BSONDocument] =
      document.getAsOpt[BSONDocument](name)

    def children(document: BSONDocument, name: String): List[BSONDocument] = {
      document
        .getAsOpt[List[BSONDocument]](name)
        .getOrElse(List.empty[BSONDocument])
    }

    def double(document: BSONDocument, name: String): Option[Double] =
      document.getAsOpt[Double](name)

    def int(document: BSONDocument, name: String): Option[Int] =
      document.getAsOpt[Int](name)

    def long(document: BSONDocument, name: String): Option[Long] =
      document.getAsOpt[BSONNumberLike](name).flatMap { _.toLong.toOption }

    def string(document: BSONDocument, name: String): Option[String] =
      document.getAsOpt[String](name)

    def uuid(document: BSONDocument, name: String): Option[UUID] =
      document.getAsOpt[BSONBinary](name).collect {
        case bin @ BSONBinary(Subtype.UuidSubtype) =>
          UUID.nameUUIDFromBytes(bin.byteArray)
      }
  }
}
