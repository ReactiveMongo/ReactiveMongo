package reactivemongo.api

import java.util.UUID

import scala.reflect.ClassTag

import scala.util.Try

import reactivemongo.api.bson.buffer.{ ReadableBuffer, WritableBuffer }

import reactivemongo.core.protocol.Response

trait SerializationPack { self: Singleton =>
  type Value
  type ElementProducer
  type Document <: Value
  type Writer[A]
  type Reader[A]
  type NarrowValueReader[A]
  private[reactivemongo] type WidenValueReader[A]

  private[reactivemongo] val IsDocument: ClassTag[Document]
  private[reactivemongo] val IsValue: ClassTag[Value]

  def IdentityWriter: Writer[Document]
  def IdentityReader: Reader[Document]

  def serialize[A](a: A, writer: Writer[A]): Document

  def deserialize[A](document: Document, reader: Reader[A]): A

  private[reactivemongo] def writeToBuffer(
    buffer: WritableBuffer,
    document: Document): WritableBuffer

  private[reactivemongo] def readFromBuffer(buffer: ReadableBuffer): Document

  private[reactivemongo] final def readAndDeserialize[A](buffer: ReadableBuffer, reader: Reader[A]): A = deserialize(readFromBuffer(buffer), reader)

  private[reactivemongo] def readAndDeserialize[A](response: Response, reader: Reader[A]): A = {
    val channelBuf = ReadableBuffer(response.documents)

    readAndDeserialize(channelBuf, reader)
  }

  private[reactivemongo] final def serializeAndWrite[A](buffer: WritableBuffer, document: A, writer: Writer[A]): WritableBuffer = writeToBuffer(buffer, serialize(document, writer))

  /** Prepares a writer from the given serialization function. */
  def writer[A](f: A => Document): Writer[A]

  /** Returns true if the given `document` is empty. */
  def isEmpty(document: Document): Boolean

  def widenReader[T](r: NarrowValueReader[T]): WidenValueReader[T]

  private[reactivemongo] def readValue[A](value: Value, reader: WidenValueReader[A]): Try[A]

  // Returns a Reader from a function
  private[reactivemongo] def reader[A](f: Document => A): Reader[A]

  private[reactivemongo] def narrowIdentityReader: NarrowValueReader[Value]

  private[reactivemongo] def bsonSize(value: Value): Int

  private[reactivemongo] def newBuilder: SerializationPack.Builder[self.type]

  private[reactivemongo] def newDecoder: SerializationPack.Decoder[self.type]

  private[reactivemongo] def pretty(doc: Document): String = doc.toString
}

object SerializationPack {
  /** A builder for serialization simple values (useful for the commands) */
  private[reactivemongo] trait Builder[P <: SerializationPack] {
    protected[reactivemongo] val pack: P

    /** Returns a new document from a sequence of element producers. */
    def document(elements: Seq[pack.ElementProducer]): pack.Document

    /** Returns a new non empty array of values */
    def array(value: pack.Value, values: Seq[pack.Value]): pack.Value

    /** Returns a byte array as a serialized value. */
    def binary(data: Array[Byte]): pack.Value

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

    /** Returns a long as a serialized value. */
    def long(l: Long): pack.Value

    /** Returns a double as a serialized value. */
    def double(d: Double): pack.Value

    /** Returns a string as a serialized value. */
    def string(s: String): pack.Value

    /** Returns an UUID as a serialized value. */
    def uuid(id: UUID): pack.Value

    /** Returns a timestamp as a serialized value. */
    def timestamp(time: Long): pack.Value

    /** Returns a date/time as a serialized value. */
    def dateTime(time: Long): pack.Value

    /** Returns a regular expression value. */
    def regex(pattern: String, options: String): pack.Value

    /** Returns a new object ID. */
    def generateObjectId(): pack.Value
  }

  /**
   * A decoder for serialization simple values (for internal use).
   *
   * @define returnsNamedElement Returns the named element from the given document
   */
  private[reactivemongo] trait Decoder[P <: SerializationPack] {
    protected[reactivemongo] val pack: P

    /** Extract the value, if and only if it's a document. */
    def asDocument(value: pack.Value): Option[pack.Document]

    /** Returns the names of the document elements. */
    def names(document: pack.Document): Set[String]

    /** @returnsNamedElement, if the element exists. */
    def get(document: pack.Document, name: String): Option[pack.Value]

    /**
     * @returnsNamedElement, if the element exists
     * with expected `T` as value type.
     */
    @com.github.ghik.silencer.silent(".*\\ ev\\ .*is\\ never\\ used.*")
    final def value[T](
      document: pack.Document,
      name: String)(
      implicit
      ev: T <:< pack.Value, cls: ClassTag[T]): Option[T] =
      get(document, name).collect { case `cls`(t) => t }

    final def read[T](document: pack.Document, name: String)(implicit r: pack.NarrowValueReader[T]): Option[T] = {
      val widenReader = pack.widenReader[T](r)

      get(document, name).flatMap(
        pack.readValue[T](_, widenReader).toOption)
    }

    /**
     * @returnsNamedElement, if the element is an array field.
     */
    def array(document: pack.Document, name: String): Option[Seq[pack.Value]]

    final def values[T](document: pack.Document, name: String)(implicit r: pack.NarrowValueReader[T]): Option[Seq[T]] = {
      val widenReader = pack.widenReader[T](r)

      array(document, name).map {
        _.flatMap(pack.readValue[T](_, widenReader).toOption)
      }
    }

    /**
     * @returnsNamedElement, if the element is a binary field
     */
    def binary(document: pack.Document, name: String): Option[Array[Byte]]

    /**
     * @returnsNamedElement, if the element is a boolean-like field
     * (numeric or boolean).
     */
    def booleanLike(document: pack.Document, name: String): Option[Boolean]

    /**
     * @returnsNamedElement, if the element is a nested document.
     */
    def child(document: pack.Document, name: String): Option[pack.Document]

    /**
     * @returnsNamedElement, if the element is a list of nested documents.
     */
    def children(document: pack.Document, name: String): List[pack.Document]

    /**
     * @returnsNamedElement, if the element is a double field.
     */
    def double(document: pack.Document, name: String): Option[Double]

    /**
     * @returnsNamedElement, if the element is a integer field.
     */
    def int(document: pack.Document, name: String): Option[Int]

    /**
     * @returnsNamedElement, if the element is a long field.
     */
    def long(document: pack.Document, name: String): Option[Long]

    /**
     * @returnsNamedElement, if the element is a string field.
     */
    def string(document: pack.Document, name: String): Option[String]

    /**
     * @returnsNamedElement, if the element is a binary/uuid field.
     */
    def uuid(document: pack.Document, name: String): Option[UUID]
  }
}
