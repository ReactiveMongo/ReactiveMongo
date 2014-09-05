package reactivemongo.api

trait SerializationPack { self: Singleton =>
  type Document
  type Writer[A]
  type Reader[A]

  def serialize[A](a: A, writer: Writer[A]): Document
  def deserialize[A](document: Document, reader: Reader[A]): A


}

object BSONSerializationPack extends SerializationPack {
  import reactivemongo.bson._

  type Document = BSONDocument
  type Writer[A] = BSONDocumentWriter[A]
  type Reader[A] = BSONDocumentReader[A]

  def serialize[A](a: A, writer: Writer[A]): Document = ???
  def deserialize[A](document: Document, reader: Reader[A]): A = ???
}