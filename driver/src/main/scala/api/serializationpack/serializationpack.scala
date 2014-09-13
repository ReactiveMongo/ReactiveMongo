package reactivemongo.api

import reactivemongo.bson.buffer.{ ReadableBuffer, WritableBuffer }

trait SerializationPack { self: Singleton =>
  type Document
  type Writer[A]
  type Reader[A]

  def serialize[A](a: A, writer: Writer[A]): Document
  def deserialize[A](document: Document, reader: Reader[A]): A

  def writeToBuffer(buffer: WritableBuffer, document: Document): WritableBuffer
  def readFromBuffer(buffer: ReadableBuffer): Document

  def serializeAndWrite[A](buffer: WritableBuffer, document: A, writer: Writer[A]): WritableBuffer =
    writeToBuffer(buffer, serialize(document, writer))
  def readAndDeserialize[A](buffer: ReadableBuffer, reader: Reader[A]): A =
    deserialize(readFromBuffer(buffer), reader)
}

object BSONSerializationPack extends SerializationPack {
  import reactivemongo.bson._
  import reactivemongo.bson.buffer.DefaultBufferHandler

  type Document = BSONDocument
  type Writer[A] = BSONDocumentWriter[A]
  type Reader[A] = BSONDocumentReader[A]

  def serialize[A](a: A, writer: Writer[A]): Document =
    writer.write(a)
  def deserialize[A](document: Document, reader: Reader[A]): A =
    reader.read(document)

  def writeToBuffer(buffer: WritableBuffer, document: Document): WritableBuffer =
    DefaultBufferHandler.writeDocument(document, buffer)
  def readFromBuffer(buffer: ReadableBuffer): Document =
    DefaultBufferHandler.readDocument(buffer).get
}