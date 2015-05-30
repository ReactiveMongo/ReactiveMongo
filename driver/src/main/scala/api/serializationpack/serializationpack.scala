package reactivemongo.api

import java.nio.ByteOrder

import reactivemongo.bson.buffer.{ ReadableBuffer, WritableBuffer }
import reactivemongo.core.AkkaReadableBuffer

trait SerializationPack { self: Singleton =>
  type Document
  type Writer[A]
  type Reader[A]

  def IdentityWriter: Writer[Document]
  def IdentityReader: Reader[Document]

  def serialize[A](a: A, writer: Writer[A]): Document
  def deserialize[A](document: Document, reader: Reader[A]): A

  def writeToBuffer(buffer: WritableBuffer, document: Document): WritableBuffer
  def readFromBuffer(buffer: ReadableBuffer): Document

  def serializeAndWrite[A](buffer: WritableBuffer, document: A, writer: Writer[A]): WritableBuffer =
    writeToBuffer(buffer, serialize(document, writer))
  def readAndDeserialize[A](buffer: ReadableBuffer, reader: Reader[A]): A =
    deserialize(readFromBuffer(buffer), reader)


  import reactivemongo.core.protocol.Response
  import reactivemongo.core.netty.ChannelBufferReadableBuffer

  final def readAndDeserialize[A](response: Response, reader: Reader[A]): A = {
    implicit val order = ByteOrder.LITTLE_ENDIAN
    val buf = response.documents.splitAt(4)
    val byteStringBuffer = new AkkaReadableBuffer(buf._2.splitAt(buf._1.iterator.getInt)._2)
    //val channelBuf = ChannelBufferReadableBuffer(buf.readBytes(buf.getInt(buf.readerIndex)))
    readAndDeserialize(byteStringBuffer, reader)
  }
}

object BSONSerializationPack extends SerializationPack {
  import reactivemongo.bson._
  import reactivemongo.bson.buffer.DefaultBufferHandler

  type Document = BSONDocument
  type Writer[A] = BSONDocumentWriter[A]
  type Reader[A] = BSONDocumentReader[A]

  object IdentityReader extends Reader[Document] {
    def read(document: Document): Document = document
  }

  object IdentityWriter extends Writer[Document] {
    def write(document: Document): Document = document
  }

  def serialize[A](a: A, writer: Writer[A]): Document =
    writer.write(a)
  def deserialize[A](document: Document, reader: Reader[A]): A =
    reader.read(document)

  def writeToBuffer(buffer: WritableBuffer, document: Document): WritableBuffer =
    DefaultBufferHandler.writeDocument(document, buffer)
  def readFromBuffer(buffer: ReadableBuffer): Document =
    DefaultBufferHandler.readDocument(buffer).get
}