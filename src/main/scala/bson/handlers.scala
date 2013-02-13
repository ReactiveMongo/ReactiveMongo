package reactivemongo.bson.handlers

import org.jboss.netty.buffer._
import reactivemongo.bson._
import reactivemongo.bson.netty._
import reactivemongo.core.protocol._

/**
 * A typeclass that creates a raw BSON document contained in a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] from a `DocumentType` instance.
 *
 * @tparam DocumentType The type of the instance that can be turned into a BSON document.
 */
trait RawBSONDocumentWriter[-DocumentType] {
  def write(document: DocumentType) :ChannelBuffer
}

/**
 * A typeclass that creates a `DocumentType` instance from a raw BSON document contained in a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
 *
 * @tparam DocumentType The type of the instance to create.
 */
trait RawBSONDocumentReader[+DocumentType] {
  def read(buffer: ChannelBuffer) :DocumentType
}

/**
 * A typeclass that creates a BSONDocument from a `DocumentType`.
 *
 * @tparam DocumentType The type of the instance that can be turned into a BSONDocument.
 */
trait BSONDocumentWriter[-DocumentType] extends RawBSONDocumentWriter[DocumentType] {
  def toBSON(document: DocumentType) :BSONDocument

  final def write(document: DocumentType) = {
    val buffer = ChannelBufferWritableBuffer()
    DefaultBufferHandler.write(buffer, toBSON(document))
    buffer.buffer
  }
}

/**
 * A typeclass that creates a `DocumentType from a BSONDocument.
 *
 * @tparam DocumentType The type of the instance to create.
 */
trait BSONDocumentReader[+DocumentType] extends RawBSONDocumentReader[DocumentType] {
  def fromBSON(doc: BSONDocument) :DocumentType

  final def read(buffer: ChannelBuffer) = {
    val readableBuffer = ChannelBufferReadableBuffer(buffer)
    val doc = DefaultBufferHandler.readDocument(readableBuffer).get // TODO
    fromBSON(doc)
  }
}

/**
 * A handler that produces an Iterator of `DocumentType`,
 * provided that there is an implicit [[reactivemongo.bson.handlers.BSONReader]][DocumentType] in the scope.
 */
trait BSONReaderHandler {
  def handle[DocumentType](reply: Reply, buffer: ChannelBuffer)(implicit reader: RawBSONDocumentReader[DocumentType]) :Iterator[DocumentType]
}

/** Default [[reactivemongo.bson.handlers.BSONReader]], [[reactivemongo.bson.handlers.BSONWriter]], [[reactivemongo.bson.handlers.BSONReaderHandler]]. */
object DefaultBSONHandlers {
  implicit object DefaultBSONReaderHandler extends BSONReaderHandler {
    def handle[DocumentType](reply: Reply, buffer: ChannelBuffer)(implicit reader: RawBSONDocumentReader[DocumentType]) :Iterator[DocumentType] = {
      new Iterator[DocumentType] {
        def hasNext = buffer.readable
        def next = reader.read(buffer.readBytes(buffer.getInt(buffer.readerIndex)))
      }
    }
  }

  implicit object DefaultBSONDocumentReader extends BSONDocumentReader[BSONDocument] {
    def fromBSON(doc: BSONDocument) :BSONDocument = doc
  }

  implicit object DefaultBSONDocumentWriter extends BSONDocumentWriter[BSONDocument] {
    def toBSON(doc: BSONDocument) = doc
  }

  /** Parses the given response and produces an iterator of [[reactivemongo.bson.DefaultBSONIterator]]s. */
  def parse(response: Response) = DefaultBSONReaderHandler.handle(response.reply, response.documents)(DefaultBSONDocumentReader)
}