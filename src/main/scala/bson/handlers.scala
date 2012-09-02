package reactivemongo.bson.handlers

import org.jboss.netty.buffer._
import reactivemongo.bson._
import reactivemongo.core.protocol._

/**
 * A typeclass that creates a raw BSON document contained in a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]] from a `DocumentType` instance.
 *
 * @tparam DocumentType The type of the instance that can be turned into a BSON document.
 */
trait RawBSONWriter[-DocumentType] {
  def write(document: DocumentType) :ChannelBuffer
}

/**
 * A typeclass that creates a `DocumentType` instance from a raw BSON document contained in a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
 *
 * @tparam DocumentType The type of the instance to create.
 */
trait RawBSONReader[+DocumentType] {
  def read(buffer: ChannelBuffer) :DocumentType
}

/**
 * A typeclass that creates a BSONDocument from a `DocumentType`.
 *
 * @tparam DocumentType The type of the instance that can be turned into a BSONDocument.
 */
trait BSONWriter[-DocumentType] extends RawBSONWriter[DocumentType] {
  def toBSON(document: DocumentType) :BSONDocument

  final def write(document: DocumentType) = toBSON(document).makeBuffer
}

/**
 * A typeclass that creates a `DocumentType from a BSONDocument.
 *
 * @tparam DocumentType The type of the instance to create.
 */
trait BSONReader[+DocumentType] extends RawBSONReader[DocumentType] {
  def fromBSON(doc: BSONDocument) :DocumentType

  final def read(buffer: ChannelBuffer) = fromBSON(BSONDocument(buffer))
}

/**
 * A handler that produces an Iterator of `DocumentType`,
 * provided that there is an implicit [[reactivemongo.bson.handlers.BSONReader]][DocumentType] in the scope.
 */
trait BSONReaderHandler {
  def handle[DocumentType](reply: Reply, buffer: ChannelBuffer)(implicit reader: RawBSONReader[DocumentType]) :Iterator[DocumentType]
}

/** Default [[reactivemongo.bson.handlers.BSONReader]], [[reactivemongo.bson.handlers.BSONWriter]], [[reactivemongo.bson.handlers.BSONReaderHandler]]. */
object DefaultBSONHandlers {
  implicit object DefaultBSONReaderHandler extends BSONReaderHandler {
    def handle[DocumentType](reply: Reply, buffer: ChannelBuffer)(implicit reader: RawBSONReader[DocumentType]) :Iterator[DocumentType] = {
      new Iterator[DocumentType] {
        def hasNext = buffer.readable
        def next = reader.read(buffer.readBytes(buffer.getInt(buffer.readerIndex)))
      }
    }
  }

  implicit object DefaultBSONDocumentReader extends BSONReader[TraversableBSONDocument] {
    def fromBSON(doc: BSONDocument) = doc.toTraversable
  }

  implicit object DefaultBSONDocumentWriter extends BSONWriter[BSONDocument] {
    def toBSON(doc: BSONDocument) = doc
  }

  /** Parses the given response and produces an iterator of [[reactivemongo.bson.DefaultBSONIterator]]s. */
  def parse(response: Response) = DefaultBSONReaderHandler.handle(response.reply, response.documents)(DefaultBSONDocumentReader)
}