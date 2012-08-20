package reactivemongo.bson.handlers

import org.jboss.netty.buffer._
import reactivemongo.bson._
import reactivemongo.core.protocol._

/**
 * A typeclass that writes a ''DocumentType'' instance as a Bson document into a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
 *
 * @tparam DocumentType The type of instances that can be turned into Bson documents.
 */
trait BSONWriter[-DocumentType] {
  def write(document: DocumentType) :ChannelBuffer
}

/**
 * A typeclass that creates a ''DocumentType'' instance from as a Bson document from a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
 *
 * @tparam DocumentType The type of instances to create.
 */
trait BSONReader[+DocumentType] {
  def read(buffer: ChannelBuffer) :DocumentType
}

/**
 * A handler that produces an Iterator of ''DocumentType'',
 * provided that there is an implicit [[org.asyncmongo.handlers.BSONReader]][DocumentType] in the scope.
 */
trait BSONReaderHandler {
  def handle[DocumentType](reply: Reply, buffer: ChannelBuffer)(implicit reader: BSONReader[DocumentType]) :Iterator[DocumentType]
}

/** Default [[org.asyncmongo.handlers.BSONReader]], [[org.asyncmongo.handlers.BSONWriter]], [[org.asyncmongo.handlers.BSONReaderHandler]]. */
object DefaultBSONHandlers {
  implicit object DefaultBSONReaderHandler extends BSONReaderHandler {
    def handle[DocumentType](reply: Reply, buffer: ChannelBuffer)(implicit reader: BSONReader[DocumentType]) :Iterator[DocumentType] = {
      new Iterator[DocumentType] {
        def hasNext = buffer.readable
        def next = reader.read(buffer.readBytes(buffer.getInt(buffer.readerIndex)))
      }
    }
  }

  implicit object DefaultBSONDocumentReader extends BSONReader[TraversableBSONDocument] {
    override def read(buffer: ChannelBuffer) :TraversableBSONDocument = BSONDocument(buffer)
  }

  implicit object DefaultBSONDocumentWriter extends BSONWriter[BSONDocument] {
    def write(document :BSONDocument) = document.makeBuffer
  }

  /** Parses the given response and produces an iterator of [[org.asyncmongo.bson.DefaultBSONIterator]]s. */
  def parse(response: Response) = DefaultBSONReaderHandler.handle(response.reply, response.documents)(DefaultBSONDocumentReader)
}