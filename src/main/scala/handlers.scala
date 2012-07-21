package org.asyncmongo.handlers

import org.asyncmongo.bson._
import org.asyncmongo.protocol._
import org.jboss.netty.buffer._

/**
 * A typeclass that writes a ''DocumentType'' instance as a Bson document into a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
 *
 * @tparam DocumentType The type of instances that can be turned into Bson documents.
 */
trait BSONWriter[DocumentType] {
  def write(document: DocumentType) :ChannelBuffer
}

/**
 * A typeclass that creates a ''DocumentType'' instance from as a Bson document from a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
 *
 * @tparam DocumentType The type of instances to create.
 */
trait BSONReader[DocumentType] {
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
  implicit object DefaultBSONReader extends BSONReader[DefaultBSONIterator] {
    override def read(buffer: ChannelBuffer): DefaultBSONIterator = DefaultBSONIterator(buffer)
  }

  implicit object DefaultBSONWriter extends BSONWriter[Bson] {
    def write(document: Bson) = document.makeBuffer
  }

  /** Parses the given response and produces an iterator of [[org.asyncmongo.bson.DefaultBSONIterator]]s. */
  def parse(response: Response) = DefaultBSONReaderHandler.handle(response.reply, response.documents)(DefaultBSONReader)
}

object JacksonBSONHandlers {
  import org.codehaus.jackson.JsonNode
  import org.codehaus.jackson.map.ObjectMapper
  import de.undercouch.bson4jackson._
  import de.undercouch.bson4jackson.io._
  import de.undercouch.bson4jackson.uuid._

  import java.util.HashMap

  val JacksonNodeReaderHandler = DefaultBSONHandlers.DefaultBSONReaderHandler

  private def mapper = {
    val fac = new BsonFactory()
    fac.enable(BsonParser.Feature.HONOR_DOCUMENT_LENGTH)
    val om = new ObjectMapper(fac)
    om.registerModule(new BsonUuidModule())
    om
  }

  object MapReader extends BSONReader[HashMap[Object, Object]] {
    override def read(buffer: ChannelBuffer): HashMap[Object, Object] = {
      mapper.readValue(new ChannelBufferInputStream(buffer), classOf[HashMap[Object, Object]])
    }
  }

  object JacksonNodeReader extends BSONReader[JsonNode] {
    override def read(buffer: ChannelBuffer): JsonNode = {
      mapper.readValue(new ChannelBufferInputStream(buffer), classOf[JsonNode])
    }
  }
}