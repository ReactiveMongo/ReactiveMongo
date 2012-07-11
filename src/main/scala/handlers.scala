package org.asyncmongo.handlers

import org.asyncmongo.bson._
import org.asyncmongo.protocol._
import org.jboss.netty.buffer._

trait BSONWriter[DocumentType] {
  def write(document: DocumentType) :ChannelBuffer
}

trait BSONReader[DocumentType] {
  def read(buffer: ChannelBuffer) :DocumentType
}

trait BSONReaderHandler {
  def handle[DocumentType](reply: Reply, buffer: ChannelBuffer)(implicit reader: BSONReader[DocumentType]) :Iterator[DocumentType]
}

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
    def write(document: Bson) = document.getBuffer
  }

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