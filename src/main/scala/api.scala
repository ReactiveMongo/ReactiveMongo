package org.asyncmongo.api

import akka.dispatch.Future
import org.asyncmongo.protocol.Query
import org.asyncmongo.protocol.WritableMessage
import akka.util.Timeout
import akka.util.duration._
import org.asyncmongo.protocol.Reply
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBufferInputStream

object Mongo {
  implicit val timeout = Timeout(5 seconds)
  
  def find[T, U](fullCollectionName: String, query: T, limit: Int)(implicit writer: BSONWriter[T], reader: BSONReaderHandler[U], m: Manifest[U]) :Future[Iterator[U]] = {
    val op = Query(0, fullCollectionName, 0, limit)
    val bson = writer.write(query)
    val message = WritableMessage(op, bson)
    
    org.asyncmongo.MongoSystem.ask(message).map { response =>
      reader.handle(response.reply, response.documents)
    }
  }
}

trait BSONReader[DocumentType] extends Iterator[DocumentType] {
  val count: Int
}
trait BSONReaderHandler[DocumentType] {
  def handle(reply: Reply, buffer: ChannelBuffer) :BSONReader[DocumentType]
  //def handle(reply: Reply, bytes: Array[Byte]) :BSONReader[DocumentType]
}

trait BSONWriter[DocumentType] {
  def write(document: DocumentType) :Array[Byte]
}

object DefaultBSONHandlers {
  import de.undercouch.bson4jackson._
  import de.undercouch.bson4jackson.io._
  import de.undercouch.bson4jackson.uuid._
  import org.codehaus.jackson.map.ObjectMapper

  implicit object MapReaderHandler extends BSONReaderHandler[java.util.HashMap[Object, Object]] {
    override def handle(reply: Reply, buffer: ChannelBuffer): BSONReader[java.util.HashMap[Object, Object]] = MapReader(reply.numberReturned, buffer)
  }

  case class MapReader(count: Int, buffer: ChannelBuffer) extends BSONReader[java.util.HashMap[Object, Object]] {
    private val mapper = {
      val fac = new BsonFactory()
      fac.enable(BsonParser.Feature.HONOR_DOCUMENT_LENGTH)
      val om = new ObjectMapper(fac)
      om.registerModule(new BsonUuidModule())
      om
    }
    private val is = new ChannelBufferInputStream(buffer)
    override def next: java.util.HashMap[Object, Object] = {
      mapper.readValue(new ChannelBufferInputStream(buffer), classOf[java.util.HashMap[Object, Object]])
    }
    override def hasNext = is.available > 0
    override def size = count
  }
  
  implicit object HashMapWriter extends BSONWriter[java.util.HashMap[Object, Object]] {
    private val mapper = {
      val fac = new BsonFactory()
      fac.enable(BsonParser.Feature.HONOR_DOCUMENT_LENGTH)
      val om = new ObjectMapper(fac)
      om.registerModule(new BsonUuidModule())
      om
    }
    def write(document :java.util.HashMap[Object, Object]) = mapper.writeValueAsBytes(document)
  }
}


import java.util.HashMap
import DefaultBSONHandlers._

object Test {
  def test = {
    val query = new HashMap[Object, Object]()
    query.put("name", "Jack")
    val future = Mongo.find[HashMap[Object, Object], HashMap[Object, Object]]("plugin.acoll", query, 2)
    println("Test: future is " + future)
    future.onComplete {
      case Right(map) => {
        println("got a result! " + map.size + " documents available")
        for(m <- map) {
          println("doc: " + m)
        }
      }
      case Left(e) => throw e
    }
  }
}