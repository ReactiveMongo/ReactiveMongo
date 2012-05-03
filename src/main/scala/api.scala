package org.asyncmongo.api

import akka.dispatch.Future
import org.asyncmongo.protocol._
import org.asyncmongo.protocol.messages._
import akka.util.Timeout
import akka.util.duration._
import org.asyncmongo.protocol.Reply
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.asyncmongo.MongoSystem

object Mongo {
  implicit val timeout = Timeout(5 seconds)
  
  def find[T, U, V](fullCollectionName: String, query: T, fields: Option[U], skip: Int, limit: Int)(implicit writer: BSONWriter[T], writer2: BSONWriter[U], reader: BSONReaderHandler[V], m: Manifest[V]) :Future[Iterator[V]] = {
    val op = Query(0, fullCollectionName, skip, limit)
    val bson = if(fields.isDefined) writer.write(query) ++ writer2.write(fields.get) else writer.write(query)
    val message = WritableMessage(op, bson)
    
    MongoSystem.ask(message).map { response =>
      reader.handle(response.reply, response.documents)
    }
  }
  
  def count[U](fullCollectionName: String)(implicit reader: BSONReaderHandler[U], m: Manifest[U]) :Future[U] = {
    val message = Count.makeWritableMessage(fullCollectionName.span(_ != '.')._1, fullCollectionName, None, None, (new java.util.Random()).nextInt(Integer.MAX_VALUE))
    MongoSystem.ask(message).map { response =>
      reader.handle(response.reply, response.documents).next
    }
  }
  
  def insert[T](fullCollectionName: String, document: T)(implicit writer: BSONWriter[T]) :Unit = {
    val op = Insert(0, fullCollectionName)
    val bson = writer.write(document)
    val message = WritableMessage(op, bson)
    MongoSystem.send(message)
  }
  
  def insert[T, U](fullCollectionName: String, document: T, writeConcern: GetLastError)(implicit writer: BSONWriter[T], reader: BSONReaderHandler[U], m: Manifest[U]) :Future[U] = {
    val op = Insert(0, fullCollectionName)
    val bson = writer.write(document)
    val message = WritableMessage(op, bson)
    MongoSystem.ask(message, writeConcern).map { response =>
      reader.handle(response.reply, response.documents).next
    }
  }
  
  def remove[T](fullCollectionName: String, query: T)(implicit writer: BSONWriter[T]) :Unit = remove(fullCollectionName, query, false)
  
  def remove[T](fullCollectionName: String, query: T, firstMatchOnly: Boolean)(implicit writer: BSONWriter[T]) : Unit = {
    val op = Delete(fullCollectionName, if(firstMatchOnly) 1 else 0)
    val bson = writer.write(query)
    val message = WritableMessage(op, bson)
    MongoSystem.send(message)
  }
  
  def remove[T, U](fullCollectionName: String, query: T, writeConcern: GetLastError, firstMatchOnly: Boolean = false)(implicit writer: BSONWriter[T], reader: BSONReaderHandler[U], m: Manifest[U]) :Future[U] = {
    val op = Delete(fullCollectionName, if(firstMatchOnly) 1 else 0)
    val bson = writer.write(query)
    val message = WritableMessage(op, bson)
    MongoSystem.ask(message, writeConcern).map { response =>
      reader.handle(response.reply, response.documents).next
    }
  }
}


class DB(
  val name: String,
  val nodes: List[(String, Int)] = List("localhost" -> 27017)
) {
  
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

object Test { //Mongo.find[HashMap[Object, Object], HashMap[Object, Object]]("plugin.acoll", query, 2)
  def test = {
    val query = new HashMap[Object, Object]()
    query.put("name", "Jack")
    val future = Mongo.find("plugin.acoll", query, None, 2, 0)
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
    Mongo.count("plugin.acoll").onComplete {
      case Right(map) => {
        println("count! " + map.size + " documents available")
        println("doc: " + map)
      }
      case Left(e) => throw e
    }
  }
}