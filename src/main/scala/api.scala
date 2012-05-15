package org.asyncmongo.api

import akka.dispatch.{Future, Promise}
import org.asyncmongo.protocol._
import org.asyncmongo.protocol.messages._
import akka.util.Timeout
import akka.util.duration._
import org.asyncmongo.protocol.Reply
import org.asyncmongo.actors.MongoConnection
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBufferInputStream

case class Mongo(connection: MongoConnection, implicit val timeout :Timeout = Timeout(5 seconds)) {
  def find[T, U, V](fullCollectionName: String, query: T, fields: Option[U], skip: Int, limit: Int)(implicit writer: BSONWriter[T], writer2: BSONWriter[U], handler: BSONReaderHandler, reader: BSONReader[V], m: Manifest[V]) :Future[Cursor[V]] = {
    val op = Query(0, fullCollectionName, skip, 19)
    val bson = writer.write(query)
    if(fields.isDefined)
      bson.writeBytes(writer2.write(fields.get))
    val message = WritableMessage(op, bson)
    
    connection.ask(message).map { response =>
      new Cursor(response, connection, op)
    }
  }
  
  def count[U](fullCollectionName: String)(implicit handler: BSONReaderHandler, reader: BSONReader[U], m: Manifest[U]) :Future[U] = {
    val message = Count.makeWritableMessage(fullCollectionName.span(_ != '.')._1, fullCollectionName, None, None, (new java.util.Random()).nextInt(Integer.MAX_VALUE))
    connection.ask(message).map { response =>
      handler.handle(response.reply, response.documents).next
    }
  }
  
  def insert[T](fullCollectionName: String, document: T)(implicit writer: BSONWriter[T]) :Unit = {
    val op = Insert(0, fullCollectionName)
    val bson = writer.write(document)
    val message = WritableMessage(op, bson)
    connection.send(message)
  }
  
  def insert[T, U](fullCollectionName: String, document: T, writeConcern: GetLastError)(implicit writer: BSONWriter[T], handler: BSONReaderHandler, reader: BSONReader[U], m: Manifest[U]) :Future[U] = {
    val op = Insert(0, fullCollectionName)
    val bson = writer.write(document)
    val message = WritableMessage(op, bson)
    connection.ask(message, writeConcern).map { response =>
      handler.handle(response.reply, response.documents).next
    }
  }
  
  def remove[T](fullCollectionName: String, query: T)(implicit writer: BSONWriter[T]) :Unit = remove(fullCollectionName, query, false)
  
  def remove[T](fullCollectionName: String, query: T, firstMatchOnly: Boolean)(implicit writer: BSONWriter[T]) : Unit = {
    val op = Delete(fullCollectionName, if(firstMatchOnly) 1 else 0)
    val bson = writer.write(query)
    val message = WritableMessage(op, bson)
    connection.send(message)
  }
  
  def remove[T, U](fullCollectionName: String, query: T, writeConcern: GetLastError, firstMatchOnly: Boolean = false)(implicit writer: BSONWriter[T], handler: BSONReaderHandler, reader: BSONReader[U], m: Manifest[U]) :Future[U] = {
    val op = Delete(fullCollectionName, if(firstMatchOnly) 1 else 0)
    val bson = writer.write(query)
    val message = WritableMessage(op, bson)
    connection.ask(message, writeConcern).map { response =>
      handler.handle(response.reply, response.documents).next
    }
  }
}

class Cursor[T](response: ReadReply, connection: MongoConnection, query: Query)(implicit handler: BSONReaderHandler, reader: BSONReader[T], timeout :Timeout) {
  lazy val iterator :Iterator[T] = handler.handle(response.reply, response.documents)
  def next :Option[Future[Cursor[T]]] = {
    if(hasNext) {
      println("cursor: call next")
      val op = GetMore(query.fullCollectionName, query.numberToReturn, response.reply.cursorID)
      Some(connection.ask(WritableMessage(op)).map { response => new Cursor(response, connection, query) })
    } else None
  }
  def hasNext :Boolean = response.reply.cursorID != 0
  def close = if(hasNext) {
    connection.send(WritableMessage(KillCursors(Set(response.reply.cursorID))))
  }
}

object Cursor {
  // for test purposes
  import akka.dispatch.Await
  def stream[T](cursor: Cursor[T])(implicit timeout :Timeout) :Stream[T] = {
    implicit val ec = MongoConnection.system.dispatcher
    if(cursor.iterator.hasNext) {
      Stream.cons(cursor.iterator.next, stream(cursor))
    } else if(cursor.hasNext) {
      stream(Await.result(cursor.next.get, timeout.duration))
    } else Stream.empty
  }

  import play.api.libs.iteratee._
  import play.api.libs.concurrent.{Promise => PlayPromise, _}


  def enumerate[T](futureCursor: Option[Future[Cursor[T]]]) :Enumerator[T] = {
    var currentCursor :Option[Cursor[T]] = None
    Enumerator.fromCallback { () =>
      if(currentCursor.isDefined && currentCursor.get.iterator.hasNext){
        println("enumerate: give next element from iterator")
        PlayPromise.pure(Some(currentCursor.get.iterator.next))
      } else if(currentCursor.isDefined && currentCursor.get.hasNext) {
        println("enumerate: fetching next cursor")
        new AkkaPromise(currentCursor.get.next.get.map { cursor =>
          println("redeemed from next cursor")
          currentCursor = Some(cursor)
          Some(cursor.iterator.next)
        })
      } else if(!currentCursor.isDefined && futureCursor.isDefined) {
        println("enumerate: fetching from first future")
        new AkkaPromise(futureCursor.get.map { cursor =>
          println("redeemed from first cursor")
          currentCursor = Some(cursor)
          Some(cursor.iterator.next)
        })
      } else PlayPromise.pure(None)
    }
  }
}

class DB(
  val name: String,
  val nodes: List[(String, Int)] = List("localhost" -> 27017)
) {
  
}

trait BSONWriter[DocumentType] {
  def write(document: DocumentType) :ChannelBuffer
}

trait BSONReader[DocumentType] {
  def read(buffer: ChannelBuffer) :DocumentType
}

trait BSONReaderHandler {
  def handle[DocumentType](reply: Reply, buffer: ChannelBuffer)(implicit reader: BSONReader[DocumentType]) :Iterator[DocumentType]
}

import org.asyncmongo.bson._

object DefaultBSONHandlers {
  import de.undercouch.bson4jackson._
  import de.undercouch.bson4jackson.io._
  import de.undercouch.bson4jackson.uuid._
  import org.codehaus.jackson.map.ObjectMapper

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
}

/*object DefaultBSONHandlers {
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
}*/


import java.util.HashMap
import DefaultBSONHandlers._

object Test { //Mongo.find[HashMap[Object, Object], HashMap[Object, Object]]("plugin.acoll", query, 2)
  import akka.dispatch.Await
  import akka.pattern.ask
  import akka.util.Timeout
  import akka.util.duration._

  import play.api.libs.iteratee._
   
  implicit val timeout = Timeout(5 seconds)

  def test = {
    val mongo = Mongo(MongoConnection(List("localhost" -> 27017)))
    val query = new Bson()//new HashMap[Object, Object]()
    query.writeElement("name", "Jack")
    val future = mongo.find("plugin.acoll", query, None, 2, 0)
    println("Test: future is " + future)
    //Cursor.stream(Await.result(future, timeout.duration)).print("\n")
    Cursor.enumerate(Some(future))(Iteratee.foreach { t =>
      println("fetched t=" + DefaultBSONIterator.pretty(t))
    })
  }
}