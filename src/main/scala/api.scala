package org.asyncmongo.api

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.dispatch.Future
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import org.asyncmongo.actors.MongoDBSystem
import org.asyncmongo.bson._
import org.asyncmongo.handlers._
import org.asyncmongo.protocol._
import org.asyncmongo.protocol.messages._
import org.asyncmongo.actors.Authenticate
import akka.actor.ActorSystem$

case class DB(dbName: String, connection: MongoConnection, implicit val timeout :Timeout = Timeout(5 seconds)) {
  def apply(name: String) :Collection = Collection(dbName, name, connection, timeout)

  def authenticate(user: String, password: String) :Future[Boolean] = connection.authenticate(dbName, user, password)
}

case class Collection(
  dbName: String,
  collectionName: String,
  connection: MongoConnection,
  implicit val timeout :Timeout
) {
  lazy val fullCollectionName = dbName + "." + collectionName

  def count :Future[Int] = {
    import DefaultBSONHandlers._
    connection.ask(Count(dbName, collectionName).maker).map { response =>
      DefaultBSONReaderHandler.handle(response.reply, response.documents).next.find(_.name == "n").get match {
        case BSONDouble(_, n) => n.toInt
        case _ => 0
      }
    }
  }

  def find[T, U, V](query: T, fields: Option[U], skip: Int, limit: Int, flags: Int = 0)(implicit writer: BSONWriter[T], writer2: BSONWriter[U], handler: BSONReaderHandler, reader: BSONReader[V]) :Future[Cursor[V]] = {
    val op = Query(flags, fullCollectionName, skip, 10) // TODO: test-purpose number of docs to return, remove
    val bson = writer.write(query)
    if(fields.isDefined)
      bson.writeBytes(writer2.write(fields.get))
    val message = RequestMaker(op, bson)
    
    connection.ask(message).map { response =>
      new Cursor(response, connection, op)
    }
  }
  
  def insert[T](document: T)(implicit writer: BSONWriter[T]) :Unit = {
    val op = Insert(0, fullCollectionName)
    val bson = writer.write(document)
    val message = RequestMaker(op, bson)
    connection.send(message)
  }
  
  def insert[T](document: T, writeConcern: GetLastError)(implicit writer: BSONWriter[T], handler: BSONReaderHandler) :Future[LastError] = {
    val op = Insert(0, fullCollectionName)
    val bson = writer.write(document)
    val message = RequestMaker(op, bson)
    connection.ask(message, writeConcern).map { response =>
      println(response.reply.stringify)
      import DefaultBSONHandlers._
      LastError(response)
    }
  }
  
  def remove[T](query: T)(implicit writer: BSONWriter[T]) :Unit = remove(query, false)
  
  def remove[T](query: T, firstMatchOnly: Boolean)(implicit writer: BSONWriter[T]) : Unit = {
    val op = Delete(fullCollectionName, if(firstMatchOnly) 1 else 0)
    val bson = writer.write(query)
    val message = RequestMaker(op, bson)
    connection.send(message)
  }
  
  def remove[T, U](query: T, writeConcern: GetLastError, firstMatchOnly: Boolean = false)(implicit writer: BSONWriter[T], handler: BSONReaderHandler, reader: BSONReader[U], m: Manifest[U]) :Future[U] = {
    val op = Delete(fullCollectionName, if(firstMatchOnly) 1 else 0)
    val bson = writer.write(query)
    val message = RequestMaker(op, bson)
    connection.ask(message, writeConcern).map { response =>
      handler.handle(response.reply, response.documents).next
    }
  }
}

class Cursor[T](response: Response, connection: MongoConnection, query: Query)(implicit handler: BSONReaderHandler, reader: BSONReader[T], timeout :Timeout) {
  lazy val iterator :Iterator[T] = handler.handle(response.reply, response.documents)
  def next :Option[Future[Cursor[T]]] = {
    if(hasNext) {
      println("cursor: call next")
      val op = GetMore(query.fullCollectionName, query.numberToReturn, response.reply.cursorID)
      Some(connection.ask(RequestMaker(op).copy(channelIdHint=Some(response.info.channelId))).map { response => new Cursor(response, connection, query) })
    } else None
  }
  def hasNext :Boolean = response.reply.cursorID != 0
  def close = if(hasNext) {
    connection.send(RequestMaker(KillCursors(Set(response.reply.cursorID))))
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
          if(cursor.iterator.hasNext) {
            println("has result")
            Some(cursor.iterator.next)
          }
          else {
            println("no result")
            None
          }
        })
      } else {
        println("Nothing to enumerate")
        PlayPromise.pure(None)
      }
    }
  }
}

class MongoConnection(
  val mongosystem: ActorRef
) {
  /** write an op and wait for db response */
  def ask(message: RequestMaker)(implicit timeout: Timeout) :Future[Response] = {
    (mongosystem ? message).mapTo[Response]
  }

  /** write a no-response op followed by a GetLastError command and wait for its response */
  def ask(message: RequestMaker, writeConcern: GetLastError)(implicit timeout: Timeout) = {
    (mongosystem ? ((message, writeConcern.maker))).mapTo[Response] // Broken
  }

  /** write a no-response op without getting a future */
  def send(message: RequestMaker) = mongosystem ! message

  /** authenticate on the given db. */
  def authenticate(db: String, user: String, password: String)(implicit timeout: Timeout) :Future[Boolean] = {
    (mongosystem ? Authenticate(db, user, password)).mapTo[Boolean]
  }

  def stop = MongoConnection.system.stop(mongosystem)
}

object MongoConnection {
  import com.typesafe.config.ConfigFactory
  val config = ConfigFactory.load()

  val system = ActorSystem("mongodb", config.getConfig("mongo-async-driver"))

  def apply(nodes: List[String], authentications :List[Authenticate] = List.empty, name: Option[String]= None) = {
    val props = Props(new MongoDBSystem(nodes, authentications))
    new MongoConnection(if(name.isDefined) system.actorOf(props, name = name.get) else system.actorOf(props))
  }
}

object Test {
  import akka.dispatch.Await
  import akka.pattern.ask
  import DefaultBSONHandlers._
  import play.api.libs.iteratee._
   
  implicit val timeout = Timeout(5 seconds)

  def test = {
    val connection = MongoConnection(List("localhost:27016"))//, List(Authenticate("plugin", "jack", "toto")))
    val fut = connection.authenticate("plugin", "jack", "toto")
    fut.onComplete { case yop => println("auth completed for jack " + yop) }
    val db = DB("plugin", connection)
    /*MongoConnection.system.scheduler.scheduleOnce(1000 milliseconds) {
      db.authenticate("jack", "toto")
    }*/
    val collection = db("acoll")
    /*val query = new Bson()//new HashMap[Object, Object]()
    connection.ask(Request(GetMore("plugin.acoll", 2, 12))).onComplete {
      case Right(response) =>
      println()
      println("!! \t Wrong GetMore gave " + response.reply.stringify + "\n\t\t ==> " + response.error)
      println()
      case _ =>
    }
    query.write(BSONString("name", "Jack"))
    val future = collection.find(query, None, 2, 0)
    collection.count.onComplete {
      case Left(t) =>
      case Right(t) => println("count on plugin.acoll gave " + t)
    }
    println("Test: future is " + future)*/
    val tags = Bson(
      BSONString("tag1", "yop"),
      BSONString("tag2", "..."))
    val toSave = Bson(
      BSONString("name", "Kurt"),
      BSONDocument("tags", tags.getBuffer))
    //toSave.write(BSONString("$kk", "Kurt"))
    //Cursor.stream(Await.result(future, timeout.duration)).print("\n")
    /*Cursor.enumerate(Some(future))(Iteratee.foreach { t =>
      println("fetched t=" + DefaultBSONIterator.pretty(t))
    })*/
      MongoConnection.system.scheduler.scheduleOnce(2000 milliseconds) {
        connection.authenticate("plugin", "franck", "toto").onComplete { case yop => println("auth completed for franck " + yop) }
        collection.insert(toSave, GetLastError("plugin", false, None, false)).onComplete {
          case Left(t) => { println("error!, throwable\n\t\t"); t.printStackTrace; println("\n\t for insert=" + toSave) }
          case Right(le) => {
            println("insertion " + (if(le.inError) "failed" else "succeeded") + ": " + le.stringify)
          }
        }
        connection.ask(ReplStatus.maker).onComplete {
          case Left(t) => println("error!, throwable = " + t)
          case Right(message) => {
            println("ReplStatus " + DefaultBSONIterator.pretty(DefaultBSONReaderHandler.handle(message.reply, message.documents).next))
          }
        }
        connection.ask(IsMaster().maker).onComplete {
          case Left(t) => println("error!, throwable = " + t)
          case Right(message) => {
            println("IsMaster " + DefaultBSONIterator.pretty(DefaultBSONReaderHandler.handle(message.reply, message.documents).next))
          }
        }
    }
    /*for(i <- 0 until 1) {
      println()
      Cursor.enumerate(Some(collection.find(new Bson(), None, 0, 0)))(Iteratee.foreach { t =>
        println("fetched t=" + DefaultBSONIterator.pretty(t))
        //print(".")
      })
      println()
    }
    MongoConnection.system.scheduler.scheduleOnce(7000 milliseconds) {
      println("sending scheduled message...")
      //connection.mongosystem ! IsMaster("plugin").makeRequest
      Cursor.enumerate(Some(collection.find(new Bson(), None, 0, 0)))(Iteratee.foreach { t =>
        //println("fetched t=" + DefaultBSONIterator.pretty(t))
      })
    }*/

  }
}