package org.asyncmongo.api

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.dispatch.Future
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import org.asyncmongo.actors.{Authenticate, MongoDBSystem, MonitorActor}
import org.asyncmongo.bson._
import org.asyncmongo.handlers._
import org.asyncmongo.protocol._
import org.asyncmongo.protocol.commands.{Update => FindAndModifyUpdate, _}
import org.slf4j.{Logger, LoggerFactory}

/**
 * A Mongo Database.
 *
 * @param dbName database name.
 * @param connection the [[org.asyncmongo.api.MongoConnection]] that will be used to query this database.
 * @param timeout the default timeout for the queries. Defaults to 5 seconds.
 */
case class DB(dbName: String, connection: MongoConnection, implicit val timeout :Timeout = Timeout(5 seconds)) { // TODO timeout
  /**  Gets a [[org.asyncmongo.api.Collection]] from this database. */
  def apply(name: String) :Collection = Collection(dbName, name, connection, timeout)

  /** Authenticates the connection on this database. */ // TODO return type
  def authenticate(user: String, password: String) :Future[Map[String, BSONElement]] = connection.authenticate(dbName, user, password)
}

/**
 * A Mongo Collection.
 *
 * @param dbName database name.
 * @param collectionName the name of this collection.
 * @param connection the [[org.asyncmongo.api.MongoConnection]] that will be used to query this database.
 * @param timeout the default timeout for the queries.
 */
case class Collection(
  dbName: String,
  collectionName: String,
  connection: MongoConnection,
  implicit val timeout :Timeout
) {
  /** The full collection name. */
  lazy val fullCollectionName = dbName + "." + collectionName

  // TODO
  /** Counts the number of documents in this collection. */
  def count() :Future[Int] = {
    import DefaultBSONHandlers._
    connection.ask(Count(collectionName)(dbName).maker).map { response =>
      DefaultBSONReaderHandler.handle(response.reply, response.documents).next.find(_.name == "n").get match {
        case ReadBSONElement(_, BSONDouble(n)) => n.toInt
        case _ => 0
      }
    }
  }

  /**
   * Find the documents matching the given criteria.
   *
   * Example:
   * {{{
   * import org.asyncmongo.api._
import org.asyncmongo.bson._
import org.asyncmongo.handlers.DefaultBSONHandlers._
import play.api.libs.iteratee.Iteratee

object Samples {

  def listDocs() = {
    val connection = MongoConnection( List( "localhost:27016" ) )
    val db = DB("plugin", connection)
    val collection = db("acoll")

    // get a Future[Cursor[DefaultBSONIterator]]
    val futureCursor = collection.find(Bson(BSONString("name", "Jack")))

    // let's enumerate this cursor and print a readable representation of each document in the response
    Cursor.enumerate(futureCursor)(Iteratee.foreach { doc =>
      println("found document: " + DefaultBSONIterator.pretty(doc))
    })
  }
}
   * }}}
   *
   * This method accepts any query and fields object, provided that there is an implicit [[org.asyncmongo.handlers.BSONWriter]] typeclass for handling them in the scope.
   * You can use the typeclasses defined in [[org.asyncmongo.handlers.DefaultBSONHandlers]] object.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam T the type of the query. An implicit [[org.asyncmongo.handlers.BSONWriter]][T] typeclass for handling it has to be in the scope.
   * @tparam U the type of the fields object. An implicit [[org.asyncmongo.handlers.BSONWriter]][U] typeclass for handling it has to be in the scope.
   * @tparam V the type of the matched documents. An implicit [[org.asyncmongo.handlers.BSONReader]][V] typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   * @param fields Get only a subset of each matched documents. Defaults to None.
   * @param skip A number of documents to skip. Defaults to 0.
   * @param limit An upper limit on the number of documents to retrieve. Defaults to 0 (meaning no upper limit).
   * @param flags Optional query flags.
   *
   * @return a future cursor of documents. You can get an enumerator for it, please see the [[org.asyncmongo.api.Cursor]] companion object.
   */
  def find[T, U, V](query: T, fields: Option[U] = None, skip: Int = 0, limit: Int = 0, flags: Int = 0)(implicit writer: BSONWriter[T], writer2: BSONWriter[U], handler: BSONReaderHandler, reader: BSONReader[V]) :Future[Cursor[V]] = {
    val op = Query(flags, fullCollectionName, skip, limit)
    val bson = writer.write(query)
    if(fields.isDefined)
      bson.writeBytes(writer2.write(fields.get))
    val message = RequestMaker(op, bson)

    connection.ask(message).map { response =>
      new Cursor(response, connection, op)
    }
  }

  /**
   * Inserts a document into the collection without writeConcern.
   *
   * Please note that you cannot be sure that the document has been effectively written and when (hence the Unit return type).
   *
   * @tparam T the type of the document to insert. An implicit [[org.asyncmongo.handlers.BSONWriter]][T] typeclass for handling it has to be in the scope.
   *
   * @param document the document to insert.
   */
  def uncheckedInsert[T](document: T)(implicit writer: BSONWriter[T]) :Unit = {
    val op = Insert(0, fullCollectionName)
    val bson = writer.write(document)
    val message = RequestMaker(op, bson)
    connection.send(message)
  }

  /**
   * Inserts a document into the collection and wait for the [[org.asyncmongo.protocol.commands.LastError]] result.
   *
   * Please read the documentation about [[org.asyncmongo.protocol.commands.GetLastError]] to know how to use it properly.
   *
   * @tparam T the type of the document to insert. An implicit [[org.asyncmongo.handlers.BSONWriter]][T] typeclass for handling it has to be in the scope.
   *
   * @param document the document to insert.
   * @param writeConcern the [[org.asyncmongo.protocol.commands.GetLastError]] command message to send in order to control how the document is inserted. Defaults to GetLastError().
   *
   * @return a future [[org.asyncmongo.protocol.commands.LastError]] that can be used to check whether the insertion was successful.
   */
  def insert[T](document: T, writeConcern: GetLastError = GetLastError())(implicit writer: BSONWriter[T]) :Future[LastError] = {
    val op = Insert(0, fullCollectionName)
    val bson = writer.write(document)
    val checkedWriteRequest = CheckedWriteRequest(op, bson, writeConcern)
    connection.ask(checkedWriteRequest).map { response =>
      LastError(response)
    }
  }

  /**
   * Updates one or more documents matching the given selector with the given modifier or update object.
   *
   * Please note that you cannot be sure that the matched documents have been effectively updated and when (hence the Unit return type).
   *
   * @tparam S the type of the selector object. An implicit [[org.asyncmongo.handlers.BSONWriter]][S] typeclass for handling it has to be in the scope.
   * @tparam U the type of the modifier or update object. An implicit [[org.asyncmongo.handlers.BSONWriter]][U] typeclass for handling it has to be in the scope.
   *
   * @param selector the selector object, for finding the documents to update.
   * @param update the modifier object (with special keys like \$set) or replacement object.
   * @param upsert states whether the update objet should be inserted if no match found. Defaults to false.
   * @param multi states whether the update may be done on all the matching documents.
   */
  def uncheckedUpdate[S, U](selector: S, update: U, upsert: Boolean = false, multi: Boolean = false)(implicit selectorWriter: BSONWriter[S], updateWriter: BSONWriter[U]) :Unit = {
    val flags = 0 | (if(upsert) UpdateFlags.Upsert else 0) | (if(multi) UpdateFlags.MultiUpdate else 0)
    val op = Update(fullCollectionName, flags)
    val bson = selectorWriter.write(selector)
    bson.writeBytes(updateWriter.write(update))
    val message = RequestMaker(op, bson)
    connection.send(message)
  }

  /**
   * Updates one or more documents matching the given selector with the given modifier or update object.
   *
   * @tparam S the type of the selector object. An implicit [[org.asyncmongo.handlers.BSONWriter]][S] typeclass for handling it has to be in the scope.
   * @tparam U the type of the modifier or update object. An implicit [[org.asyncmongo.handlers.BSONWriter]][U] typeclass for handling it has to be in the scope.
   *
   * @param selector the selector object, for finding the documents to update.
   * @param update the modifier object (with special keys like \$set) or replacement object.
   * @param writeConcern the [[org.asyncmongo.protocol.commands.GetLastError]] command message to send in order to control how the documents are updated. Defaults to GetLastError().
   * @param upsert states whether the update objet should be inserted if no match found. Defaults to false.
   * @param multi states whether the update may be done on all the matching documents.
   *
   * @return a future [[org.asyncmongo.protocol.commands.LastError]] that can be used to check whether the update was successful.
   */
  def update[S, U](selector: S, update: U, writeConcern: GetLastError = GetLastError(), upsert: Boolean = false, multi: Boolean = false)(implicit selectorWriter: BSONWriter[S], updateWriter: BSONWriter[U]) :Future[LastError] = {
    val flags = 0 | (if(upsert) UpdateFlags.Upsert else 0) | (if(multi) UpdateFlags.MultiUpdate else 0)
    val op = Update(fullCollectionName, flags)
    val bson = selectorWriter.write(selector)
    bson.writeBytes(updateWriter.write(update))
    val message = CheckedWriteRequest(op, bson, writeConcern)
    connection.ask(message).map { response =>
      LastError(response)
    }
  }

  /**
   * Remove the matched document(s) from the collection without writeConcern.
   *
   * Please note that you cannot be sure that the matched documents have been effectively removed and when (hence the Unit return type).
   *
   * @tparam T the type of the selector of documents to remove. An implicit [[org.asyncmongo.handlers.BSONWriter]][T] typeclass for handling it has to be in the scope.
   *
   * @param query the selector of documents to remove.
   * @param firstMatchOnly states whether only the first matched documents has to be removed from this collection.
   */
  def uncheckedRemove[T](query: T, firstMatchOnly: Boolean = false)(implicit writer: BSONWriter[T]) : Unit = {
    val op = Delete(fullCollectionName, if(firstMatchOnly) 1 else 0)
    val bson = writer.write(query)
    val message = RequestMaker(op, bson)
    connection.send(message)
  }

  /**
   * Remove the matched document(s) from the collection and wait for the [[org.asyncmongo.protocol.commands.LastError]] result.
   *
   * Please read the documentation about [[org.asyncmongo.protocol.commands.GetLastError]] to know how to use it properly.
   *
   * @tparam T the type of the selector of documents to remove. An implicit [[org.asyncmongo.handlers.BSONWriter]][T] typeclass for handling it has to be in the scope.
   *
   * @param query the selector of documents to remove.
   * @param writeConcern the [[org.asyncmongo.protocol.commands.GetLastError]] command message to send in order to control how the documents are removed. Defaults to GetLastError().
   * @param firstMatchOnly states whether only the first matched documents has to be removed from this collection.
   *
   * @return a future [[org.asyncmongo.protocol.commands.LastError]] that can be used to check whether the removal was successful.
   */
  def remove[T](query: T, writeConcern: GetLastError = GetLastError(), firstMatchOnly: Boolean = false)(implicit writer: BSONWriter[T]) :Future[LastError] = {
    val op = Delete(fullCollectionName, if(firstMatchOnly) 1 else 0)
    val bson = writer.write(query)
    val checkedWriteRequest = CheckedWriteRequest(op, bson, writeConcern)
    connection.ask(checkedWriteRequest).map { response =>
      LastError(response)
    }
  }

  /**
   * Sends a command and get the future result of the command.
   *
   * @param command The command to send.
   *
   * @return a future containing the result of the command.
   */ // TODO move into DB
  def command(command: Command) :Future[command.Result] =
    connection.ask(command.apply(dbName).maker).map(command.ResultMaker(_))
}

object Collection {
  private val logger = LoggerFactory.getLogger("Collection")
}

/**
 * Allows to fetch the next documents matching a query.
 *
 * Please note that you should not use Cursor directly.
 * You are invited to use the enumerator/iteratee pattern to handle Cursor operations.
 * You may take a look to {{{Cursor.enumerate[T](futureCursor: Future[Cursor[T]]) :Enumerator[T]}}} to produce an enumerator and consume the documents using an iteratee on your own.
 *
 * Example:
 * {{{
 * import org.asyncmongo.api._
import org.asyncmongo.bson._
import org.asyncmongo.handlers.DefaultBSONHandlers._
import play.api.libs.iteratee.Iteratee

object Samples {

  def listDocs() = {
    val connection = MongoConnection( List( "localhost:27016" ) )
    val db = DB("plugin", connection)
    val collection = db("acoll")

    // get a Future[Cursor[DefaultBSONIterator]]
    val futureCursor = collection.find(Bson(BSONString("name", "Jack")))

    // let's enumerate this cursor and print a readable representation of each document in the response
    Cursor.enumerate(futureCursor)(Iteratee.foreach { doc =>
      println("found document: " + DefaultBSONIterator.pretty(doc))
    })
  }
}
}}}
 *
 * It is worth diving into the [[https://github.com/playframework/Play20/wiki/Iteratees Play! 2.0 Iteratee documentation]].
 *
 * @tparam T the type of the matched documents. An implicit [[org.asyncmongo.handlers.BSONReader]][T] typeclass for handling it has to be in the scope.
 *
 * @param response The response to handle.
 * @param connection The connection that must be used to fetch the next documents.
 * @param query The original query.
 */
class Cursor[T](response: Response, connection: MongoConnection, query: Query)(implicit handler: BSONReaderHandler, reader: BSONReader[T], timeout :Timeout) {
  import Cursor.logger
  /** An iterator on the last fetched documents. */
  lazy val iterator :Iterator[T] = handler.handle(response.reply, response.documents)
  /** Gets the next instance of that cursor. */
  def next :Option[Future[Cursor[T]]] = {
    if(hasNext) {
      logger.debug("cursor: calling next on " + response.reply.cursorID)
      val op = GetMore(query.fullCollectionName, query.numberToReturn, response.reply.cursorID)
      Some(connection.ask(RequestMaker(op).copy(channelIdHint=Some(response.info.channelId))).map { response => new Cursor(response, connection, query) })
    } else None
  }
  /** Tells if another instance of cursor can be fetched. */ // TODO redundant!
  def hasNext :Boolean = response.reply.cursorID != 0
  /** Explicitly closes that cursor. */
  def close = if(hasNext) {
    connection.send(RequestMaker(KillCursors(Set(response.reply.cursorID))))
  }
}

object Cursor {
  private val logger = LoggerFactory.getLogger("Cursor")
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

  /**
   * Enumerates the given future cursor.
   *
   * Example:
   * {{{
import org.asyncmongo.api._
import org.asyncmongo.bson._
import org.asyncmongo.handlers.DefaultBSONHandlers._
import play.api.libs.iteratee.Iteratee

object Samples {

  def listDocs() = {
    val connection = MongoConnection( List( "localhost:27016" ) )
    val db = DB("plugin", connection)
    val collection = db("acoll")

    // get a Future[Cursor[DefaultBSONIterator]]
    val futureCursor = collection.find(Bson(BSONString("name", "Jack")))

    // let's enumerate this cursor and print a readable representation of each document in the response
    Cursor.enumerate(futureCursor)(Iteratee.foreach { doc =>
      println("found document: " + DefaultBSONIterator.pretty(doc))
    })
  }
}
}}}
   *
   * It is worth diving into the [[https://github.com/playframework/Play20/wiki/Iteratees Play! 2.0 Iteratee documentation]].
   *
   * @tparam T the type of the matched documents. An implicit [[org.asyncmongo.handlers.BSONReader]][T] typeclass for handling it has to be in the scope.
   */
  def enumerate[T](futureCursor: Future[Cursor[T]]) :Enumerator[T] =
    Enumerator.flatten(futureCursor.map { cursor =>
      Enumerator.unfoldM(cursor) { cursor =>
        if(cursor.iterator.hasNext)
          PlayPromise.pure(Some((cursor,Some(cursor.iterator.next))))
        else if (cursor.hasNext)
          cursor.next.get.asPromise.map(c => Some((c,None)))
        else
          PlayPromise.pure(None)
      }
    }.asPromise) &> Enumeratee.collect { case Some(e) => e }
}

/**
 * A Mongo Connection.
 *
 * This is a wrapper around a reference to a [[org.asyncmongo.actors.MongoDBSystem]] Actor.
 * Connection here does not mean that there is one open channel to the server.
 * Behind the scene, many connections (channels) are open on all the available servers in the replica set.
 *
 * @param mongosystem A reference to a [[org.asyncmongo.actors.MongoDBSystem]] Actor.
 */
class MongoConnection(
  val mongosystem: ActorRef,
  monitor: ActorRef
) {
  /**
   * Get a future that will be successful when a primary node is available or times out.
   */
  def waitForPrimary(waitForAvailability: Timeout) :Future[_] = {
    monitor.ask(org.asyncmongo.actors.WaitForPrimary)(waitForAvailability)
  }

  /**
   * Writes a request and wait for a response.
   *
   * The implicit timeout concerns the time of execution on the database.
   * If no suitable node for handling this request is available, the returned future will be in error.
   *
   * @param message The request maker.
   *
   * @return The future response.
   */
  def ask(message: RequestMaker)(implicit timeout: Timeout) :Future[Response] = {
    (mongosystem ? message).mapTo[Response]
  }

  /**
   * Writes a request and wait for a response, waiting for a suitable node or times out.
   *
   * The implicit timeout concerns the time of execution on the database.
   *   - waitForAvailability is the maximum amount of time that will be spent expecting a suitable node for this request.
   *   - dbTimemout is the maximum amount of time that will be spent expecting a response from a database.
   *
   * @param message The request maker.
   * @param waitForAvailability wait for a suitable node up to the given timeout.
   *
   * @return The future response.
   */
  def ask(message: RequestMaker, waitForAvailability: Timeout)(implicit dbTimeout: Timeout) :Future[Response] = {
    monitor.ask("primary")(waitForAvailability).flatMap { s=>
      (mongosystem ? message).mapTo[Response]
    }
  }

  /**
   * Writes a checked write request and wait for a response.
   *
   * @param message The request maker.
   *
   * @return The future response.
   */
  def ask(checkedWriteRequest: CheckedWriteRequest)(implicit timeout: Timeout) = {
    (mongosystem ? checkedWriteRequest).mapTo[Response]
  }

  /**
   * Writes a checked write request and wait for a response, waiting for a primary node or times out.
   *
   * The implicit timeout concerns the time of execution on the database.
   *   - waitForAvailability is the maximum amount of time that will be spent expecting a primary node for this request.
   *   - dbTimemout is the maximum amount of time that will be spent expecting a response from the database.
   *
   * @param message The request maker.
   * @param waitForAvailability wait for a primary node up to the given timeout.
   *
   * @return The future response.
   */
  def ask(checkedWriteRequest: CheckedWriteRequest, waitForAvailability: Timeout)(implicit dbTimeout: Timeout) :Future[Response] = {
    monitor.ask("primary")(waitForAvailability).flatMap { s=>
      (mongosystem ? checkedWriteRequest).mapTo[Response]
    }
  }

  /**
   * Writes a request and drop the response if any.
   *
   * @param message The request maker.
   */
  def send(message: RequestMaker) = mongosystem ! message

  /** Authenticates the connection on the given database. */ // TODO return type
  def authenticate(db: String, user: String, password: String)(implicit timeout: Timeout) :Future[Map[String, BSONElement]] = {
    (mongosystem ? Authenticate(db, user, password)).mapTo[Map[String, BSONElement]]
  }

  def close(implicit timeout: Timeout) :Future[String] = (mongosystem ? org.asyncmongo.actors.Close).mapTo[String]

  def stop = MongoConnection.system.stop(mongosystem)
}

object MongoConnection {
  import com.typesafe.config.ConfigFactory
  val config = ConfigFactory.load()

  /**
   * The actor system that creates all the required actors.
   */
  val system = ActorSystem("mongodb", config.getConfig("mongo-async-driver"))

  /**
   * Creates a new MongoConnection.
   *
   * @param nodes A list of node names, like ''node1.foo.com:27017''. Port is optional, it is 27017 by default.
   * @param authentications A list of Authenticates.
   * @param name The name of the newly created [[org.asyncmongo.actors.MongoDBSystem]] actor, if needed.
   */
  def apply(nodes: List[String], authentications :List[Authenticate] = List.empty, name: Option[String]= None) = {
    val props = Props(new MongoDBSystem(nodes, authentications))
    val mongosystem = if(name.isDefined) system.actorOf(props, name = name.get) else system.actorOf(props)
    val monitor = system.actorOf(Props(new MonitorActor(mongosystem)))
    new MongoConnection(mongosystem, monitor)
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
    val db = DB("plugin", connection)
    val collection = db("acoll")
    db.authenticate("plop", "toto")
    db.authenticate("jack", "toto").onComplete {
      case yop => {
        println("auth completed for jack " + yop)

        val toSave = Bson("name" -> BSONString("Kurt"))
        collection.insert(toSave, GetLastError(false, None, false)).onComplete {
          case Left(t) => { println("error!, throwable\n\t\t"); t.printStackTrace; println("\n\t for insert=" + toSave) }
          case Right(le) => {
            println("insertion " + (if(le.inError) "failed" else "succeeded") + ": " + le.stringify)
          }
        }
        db.connection.ask(FindAndModify(
            "acoll",
            Bson("name" -> BSONString("Jack")),
            FindAndModifyUpdate(Bson("$set" -> BSONDocument(Bson("name" -> BSONString("JACK")).makeBuffer)), false)
        )(db.dbName).maker).onComplete {
          case Right(response) => println("FINDANDMODIFY gave " + DefaultBSONIterator.pretty(DefaultBSONHandlers.parse(response).next))
        }
        collection.command(FindAndModify(
            "acoll",
            Bson("name" -> BSONString("Jack")),
            FindAndModifyUpdate(Bson("$set" -> BSONDocument(Bson("name" -> BSONString("JACK")).makeBuffer)), false)
        )).onComplete {
          case Right(Some(doc)) => println("FINDANDMODIFY #2 gave " + DefaultBSONIterator.pretty(DefaultBSONReader.read(doc.value)))
          case Right(_) => println("FINDANDMODIFY #2 gave no value")
          case Left(response) => println("FINDANDMODIFY ERROR #2")
        }
      }
    }
    //connection.authenticate("plugin", "franck", "toto").onComplete { case yop => println("auth completed for franck " + yop) }
    /*MongoConnection.system.scheduler.scheduleOnce(1000 milliseconds) {
      db.authenticate("jack", "toto")
    }*/

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
      "tag1" -> BSONString("yop"),
      "tag2" -> BSONString("..."))
    val toSave = Bson(
      "name" -> BSONString("Kurt"),
      "tags" -> BSONDocument(tags.makeBuffer))
    //toSave.write(BSONString("$kk", "Kurt"))
    //Cursor.stream(Await.result(future, timeout.duration)).print("\n")
    /*Cursor.enumerate(Some(future))(Iteratee.foreach { t =>
      println("fetched t=" + DefaultBSONIterator.pretty(t))
    })*/
      /* MongoConnection.system.scheduler.scheduleOnce(2000 milliseconds) {
        connection.authenticate("plugin", "franck", "toto").onComplete { case yop => println("auth completed for franck " + yop) }
        collection.insert(toSave, GetLastError(false, None, false)).onComplete {
          case Left(t) => { println("error!, throwable\n\t\t"); t.printStackTrace; println("\n\t for insert=" + toSave) }
          case Right(le) => {
            println("insertion " + (if(le.inError) "failed" else "succeeded") + ": " + le.stringify)
          }
        }
        connection.ask(ReplStatus().maker).onComplete {
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
    } */
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