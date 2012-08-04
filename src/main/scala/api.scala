package org.asyncmongo.api

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.util.Duration
import scala.concurrent.util.duration._

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
case class DB(dbName: String, connection: MongoConnection)(implicit timeout :Duration = 5 seconds, context: ExecutionContext) { // TODO timeout
  /**  Gets a [[org.asyncmongo.api.Collection]] from this database. */
  def apply(name: String) :Collection = Collection(dbName, name, connection)(timeout, context)

  /** Authenticates the connection on this database. */ // TODO return type
  def authenticate(user: String, password: String) :Future[AuthenticationResult] = connection.authenticate(dbName, user, password)
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
  connection: MongoConnection
)(implicit timeout: Duration, context: ExecutionContext) {
  /** The full collection name. */
  lazy val fullCollectionName = dbName + "." + collectionName

  // TODO
  /** Counts the number of documents in this collection. */
  def count() :Future[Int] = {
    import DefaultBSONHandlers._
    connection.ask(Count(collectionName)(dbName).maker).map { response =>
      DefaultBSONReaderHandler.handle(response.reply, response.documents).next.getAs[BSONDouble]("n").map(_.value.toInt).getOrElse(0)
    }
  }

  /**
   * Find the documents matching the given criteria.
   *
   * Example:
   * {{{
import org.asyncmongo.api._
import org.asyncmongo.bson._
import org.asyncmongo.handlers.DefaultBSONHandlers._
import play.api.libs.iteratee.Iteratee

object Samples {

  val connection = MongoConnection( List( "localhost:27016" ) )
  val db = DB("plugin", connection)
  val collection = db("acoll")

  def listDocs() = {
    // select only the documents which field 'name' equals 'Jack'
    val query = Bson("name" -> BSONString("Jack"))
    // select only the field 'name'
    val filter = Bson(
      "name" -> BSONInteger(1),
      "_id" -> BSONInteger(0)
    )

    // get a Cursor[DefaultBSONIterator]
    val cursor = collection.find(query, Some(filter))
    // let's enumerate this cursor and print a readable representation of each document in the response
    cursor.enumerate.apply(Iteratee.foreach { doc =>
      println("found document: " + DefaultBSONIterator.pretty(doc))
    })

    // or, the same with getting a list
    val cursor2 = collection.find(query, Some(filter))
    val list = cursor.toList
  }
}
}}}
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
  def find[T, U, V](query: T, fields: Option[U] = None, skip: Int = 0, limit: Int = 0, flags: Int = 0)(implicit writer: BSONWriter[T], writer2: BSONWriter[U], handler: BSONReaderHandler, reader: BSONReader[V]) :FlattenedCursor[V] = {
    val op = Query(flags, fullCollectionName, skip, limit)
    val bson = writer.write(query)
    if(fields.isDefined)
      bson.writeBytes(writer2.write(fields.get))
    val message = RequestMaker(op, bson)

    Cursor.flatten(connection.ask(message).map { response =>
      new DefaultCursor(response, connection, op)
    })
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
   * Please that after invoking some Cursor methods, this Cursor instance should not be reused as it may cause unexpected behavior.
   *
   * Example:
   * {{{
import org.asyncmongo.api._
import org.asyncmongo.bson._
import org.asyncmongo.handlers.DefaultBSONHandlers._
import play.api.libs.iteratee.Iteratee

object Samples {

  val connection = MongoConnection( List( "localhost:27016" ) )
  val db = DB("plugin", connection)
  val collection = db("acoll")

  def listDocs() = {
    // select only the documents which field 'name' equals 'Jack'
    val query = Bson("name" -> BSONString("Jack"))
    // select only the field 'name'
    val filter = Bson(
      "name" -> BSONInteger(1),
      "_id" -> BSONInteger(0)
    )

    // get a Cursor[DefaultBSONIterator]
    val cursor = collection.find(query, Some(filter))
    // let's enumerate this cursor and print a readable representation of each document in the response
    cursor.enumerate.apply(Iteratee.foreach { doc =>
      println("found document: " + DefaultBSONIterator.pretty(doc))
    })

    // or, the same with getting a list
    val cursor2 = collection.find(query, Some(filter))
    val list = cursor2.toList
  }
}
}}}
   *
   * It is worth diving into the [[https://github.com/playframework/Play20/wiki/Iteratees Play! 2.0 Iteratee documentation]].
   *
   * @tparam T the type of the matched documents. An implicit [[org.asyncmongo.handlers.BSONReader]][T] typeclass for handling it has to be in the scope.
   *
*/
trait Cursor[T] {
  /** An iterator on the last fetched documents. */
  val iterator: Iterator[T]

  val cursorId: Option[Long]
  val connection: Option[MongoConnection]

  /** Gets the next instance of that cursor. */
  def next: Future[Cursor[T]]

  /** Tells if another instance of cursor can be fetched. */
  def hasNext: Boolean

  import play.api.libs.iteratee._
  import scala.collection.generic.CanBuildFrom

  /**
   * Enumerates this cursor.
   * The reuse of this cursor may cause unexpected behavior.
   *
   * Example:
   * {{{
// get a Cursor[DefaultBSONIterator]
val cursor = collection.find(query, Some(filter))
// let's enumerate this cursor and print a readable representation of each document in the response
cursor.enumerate.apply(Iteratee.foreach { doc =>
  println("found document: " + DefaultBSONIterator.pretty(doc))
})
}
}}}
   *
   * It is worth diving into the [[https://github.com/playframework/Play20/wiki/Iteratees Play! 2.0 Iteratee documentation]].
   *
   * @tparam T the type of the matched documents. An implicit [[org.asyncmongo.handlers.BSONReader]][T] typeclass for handling it has to be in the scope.
   *
*/
  def enumerate()(implicit ctx: ExecutionContext) :Enumerator[T] =
    if(hasNext)
      Enumerator.unfoldM(this) { cursor =>
        Cursor.nextElement(cursor)
      } &> Enumeratee.collect { case Some(e) => e }
    else Enumerator.eof

  /**
   * Collects all the documents into a collection of type '''M[T]'''.
   * The reuse of this cursor may cause unexpected behavior.
   *
   * Example:
   * {{{
val cursor2 = collection.find(query, Some(filter))
val list = cursor2[List].collect()
}}}
   *
   * @tparam M the type of the returned collection.
   * @tparam T the type of the matched documents. An implicit [[org.asyncmongo.handlers.BSONReader]][T] typeclass for handling it has to be in the scope.
   */
  def collect[M[_]]()(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext) :Future[M[T]] = {
    enumerate |>>> Iteratee.fold(cbf.apply) { (builder, t :T) => builder += t }.map(_.result)
  }

   /**
   * Collects all the documents into a collection of type '''M[T]'''.
   * The reuse of this cursor may cause unexpected behavior.
   *
   * Example:
   * {{{
val cursor = collection.find(query, Some(filter))
// gather the first 3 documents
val list = cursor[List].collect(3)
}}}
   *
   * @tparam M the type of the returned collection.
   * @tparam T the type of the matched documents. An implicit [[org.asyncmongo.handlers.BSONReader]][T] typeclass for handling it has to be in the scope.
   * @param upTo The maximum size of this collection.
   */
  def collect[M[_]](upTo: Int)(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext) :Future[M[T]] = {
    enumerate &> Enumeratee.take(upTo) |>>> Iteratee.fold(cbf.apply) { (builder, t :T) => builder += t }.map(_.result)
  }

  /**
   * Collects all the documents into a collection of type '''M[T]'''.
   * The reuse of this cursor may cause unexpected behavior.
   *
   * Example:
   * {{{
val cursor2 = collection.find(query, Some(filter))
val list = cursor2.toList
}}}
   *
   * @tparam M the type of the returned collection.
   * @tparam T the type of the matched documents. An implicit [[org.asyncmongo.handlers.BSONReader]][T] typeclass for handling it has to be in the scope.
   */
  def toList()(implicit ctx: ExecutionContext) :Future[List[T]] = collect[List]()

  /**
   * Collects all the documents into a collection of type '''M[T]'''.
   * The reuse of this cursor may cause unexpected behavior.
   *
   * Example:
   * {{{
val cursor2 = collection.find(query, Some(filter))
// return the 3 first documents in a list.
val list = cursor2.toList(3)
}}}
   *
   * @tparam M the type of the returned collection.
   * @tparam T the type of the matched documents. An implicit [[org.asyncmongo.handlers.BSONReader]][T] typeclass for handling it has to be in the scope.
   */
  def toList(upTo: Int)(implicit ctx: ExecutionContext) :Future[List[T]] = collect[List](upTo)

  /**
   * Gets the first returned document, if any.
   *
   * Example:
   * {{{
val cursor2 = collection.find(query, Some(filter))
val list = cursor2[List].collect()
}}}
   *
   * @tparam T the type of the matched documents. An implicit [[org.asyncmongo.handlers.BSONReader]][T] typeclass for handling it has to be in the scope.
   */
  def headOption()(implicit ec: ExecutionContext) :Future[Option[T]] = {
    collect[Iterable](1).map(_.headOption)
  }

  /** Explicitly closes that cursor. */
  def close = if(hasNext && cursorId.isDefined && connection.isDefined) {
      connection.get.send(RequestMaker(KillCursors(Set(cursorId.get))))
  }

  /*def nextElement()(implicit ec: ExecutionContext) :Future[Option[(Cursor[T], T)]] = {
    Cursor.nextElement(this).flatMap {
      case result @ Some((cursor, Some(e))) => Future(Some(cursor -> e))
      case Some((cursor, None)) => Cursor.nextElement(cursor).flatMap { // we just got a new cursor. If this cursor has no element, then the Mongo cursor is done.
        case result @ Some((cursor, Some(e))) => Future(Some(cursor -> e))
        case _ => Future(None)
      }
      case _ => Future(None)
    }
  }*/
}

class DefaultCursor[T](response: Response, mongoConnection: MongoConnection, query: Query)(implicit handler: BSONReaderHandler, reader: BSONReader[T], timeout :Duration, ctx: ExecutionContext) extends Cursor[T] {
  import DefaultCursor.logger

  lazy val iterator :Iterator[T] = handler.handle(response.reply, response.documents)

  val cursorId = Some(response.reply.cursorID)
  override val connection = Some(mongoConnection)

  def next :Future[DefaultCursor[T]] = {
    if(hasNext) {
      logger.debug("cursor: calling next on " + response.reply.cursorID)
      val op = GetMore(query.fullCollectionName, query.numberToReturn, response.reply.cursorID)
      mongoConnection.ask(RequestMaker(op).copy(channelIdHint=Some(response.info.channelId))).map { response => new DefaultCursor(response, mongoConnection, query) }
    } else throw new NoSuchElementException()
  }

  def hasNext :Boolean = response.reply.cursorID != 0
}

/**
 * A [[org.asyncmongo.api.Cursor]] that holds no document, and which the next cursor is given in the constructor.
 */
class FlattenedCursor[T](futureCursor: Future[Cursor[T]]) extends Cursor[T] {
  val iterator :Iterator[T] = Iterator.empty

  val cursorId = None

  val connection = None

  def next = futureCursor

  def hasNext = true
}

object Cursor {
  import play.api.libs.iteratee._
  import play.api.libs.concurrent.{Promise => PlayPromise, _}

  /**
   * Flattens the given future [[org.asyncmongo.api.Cursor]] to a [[org.asyncmongo.api.FlattenedCursor]].
   */
  def flatten[T](futureCursor: Future[Cursor[T]]) = new FlattenedCursor(futureCursor)

  private def nextElement[T](cursor: Cursor[T])(implicit ec: ExecutionContext) :Future[Option[(Cursor[T], Option[T])]] = {
    if(cursor.iterator.hasNext)
      Future(Some((cursor,Some(cursor.iterator.next))))
    else if (cursor.hasNext)
      cursor.next.map(c => Some((c,None)))
    else
      Future(None)
  }
}

object DefaultCursor {
  private val logger = LoggerFactory.getLogger("Cursor")
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
  def waitForPrimary(waitForAvailability: Duration) :Future[_] = {
    new play.api.libs.concurrent.AkkaPromise(
      monitor.ask(org.asyncmongo.actors.WaitForPrimary)(akka.util.Timeout(waitForAvailability.length, waitForAvailability.unit))
    )
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
  def ask(message: RequestMaker)(implicit timeout: Duration) :Future[Response] = {
    new play.api.libs.concurrent.AkkaPromise(
      (mongosystem ? message)(akka.util.Timeout(timeout.length, timeout.unit)).mapTo[Response]
    )
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
  def ask(message: RequestMaker, waitForAvailability: Duration)(implicit dbTimeout: Duration) :Future[Response] = {
    new play.api.libs.concurrent.AkkaPromise(
      monitor.ask("primary")(akka.util.Timeout(waitForAvailability.length, waitForAvailability.unit)).flatMap { s=>
        (mongosystem ? message)(akka.util.Timeout(dbTimeout.length, dbTimeout.unit)).mapTo[Response]
      }
    )
  }

  /**
   * Writes a checked write request and wait for a response.
   *
   * @param message The request maker.
   *
   * @return The future response.
   */
  def ask(checkedWriteRequest: CheckedWriteRequest)(implicit timeout: Duration) = {
    new play.api.libs.concurrent.AkkaPromise(
      (mongosystem ? checkedWriteRequest)(akka.util.Timeout(timeout.length, timeout.unit)).mapTo[Response]
    )
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
  def ask(checkedWriteRequest: CheckedWriteRequest, waitForAvailability: Duration)(implicit dbTimeout: Duration) :Future[Response] = {
    new play.api.libs.concurrent.AkkaPromise(
      monitor.ask("primary")(akka.util.Timeout(waitForAvailability.length, waitForAvailability.unit)).flatMap { s =>
        (mongosystem ? checkedWriteRequest)(akka.util.Timeout(dbTimeout.length, dbTimeout.unit)).mapTo[Response]
      }
    )
  }

  /**
   * Writes a request and drop the response if any.
   *
   * @param message The request maker.
   */
  def send(message: RequestMaker) = mongosystem ! message

  /** Authenticates the connection on the given database. */ // TODO return type
  def authenticate(db: String, user: String, password: String)(implicit timeout: Duration) :Future[AuthenticationResult] = {
    new play.api.libs.concurrent.AkkaPromise((mongosystem ? Authenticate(db, user, password))(akka.util.Timeout(timeout.length, timeout.unit)).mapTo[AuthenticationResult])
  }

  def close(implicit timeout: Duration) :Future[String] = new play.api.libs.concurrent.AkkaPromise((mongosystem ? org.asyncmongo.actors.Close)(akka.util.Timeout(timeout.length, timeout.unit)).mapTo[String])

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
   * @param nbChannelsPerNode Number of channels to open per node. Defaults to 10.
   * @param name The name of the newly created [[org.asyncmongo.actors.MongoDBSystem]] actor, if needed.
   */
  def apply(nodes: List[String], authentications :List[Authenticate] = List.empty, nbChannelsPerNode :Int = 10, name: Option[String] = None) = {
    val props = Props(new MongoDBSystem(nodes, authentications, nbChannelsPerNode))
    val mongosystem = if(name.isDefined) system.actorOf(props, name = name.get) else system.actorOf(props)
    val monitor = system.actorOf(Props(new MonitorActor(mongosystem)))
    new MongoConnection(mongosystem, monitor)
  }
}

object Test {
  import akka.pattern.ask
  import DefaultBSONHandlers._
  import play.api.libs.iteratee._

  import scala.concurrent.util.duration._

  implicit val timeout = 5 seconds
  import ExecutionContext.Implicits.global // TODO create default ExecutionContext

  def test2 = {
    val connection = MongoConnection(List("localhost:27016"))
    val db = DB("plugin", connection)
    val collection = db("acoll")
    connection.waitForPrimary(timeout).map { _ =>
      println("ok, let's go \n")
      collection.find(BSONDocument(
        "_id" -> new BSONObjectID("501c3a63faf8fdc95e050178")
      )).headOption.filter(_.isDefined).map { k =>
        println(DefaultBSONIterator.pretty(k.get.bsonIterator))
      }
      /*val toSave = Bson("name" -> BSONString("Kurt"))
        val toSave2 = new ng.WritableBSONDocument(32)
        toSave2.append("name" -> BSONString("Kurt")) ;
        val arr = new ng.WritableBSONArray(32) ;
        arr.append(BSONInteger(2)) ;
        arr.append(BSONString("hey")) ;
        toSave2.append("plop" -> arr)
        collection.insert(toSave :Bson, GetLastError(false, None, false)).onComplete {
          case Left(t) => { println("\n\n\nNORMAL error!, throwable\n\t\t"); t.printStackTrace; println("\n\t for insert=" + toSave) }
          case Right(le) => {
            println("\n\n\nNORMAL insertion " + (if(le.inError) "failed" else "succeeded") + ": " + le.stringify)
          }
        }
        collection.insert(FBson(toSave2.makeBuffer) :Bson, GetLastError(false, None, false)).onComplete {
          case Left(t) => { println("\n\n\nerror!, throwable\n\t\t"); t.printStackTrace; println("\n\t for insert=" + toSave) }
          case Right(le) => {
            println("\n\n\ninsertion " + (if(le.inError) "failed" else "succeeded") + ": " + le.stringify)
          }
        }*/
    }.onComplete {
      case Left(e) => println("\n\tERROR!! " + e); e.printStackTrace
      case Right(t) => println("\n\tdone: " + t)
    }
  }

  def test = {
    val connection = MongoConnection(List("localhost:27016"))//, List(Authenticate("plugin", "jack", "toto")))
    MongoConnection.system.scheduler.scheduleOnce(akka.util.duration.intToDurationInt(100).milliseconds) {

    val db = DB("plugin", connection)
    val collection = db("acoll")
    db.authenticate("plop", "toto")
    db.authenticate("jack", "toto").onComplete {
      case yop => {
        println("auth completed for jack " + yop)

        /*val futureCursor = collection.find(Bson()).enumerate.apply(Iteratee.foreach{ t=>
          println("fetched t=" + DefaultBSONIterator.pretty(t))
        })*/

        /*val cursor = collection.find(Bson())

        cursor.collect[List](1).map { list =>
          println("list size=" + list.size)
          list.headOption.map { e =>
            println("fetched t=" + DefaultBSONIterator.pretty(e))
          }
        }*/

        val toSave = BSONDocument("name" -> BSONString("Kurt"))
        val toSave2 = BSONDocument("name" -> BSONString("Kurt")) ;
        val arr = BSONArray(BSONInteger(2), BSONString("hey")) ;
        toSave2.append("plop" -> arr)
        collection.insert(toSave2, GetLastError(false, None, false)).onComplete {
          case Left(t) => { println("\n\n\nerror!, throwable\n\t\t"); t.printStackTrace; println("\n\t for insert=" + toSave) }
          case Right(le) => {
            println("\n\n\ninsertion " + (if(le.inError) "failed" else "succeeded") + ": " + le.stringify)
          }
        }/*
        db.connection.ask(FindAndModify(
            "acoll",
            Bson("name" -> BSONString("Kurt")),
            FindAndModifyUpdate(Bson("$set" -> BSONDocument(Bson("name" -> BSONString("JACK")).makeBuffer)), false)
        )(db.dbName).maker).onComplete {
          case Right(response) =>
            println("FINDANDMODIFY gave " + DefaultBSONIterator.pretty(DefaultBSONHandlers.parse(response).next))
            collection.command(FindAndModify(
                "acoll",
                Bson("name" -> BSONString("JACK")),
                FindAndModifyUpdate(Bson("$set" -> BSONDocument(Bson("name" -> BSONString("JACK")).makeBuffer)), false)
            )).onComplete {
              case Right(Some(doc)) => println("FINDANDMODIFY #2 gave " + DefaultBSONIterator.pretty(DefaultBSONReader.read(doc.value)))
              case Right(_) => println("FINDANDMODIFY #2 gave no value")
              case Left(response) => println("FINDANDMODIFY ERROR #2")
            }
        }*/

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
    val tags = BSONDocument(
      "tag1" -> BSONString("yop"),
      "tag2" -> BSONString("..."))
    val toSave = BSONDocument(
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
}