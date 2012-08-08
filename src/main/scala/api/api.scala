package org.asyncmongo.api

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.util.Duration
import scala.concurrent.util.duration._

import org.asyncmongo.core.actors.{Authenticate, MongoDBSystem, MonitorActor}
import org.asyncmongo.bson._
import org.asyncmongo.bson.handlers._
import org.asyncmongo.core.protocol._
import org.asyncmongo.core.commands.{Update => FindAndModifyUpdate, _}
import org.jboss.netty.buffer.ChannelBuffer
import org.slf4j.{Logger, LoggerFactory}

import indexes._

/**
 * A Mongo Database.
 *
 * @param dbName database name.
 * @param connection the [[org.asyncmongo.api.MongoConnection]] that will be used to query this database.
 * @param timeout the default timeout for the queries. Defaults to 5 seconds.
 */
case class DB(dbName: String, connection: MongoConnection)(implicit timeout :Duration = 5 seconds, context: ExecutionContext) { // TODO timeout
  /**  Gets a [[org.asyncmongo.api.Collection]] from this database. */
  def apply(name: String) :Collection = Collection(this, name, connection)(timeout, context)

  /** Authenticates the connection on this database. */ // TODO return type
  def authenticate(user: String, password: String) :Future[AuthenticationResult] = connection.authenticate(dbName, user, password)

  /** The index manager for this database. */
  lazy val indexes = new IndexesManager(this)
}

/**
 * A Mongo Collection.
 *
 * Example:
{{{
import org.asyncmongo.api._
import org.asyncmongo.bson._
import org.asyncmongo.handlers.DefaultBSONHandlers._
import play.api.libs.iteratee.Iteratee

object Samples {

  val connection = MongoConnection( List( "localhost:27016" ) )
  val db = DB("plugin", connection)
  val collection :Collection = db("acoll")

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
 * @param db database.
 * @param collectionName the name of this collection.
 * @param connection the [[org.asyncmongo.api.MongoConnection]] that will be used to query this database.
 * @param timeout the default timeout for the queries.
 */
case class Collection(
  db: DB,
  collectionName: String,
  connection: MongoConnection
)(implicit timeout: Duration, context: ExecutionContext) {
  /** The full collection name. */
  lazy val fullCollectionName = db.dbName + "." + collectionName

  // TODO
  /** Counts the number of documents in this collection. */
  def count() :Future[Int] = {
    import DefaultBSONHandlers._
    connection.ask(Count(collectionName)(db.dbName).maker).map { response =>
      DefaultBSONReaderHandler.handle(response.reply, response.documents).next.getAs[BSONDouble]("n").map(_.value.toInt).getOrElse(0)
    }
  }

  /**
   * Find the documents matching the given criteria.
   *
   * This method accepts any query and projection object, provided that there is an implicit [[org.asyncmongo.handlers.BSONWriter]] typeclass for handling them in the scope.
   * You can use the typeclasses defined in [[org.asyncmongo.handlers.DefaultBSONHandlers]] object.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam Qry the type of the query. An implicit [[org.asyncmongo.handlers.BSONWriter]][Qry] typeclass for handling it has to be in the scope.
   * @tparam Pjn the type of the projection object. An implicit [[org.asyncmongo.handlers.BSONWriter]][Pjn] typeclass for handling it has to be in the scope.
   * @tparam Rst the type of the matched documents. An implicit [[org.asyncmongo.handlers.BSONReader]][Rst] typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   * @param projection Get only a subset of each matched documents. Defaults to None.
   * @param opts The query options (skip, batchSize, flags...).
   *
   * @return a cursor over the matched documents. You can get an enumerator for it, please see the [[org.asyncmongo.api.Cursor]] companion object.
   */
  def find[Qry, Pjn, Rst](query: Qry, projection: Pjn, opts: QueryOpts = QueryOpts())(implicit writer: BSONWriter[Qry], writer2: BSONWriter[Pjn], handler: BSONReaderHandler, reader: BSONReader[Rst]) :FlattenedCursor[Rst] =
    find(writer.write(query), Some(writer2.write(projection)), opts)

  /**
   * Find the documents matching the given criteria.
   *
   * This method accepts any query object, provided that there is an implicit [[org.asyncmongo.handlers.BSONWriter]] typeclass for handling it in the scope.
   * You can use the typeclasses defined in [[org.asyncmongo.handlers.DefaultBSONHandlers]] object.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam Qry the type of the query. An implicit [[org.asyncmongo.handlers.BSONWriter]][Qry] typeclass for handling it has to be in the scope.
   * @tparam Rst the type of the matched documents. An implicit [[org.asyncmongo.handlers.BSONReader]][Rst] typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   * @param opts The query options (skip, batchSize, flags...).
   *
   * @return a cursor over the matched documents. You can get an enumerator for it, please see the [[org.asyncmongo.api.Cursor]] companion object.
   */
  def find[Qry, Rst](query: Qry, opts: QueryOpts)(implicit writer: BSONWriter[Qry], handler: BSONReaderHandler, reader: BSONReader[Rst]) :FlattenedCursor[Rst] =
    find(writer.write(query), None, opts)

  /**
   * Find the documents matching the given criteria.
   *
   * This method accepts any query object, provided that there is an implicit [[org.asyncmongo.handlers.BSONWriter]] typeclass for handling it in the scope.
   * You can use the typeclasses defined in [[org.asyncmongo.handlers.DefaultBSONHandlers]] object.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam Qry the type of the query. An implicit [[org.asyncmongo.handlers.BSONWriter]][Qry] typeclass for handling it has to be in the scope.
   * @tparam Rst the type of the matched documents. An implicit [[org.asyncmongo.handlers.BSONReader]][Rst] typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   *
   * @return a cursor over the matched documents. You can get an enumerator for it, please see the [[org.asyncmongo.api.Cursor]] companion object.
   */
  def find[Qry, Rst](query: Qry)(implicit writer: BSONWriter[Qry], handler: BSONReaderHandler, reader: BSONReader[Rst]) :FlattenedCursor[Rst] =
    find(writer.write(query), None, QueryOpts())

  /**
   * Find the documents matching the given criteria, using the given [[org.asyncmongo.api.QueryBuilder]] instance.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam Rst the type of the matched documents. An implicit [[org.asyncmongo.handlers.BSONReader]][Rst] typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   * @param opts The query options (skip, batchSize, flags...).
   *
   * @return a cursor over the matched documents. You can get an enumerator for it, please see the [[org.asyncmongo.api.Cursor]] companion object.
   */
  def find[Rst](query: QueryBuilder, opts: QueryOpts)(implicit handler: BSONReaderHandler, reader: BSONReader[Rst]) :FlattenedCursor[Rst] =
    find(query.makeMergedBuffer, None, opts)

  /**
   * Find the documents matching the given criteria, using the given [[org.asyncmongo.api.QueryBuilder]] instance.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam Rst the type of the matched documents. An implicit [[org.asyncmongo.handlers.BSONReader]][Rst] typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   *
   * @return a cursor over the matched documents. You can get an enumerator for it, please see the [[org.asyncmongo.api.Cursor]] companion object.
   */
  def find[Rst](query: QueryBuilder)(implicit handler: BSONReaderHandler, reader: BSONReader[Rst]) :FlattenedCursor[Rst] =
    find(query.makeMergedBuffer, None, QueryOpts())

  private def find[Rst](query: ChannelBuffer, projection: Option[ChannelBuffer], opts: QueryOpts)(implicit handler: BSONReaderHandler, reader: BSONReader[Rst]) :FlattenedCursor[Rst] = {
    val op = Query(opts.flagsN, fullCollectionName, opts.skipN, opts.batchSizeN)
    if(projection.isDefined)
      query.writeBytes(projection.get)
    val message = RequestMaker(op, query)

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
    connection.ask(command.apply(db.dbName).maker).map(command.ResultMaker(_))

  /** The index manager for this collection. */
  lazy val indexes = db.indexes.onCollection(collectionName)
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
      monitor.ask(org.asyncmongo.core.actors.WaitForPrimary)(akka.util.Timeout(waitForAvailability.length, waitForAvailability.unit))
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

  def close(implicit timeout: Duration) :Future[String] = new play.api.libs.concurrent.AkkaPromise((mongosystem ? org.asyncmongo.core.actors.Close)(akka.util.Timeout(timeout.length, timeout.unit)).mapTo[String])

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