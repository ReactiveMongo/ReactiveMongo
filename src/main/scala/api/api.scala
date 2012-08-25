package reactivemongo.api

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import org.jboss.netty.buffer.ChannelBuffer
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.iteratee._
import reactivemongo.api.indexes._
import reactivemongo.core.actors._
import reactivemongo.bson._
import reactivemongo.bson.handlers._
import reactivemongo.core.protocol._
import reactivemongo.core.commands.{AuthenticationResult, Command, GetLastError, LastError}
import reactivemongo.utils.scalaToAkkaDuration
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.util.Duration
import scala.concurrent.util.duration._

/**
 * A Mongo Database.
 *
 * @param dbName database name.
 * @param connection the [[reactivemongo.api.MongoConnection]] that will be used to query this database.
 */
case class DB(dbName: String, connection: MongoConnection, failoverStrategy: FailoverStrategy = FailoverStrategy())(implicit context: ExecutionContext) {
  /**  Gets a [[reactivemongo.api.Collection]] from this database. */
  def apply(name: String) :Collection = Collection(this, name, connection, failoverStrategy)(MongoConnection.ec)

  /** Authenticates the connection on this database. */
  def authenticate(user: String, password: String)(implicit timeout: Duration) :Future[AuthenticationResult] = connection.authenticate(dbName, user, password)

  /** The index manager for this database. */
  lazy val indexes = new IndexesManager(this)

  /**
   * Sends a command and get the future result of the command.
   *
   * @param command The command to send.
   *
   * @return a future containing the result of the command.
   */
  def command(command: Command) :Future[command.Result] =
    Failover(command.apply(dbName).maker, connection.mongosystem, failoverStrategy).future.map(command.ResultMaker(_))
}

/**
 * A Mongo Collection.
 *
 * Example:
{{{
import play.api.libs.iteratee.Iteratee
import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.bson.handlers.DefaultBSONHandlers._

object Samples {

  val connection = MongoConnection( List( "localhost:27016" ) )
  val db = DB("plugin", connection)
  val collection :Collection = db("acoll")

  def listDocs() = {
    // select only the documents which field 'name' equals 'Jack'
    val query = BSONDocument("name" -> BSONString("Jack"))
    // select only the field 'name'
    val filter = BSONDocument(
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
 * @param connection the [[reactivemongo.api.MongoConnection]] that will be used to query this database.
 */
case class Collection(
  db: DB,
  collectionName: String,
  connection: MongoConnection,
  failoverStrategy: FailoverStrategy = FailoverStrategy()
)(implicit context: ExecutionContext) {
  /** The full collection name. */
  lazy val fullCollectionName = db.dbName + "." + collectionName

  /**
   * Find the documents matching the given criteria.
   *
   * This method accepts any query and projection object, provided that there is an implicit [[reactivemongo.bson.handlers.BSONWriter]] typeclass for handling them in the scope.
   * You can use the typeclasses defined in [[reactivemongo.bson.handlers.DefaultBSONHandlers]] object.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam Qry the type of the query. An implicit [[reactivemongo.bson.handlers.BSONWriter]][Qry] typeclass for handling it has to be in the scope.
   * @tparam Pjn the type of the projection object. An implicit [[reactivemongo.bson.handlers.BSONWriter]][Pjn] typeclass for handling it has to be in the scope.
   * @tparam Rst the type of the matched documents. An implicit [[reactivemongo.bson.handlers.BSONReader]][Rst] typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   * @param projection Get only a subset of each matched documents. Defaults to None.
   * @param opts The query options (skip, batchSize, flags...).
   *
   * @return a cursor over the matched documents. You can get an enumerator for it, please see the [[reactivemongo.api.Cursor]] companion object.
   */
  def find[Qry, Pjn, Rst](query: Qry, projection: Pjn, opts: QueryOpts = QueryOpts())(implicit writer: BSONWriter[Qry], writer2: BSONWriter[Pjn], handler: BSONReaderHandler, reader: BSONReader[Rst]) :FlattenedCursor[Rst] =
    find(writer.write(query), Some(writer2.write(projection)), opts)

  /**
   * Find the documents matching the given criteria.
   *
   * This method accepts any query object, provided that there is an implicit [[reactivemongo.bson.handlers.BSONWriter]] typeclass for handling it in the scope.
   * You can use the typeclasses defined in [[org.asyncmongo.handlers.DefaultBSONHandlers]] object.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam Qry the type of the query. An implicit [[reactivemongo.bson.handlers.BSONWriter]][Qry] typeclass for handling it has to be in the scope.
   * @tparam Rst the type of the matched documents. An implicit [[reactivemongo.bson.handlers.BSONReader]][Rst] typeclass for handling it has to be in the scope.
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
   * This method accepts any query object, provided that there is an implicit [[reactivemongo.bson.handlers.BSONWriter]] typeclass for handling it in the scope.
   * You can use the typeclasses defined in [[org.asyncmongo.handlers.DefaultBSONHandlers]] object.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam Qry the type of the query. An implicit [[reactivemongo.bson.handlers.BSONWriter]][Qry] typeclass for handling it has to be in the scope.
   * @tparam Rst the type of the matched documents. An implicit [[reactivemongo.bson.handlers.BSONReader]][Rst] typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   *
   * @return a cursor over the matched documents. You can get an enumerator for it, please see the [[reactivemongo.api.Cursor]] companion object.
   */
  def find[Qry, Rst](query: Qry)(implicit writer: BSONWriter[Qry], handler: BSONReaderHandler, reader: BSONReader[Rst]) :FlattenedCursor[Rst] =
    find(writer.write(query), None, QueryOpts())

  /**
   * Find the documents matching the given criteria, using the given [[reactivemongo.api.QueryBuilder]] instance.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam Rst the type of the matched documents. An implicit [[reactivemongo.bson.handlers.BSONReader]][Rst] typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   * @param opts The query options (skip, batchSize, flags...).
   *
   * @return a cursor over the matched documents. You can get an enumerator for it, please see the [[org.asyncmongo.api.Cursor]] companion object.
   */
  def find[Rst](query: QueryBuilder, opts: QueryOpts)(implicit handler: BSONReaderHandler, reader: BSONReader[Rst]) :FlattenedCursor[Rst] =
    find(query.makeMergedBuffer, None, opts)

  /**
   * Find the documents matching the given criteria, using the given [[reactivemongo.api.QueryBuilder]] instance.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam Rst the type of the matched documents. An implicit [[reactivemongo.bson.handlers.BSONReader]][Rst] typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   *
   * @return a cursor over the matched documents. You can get an enumerator for it, please see the [[reactivemongo.api.Cursor]] companion object.
   */
  def find[Rst](query: QueryBuilder)(implicit handler: BSONReaderHandler, reader: BSONReader[Rst]) :FlattenedCursor[Rst] =
    find(query.makeMergedBuffer, None, QueryOpts())

  private def find[Rst](query: ChannelBuffer, projection: Option[ChannelBuffer], opts: QueryOpts)(implicit handler: BSONReaderHandler, reader: BSONReader[Rst]) :FlattenedCursor[Rst] = {
    val op = Query(opts.flagsN, fullCollectionName, opts.skipN, opts.batchSizeN)
    if(projection.isDefined)
      query.writeBytes(projection.get)
    val requestMaker = RequestMaker(op, query)

    Cursor.flatten(Failover(requestMaker, connection.mongosystem, failoverStrategy).future.map(new DefaultCursor(_, connection, op, failoverStrategy)))
  }

  /**
   * Inserts a document into the collection without writeConcern.
   *
   * Please note that you cannot be sure that the document has been effectively written and when (hence the Unit return type).
   *
   * @tparam T the type of the document to insert. An implicit [[reactivemongo.bson.handlers.BSONWriter]][T] typeclass for handling it has to be in the scope.
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
   * Inserts a document into the collection and wait for the [[reactivemongo.core.protocol.commands.LastError]] result.
   *
   * Please read the documentation about [[reactivemongo.core.protocol.commands.GetLastError]] to know how to use it properly.
   *
   * @tparam T the type of the document to insert. An implicit [[reactivemongo.bson.handlers.BSONWriter]][T] typeclass for handling it has to be in the scope.
   *
   * @param document the document to insert.
   * @param writeConcern the [[reactivemongo.core.protocol.commands.GetLastError]] command message to send in order to control how the document is inserted. Defaults to GetLastError().
   *
   * @return a future [[reactivemongo.core.protocol.commands.LastError]] that can be used to check whether the insertion was successful.
   */
  def insert[T](document: T, writeConcern: GetLastError)(implicit writer: BSONWriter[T]) :Future[LastError] = {
    val op = Insert(0, fullCollectionName)
    val bson = writer.write(document)
    val checkedWriteRequest = CheckedWriteRequest(op, bson, writeConcern)
    Failover(checkedWriteRequest, connection.mongosystem, failoverStrategy).future.map(LastError(_))
  }

  /**
   * Inserts a document into the collection and wait for the [[reactivemongo.core.protocol.commands.LastError]] result.
   *
   * Please read the documentation about [[reactivemongo.core.protocol.commands.GetLastError]] to know how to use it properly.
   *
   * @tparam T the type of the document to insert. An implicit [[reactivemongo.bson.handlers.BSONWriter]][T] typeclass for handling it has to be in the scope.
   *
   * @param document the document to insert.
   *
   * @return a future [[reactivemongo.core.protocol.commands.LastError]] that can be used to check whether the insertion was successful.
   */
  def insert[T](document: T)(implicit writer: BSONWriter[T]) :Future[LastError] = insert(document, GetLastError())

  /**
   * Returns an iteratee that will insert the documents it feeds.
   *
   * This iteratee eventually gives the number of documents that have been inserted into the collection.
   *
   * @tparam T the type of the documents to insert. An implicit [[reactivemongo.bson.handlers.BSONWriter]][T] typeclass for handling it has to be in the scope.
   * @param bulkSize The number of documents per bulk.
   * @param bulkByteSize The maximum size for a bulk, in bytes.
   */
  def insertIteratee[T](bulkSize: Int = bulk.MaxDocs, bulkByteSize: Int = bulk.MaxBulkSize)(implicit writer: BSONWriter[T]) :Iteratee[T, Int] =
    Enumeratee.map { doc:T => writer.write(doc) } &>> bulk.iteratee(this, bulkSize, bulkByteSize)

  /**
   * Inserts the documents provided by the given enumerator into the collection and eventually returns the number of inserted documents.
   *
   *
   * @tparam T the type of the document to insert. An implicit [[reactivemongo.bson.handlers.BSONWriter]][T] typeclass for handling it has to be in the scope.
   *
   * @param enumerator An enumerator of '''T'''.
   * @param bulkSize The number of documents per bulk.
   * @param bulkByteSize The maximum size for a bulk, in bytes.
   *
   * @return a future [[reactivemongo.core.protocol.commands.LastError]] that can be used to check whether the insertion was successful.
   */
  def insert[T](enumerator: Enumerator[T], bulkSize: Int = bulk.MaxDocs, bulkByteSize: Int = bulk.MaxBulkSize)(implicit writer: BSONWriter[T]) :Future[Int] =
    enumerator |>>> insertIteratee(bulkSize, bulkByteSize)

  /**
   * Updates one or more documents matching the given selector with the given modifier or update object.
   *
   * Please note that you cannot be sure that the matched documents have been effectively updated and when (hence the Unit return type).
   *
   * @tparam S the type of the selector object. An implicit [[reactivemongo.bson.handlers.BSONWriter]][S] typeclass for handling it has to be in the scope.
   * @tparam U the type of the modifier or update object. An implicit [[reactivemongo.bson.handlers.BSONWriter]][U] typeclass for handling it has to be in the scope.
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
   * @tparam S the type of the selector object. An implicit [[reactivemongo.bson.handlers.BSONWriter]][S] typeclass for handling it has to be in the scope.
   * @tparam U the type of the modifier or update object. An implicit [[reactivemongo.bson.handlers.BSONWriter]][U] typeclass for handling it has to be in the scope.
   *
   * @param selector the selector object, for finding the documents to update.
   * @param update the modifier object (with special keys like \$set) or replacement object.
   * @param writeConcern the [[reactivemongo.core.protocol.commands.GetLastError]] command message to send in order to control how the documents are updated. Defaults to GetLastError().
   * @param upsert states whether the update objet should be inserted if no match found. Defaults to false.
   * @param multi states whether the update may be done on all the matching documents.
   *
   * @return a future [[reactivemongo.core.protocol.commands.LastError]] that can be used to check whether the update was successful.
   */
  def update[S, U](selector: S, update: U, writeConcern: GetLastError = GetLastError(), upsert: Boolean = false, multi: Boolean = false)(implicit selectorWriter: BSONWriter[S], updateWriter: BSONWriter[U]) :Future[LastError] = {
    val flags = 0 | (if(upsert) UpdateFlags.Upsert else 0) | (if(multi) UpdateFlags.MultiUpdate else 0)
    val op = Update(fullCollectionName, flags)
    val bson = selectorWriter.write(selector)
    bson.writeBytes(updateWriter.write(update))
    val checkedWriteRequest = CheckedWriteRequest(op, bson, writeConcern)
    Failover(checkedWriteRequest, connection.mongosystem, failoverStrategy).future.map(LastError(_))
  }

  /**
   * Remove the matched document(s) from the collection without writeConcern.
   *
   * Please note that you cannot be sure that the matched documents have been effectively removed and when (hence the Unit return type).
   *
   * @tparam T the type of the selector of documents to remove. An implicit [[reactivemongo.bson.handlers.BSONWriter]][T] typeclass for handling it has to be in the scope.
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
   * Remove the matched document(s) from the collection and wait for the [[reactivemongo.core.protocol.commands.LastError]] result.
   *
   * Please read the documentation about [[reactivemongo.core.protocol.commands.GetLastError]] to know how to use it properly.
   *
   * @tparam T the type of the selector of documents to remove. An implicit [[reactivemongo.bson.handlers.BSONWriter]][T] typeclass for handling it has to be in the scope.
   *
   * @param query the selector of documents to remove.
   * @param writeConcern the [[reactivemongo.core.protocol.commands.GetLastError]] command message to send in order to control how the documents are removed. Defaults to GetLastError().
   * @param firstMatchOnly states whether only the first matched documents has to be removed from this collection.
   *
   * @return a future [[reactivemongo.core.protocol.commands.LastError]] that can be used to check whether the removal was successful.
   */
  def remove[T](query: T, writeConcern: GetLastError = GetLastError(), firstMatchOnly: Boolean = false)(implicit writer: BSONWriter[T]) :Future[LastError] = {
    val op = Delete(fullCollectionName, if(firstMatchOnly) 1 else 0)
    val bson = writer.write(query)
    val checkedWriteRequest = CheckedWriteRequest(op, bson, writeConcern)
    Failover(checkedWriteRequest, connection.mongosystem, failoverStrategy).future.map(LastError(_))
  }

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
import play.api.libs.iteratee.Iteratee
import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.bson.handlers.DefaultBSONHandlers._

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
   * @tparam T the type of the matched documents. An implicit [[reactivemongo.bson.handlers.BSONReader]][T] typeclass for handling it has to be in the scope.
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
   * @tparam T the type of the matched documents. An implicit [[reactivemongo.bson.handlers.BSONReader]][T] typeclass for handling it has to be in the scope.
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
   * @tparam T the type of the matched documents. An implicit [[reactivemongo.bson.handlers.BSONReader]][T] typeclass for handling it has to be in the scope.
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
   * @tparam T the type of the matched documents. An implicit [[reactivemongo.bson.handlers.BSONReader]][T] typeclass for handling it has to be in the scope.
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
   * @tparam T the type of the matched documents. An implicit [[reactivemongo.bson.handlers.BSONReader]][T] typeclass for handling it has to be in the scope.
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
   * @tparam T the type of the matched documents. An implicit [[reactivemongo.bson.handlers.BSONReader]][T] typeclass for handling it has to be in the scope.
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
   * @tparam T the type of the matched documents. An implicit [[reactivemongo.bson.handlers.BSONReader]][T] typeclass for handling it has to be in the scope.
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

class DefaultCursor[T](response: Response, mongoConnection: MongoConnection, query: Query, failoverStrategy: FailoverStrategy)(implicit handler: BSONReaderHandler, reader: BSONReader[T], ctx: ExecutionContext) extends Cursor[T] {
  import DefaultCursor.logger

  lazy val iterator :Iterator[T] = handler.handle(response.reply, response.documents)

  val cursorId = Some(response.reply.cursorID)
  override val connection = Some(mongoConnection)

  def next :Future[DefaultCursor[T]] = {
    if(hasNext) {
      logger.debug("cursor: calling next on " + response.reply.cursorID)
      val op = GetMore(query.fullCollectionName, query.numberToReturn, response.reply.cursorID)
      Failover(RequestMaker(op).copy(channelIdHint=Some(response.info.channelId)), mongoConnection.mongosystem, failoverStrategy).future.map { response => new DefaultCursor(response, mongoConnection, query, failoverStrategy) }
    } else throw new NoSuchElementException()
  }

  def hasNext :Boolean = response.reply.cursorID != 0
}

/**
 * A [[reactivemongo.api.Cursor]] that holds no document, and which the next cursor is given in the constructor.
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
   * Flattens the given future [[reactivemongo.api.Cursor]] to a [[reactivemongo.api.FlattenedCursor]].
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
 * A helper that sends the given message to the given actor, following a failover strategy.
 * This helper holds a future reference that is completed with a result, after 1 or more attempts (specified in the given strategy).
 * If the all the tryouts configured by the given strategy were unsuccessful, the future reference is completed with a Throwable.
 *
 * Should not be used directly for most use cases.
 *
 * @tparam T Type of the message to send.
 * @param message The message to send to the given actor. This message will be wrapped into an Expecting Response message by the ''expectingResponseMaker'' function.
 * @param actorRef The reference to the MongoDBSystem actor the given message will be sent to.
 * @param strategy The Failover strategy.
 * @param expectingResponseMaker A function that takes a message of type '''T''' and wraps it into an ExpectingResponse message.
 */
class Failover[T](message: T, actorRef: ActorRef, strategy: FailoverStrategy)(expectingResponseMaker: T => ExpectingResponse)(implicit ec: ExecutionContext) {
  import Failover.logger
  private val promise = scala.concurrent.Promise[Response]()
  val future: Future[Response] = promise.future

  val akkaDelay = scalaToAkkaDuration(strategy.initialDelay)

  def send(n: Int) {
    val expectingResponse = expectingResponseMaker(message)
    actorRef ! expectingResponse
    expectingResponse.future.onComplete {
      case Left(e) =>
        if(n < strategy.retries) {
          logger.debug("Got an error, retrying...")
          MongoConnection.system.scheduler.scheduleOnce(akkaDelay * strategy.delayFactor(n))(send(n + 1))
        } else {
          logger.info("Got an error, no more attempts to do. Completing with an error...")
          promise.failure(e)
        }
      case Right(response) =>
        logger.debug("Got a successful result, completing...")
        promise.success(response)
    }
  }

  send(0)
}

object Failover {
  private val logger = LoggerFactory.getLogger("Failover")
  /**
   * Produces a [[reactivemongo.api.Failover]] holding a future reference that is completed with a result, after 1 or more attempts (specified in the given strategy).
   *
   * @param checkedWriteRequest The checkedWriteRequest to send to the given actor.
   * @param actorRef The reference to the MongoDBSystem actor the given message will be sent to.
   * @param strategy The Failover strategy.
   */
  def apply(checkedWriteRequest: CheckedWriteRequest, actorRef: ActorRef, strategy: FailoverStrategy)(implicit ec: ExecutionContext) :Failover[CheckedWriteRequest] =
    new Failover(checkedWriteRequest, actorRef, strategy)(CheckedWriteRequestExpectingResponse.apply)

  /**
   * Produces a [[reactivemongo.api.Failover]] holding a future reference that is completed with a result, after 1 or more attempts (specified in the given strategy).
   *
   * @param requestMaker The requestMaker to send to the given actor.
   * @param actorRef The reference to the MongoDBSystem actor the given message will be sent to.
   * @param strategy The Failover strategy.
   */
  def apply(requestMaker: RequestMaker, actorRef: ActorRef, strategy: FailoverStrategy)(implicit ec: ExecutionContext) :Failover[RequestMaker] =
    new Failover(requestMaker, actorRef, strategy)(RequestMakerExpectingResponse.apply)
}

/**
 * A failover strategy for sending requests.
 *
 * @param initialDelay the initial delay between the first failed attempt and the next one.
 * @param retries the number of retries to do before giving up.
 * @param delayFactor a function that takes the current iteration and returns a factor to be applied to the initialDelay.
 */
case class FailoverStrategy(
  initialDelay: Duration = 500 milliseconds,
  retries: Int = 5,
  delayFactor: Int => Double = n => 1)



/**
 * A Mongo Connection.
 *
 * This is a wrapper around a reference to a [[reactivemongo.core.actors.MongoDBSystem]] Actor.
 * Connection here does not mean that there is one open channel to the server.
 * Behind the scene, many connections (channels) are open on all the available servers in the replica set.
 *
 * @param mongosystem A reference to a [[reactivemongo.core.actors.MongoDBSystem]] Actor.
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
      monitor.ask(reactivemongo.core.actors.WaitForPrimary)(scalaToAkkaDuration(waitForAvailability))
    )
  }

  /**
   * Writes a request and wait for a response.
   *
   * @param message The request maker.
   *
   * @return The future response.
   */
  def ask(message: RequestMaker) :Future[Response] = {
    val msg = RequestMakerExpectingResponse(message)
    mongosystem ! msg
    msg.future
  }

  /**
   * Writes a checked write request and wait for a response.
   *
   * @param message The request maker.
   *
   * @return The future response.
   */
  def ask(checkedWriteRequest: CheckedWriteRequest) = {
    val msg = CheckedWriteRequestExpectingResponse(checkedWriteRequest)
    mongosystem ! msg
    msg.future
  }

  /**
   * Writes a request and drop the response if any.
   *
   * @param message The request maker.
   */
  def send(message: RequestMaker) = mongosystem ! message

  /** Authenticates the connection on the given database. */
  def authenticate(db: String, user: String, password: String)(implicit timeout: Duration) :Future[AuthenticationResult] = {
    new play.api.libs.concurrent.AkkaPromise((mongosystem ? Authenticate(db, user, password))(scalaToAkkaDuration(timeout)).mapTo[AuthenticationResult])
  }

  /** Closes this MongoConnection (closes all the channels and ends the actors) */
  def askClose()(implicit timeout: Duration) :Future[_] = new play.api.libs.concurrent.AkkaPromise((monitor ? Close)(scalaToAkkaDuration(timeout)))

  /** Closes this MongoConnection (closes all the channels and ends the actors) */
  def close() :Unit = monitor ! Close
}

object MongoConnection {
  import com.typesafe.config.ConfigFactory
  val config = ConfigFactory.load()

  val ec = ExecutionContext.fromExecutor(java.util.concurrent.Executors.newCachedThreadPool)

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
   * @param name The name of the newly created [[reactivemongo.core.actors.MongoDBSystem]] actor, if needed.
   */
  def apply(nodes: List[String], authentications :List[Authenticate] = List.empty, nbChannelsPerNode :Int = 10, name: Option[String] = None) = {
    val props = Props(new MongoDBSystem(nodes, authentications, nbChannelsPerNode))
    val mongosystem = if(name.isDefined) system.actorOf(props, name = name.get) else system.actorOf(props)
    val monitor = system.actorOf(Props(new MonitorActor(mongosystem)))
    new MongoConnection(mongosystem, monitor)
  }
}