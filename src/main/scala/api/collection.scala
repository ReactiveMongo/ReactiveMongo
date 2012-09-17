package reactivemongo.api

import reactivemongo.api.indexes.IndexesManager
import reactivemongo.bson._
import reactivemongo.bson.handlers._
import reactivemongo.core.commands.{Update => UpdateCommand, _}
import reactivemongo.core.protocol._
import reactivemongo.utils.EitherMappableFuture._

import org.jboss.netty.buffer.ChannelBuffer
import play.api.libs.iteratee._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.util.Duration

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
  val db = connection("plugin")
  val collection = db("acoll")

  def listDocs() = {
    // select only the documents which field 'firstName' equals 'Jack'
    val query = BSONDocument("firstName" -> BSONString("Jack"))
    // select only the field 'lastName'
    val filter = BSONDocument(
      "lastName" -> BSONInteger(1),
      "_id" -> BSONInteger(0)
    )

    // get a Cursor[TraversableBSONDocument]
    val cursor = collection.find(query, Some(filter))
    // let's enumerate this cursor and print a readable representation of each document in the response
    cursor.enumerate.apply(Iteratee.foreach { doc =>
      println("found document: " + DefaultBSONIterator.pretty(doc.bsonIterator))
    })

    // or, the same with getting a list
    val cursor2 = collection.find(query, Some(filter))
    val futurelist = cursor.toList
  }
}
}}}
 */
trait Collection {
  val db: DB[Collection]

  /** The name of this collection. */
  val name: String

  /** The full collection name. */
  lazy val fullCollectionName = db.name + "." + name

  // abstract

  /** A low-level method for finding documents. Should not be used outside. */
  protected def find[Rst](query: ChannelBuffer, projection: Option[ChannelBuffer], opts: QueryOpts)(implicit handler: BSONReaderHandler, reader: RawBSONReader[Rst], ec: ExecutionContext) :FlattenedCursor[Rst]


  /**
   * Inserts a document into the collection without writeConcern.
   *
   * Please note that you cannot be sure that the document has been effectively written and when (hence the Unit return type).
   *
   * @tparam T the type of the document to insert. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][T] typeclass for handling it has to be in the scope.
   *
   * @param document the document to insert.
   */
  def uncheckedInsert[T](document: T)(implicit writer: RawBSONWriter[T]) :Unit

  /**
   * Inserts a document into the collection and wait for the [[reactivemongo.core.protocol.commands.LastError]] result.
   *
   * Please read the documentation about [[reactivemongo.core.protocol.commands.GetLastError]] to know how to use it properly.
   *
   * @tparam T the type of the document to insert. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][T] typeclass for handling it has to be in the scope.
   *
   * @param document the document to insert.
   * @param writeConcern the [[reactivemongo.core.protocol.commands.GetLastError]] command message to send in order to control how the document is inserted. Defaults to GetLastError().
   *
   * @return a future [[reactivemongo.core.protocol.commands.LastError]] that can be used to check whether the insertion was successful.
   */
  def insert[T](document: T, writeConcern: GetLastError)(implicit writer: RawBSONWriter[T], ec: ExecutionContext) :Future[LastError]

  /**
   * Updates one or more documents matching the given selector with the given modifier or update object.
   *
   * Please note that you cannot be sure that the matched documents have been effectively updated and when (hence the Unit return type).
   *
   * @tparam S the type of the selector object. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][S] typeclass for handling it has to be in the scope.
   * @tparam U the type of the modifier or update object. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][U] typeclass for handling it has to be in the scope.
   *
   * @param selector the selector object, for finding the documents to update.
   * @param update the modifier object (with special keys like \$set) or replacement object.
   * @param upsert states whether the update objet should be inserted if no match found. Defaults to false.
   * @param multi states whether the update may be done on all the matching documents.
   */
  def uncheckedUpdate[S, U](selector: S, update: U, upsert: Boolean = false, multi: Boolean = false)(implicit selectorWriter: RawBSONWriter[S], updateWriter: RawBSONWriter[U]) :Unit

  /**
   * Updates one or more documents matching the given selector with the given modifier or update object.
   *
   * @tparam S the type of the selector object. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][S] typeclass for handling it has to be in the scope.
   * @tparam U the type of the modifier or update object. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][U] typeclass for handling it has to be in the scope.
   *
   * @param selector the selector object, for finding the documents to update.
   * @param update the modifier object (with special keys like \$set) or replacement object.
   * @param writeConcern the [[reactivemongo.core.protocol.commands.GetLastError]] command message to send in order to control how the documents are updated. Defaults to GetLastError().
   * @param upsert states whether the update objet should be inserted if no match found. Defaults to false.
   * @param multi states whether the update may be done on all the matching documents.
   *
   * @return a future [[reactivemongo.core.protocol.commands.LastError]] that can be used to check whether the update was successful.
   */
  def update[S, U](selector: S, update: U, writeConcern: GetLastError = GetLastError(), upsert: Boolean = false, multi: Boolean = false)(implicit selectorWriter: RawBSONWriter[S], updateWriter: RawBSONWriter[U], ec: ExecutionContext) :Future[LastError]

    /**
   * Remove the matched document(s) from the collection without writeConcern.
   *
   * Please note that you cannot be sure that the matched documents have been effectively removed and when (hence the Unit return type).
   *
   * @tparam T the type of the selector of documents to remove. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][T] typeclass for handling it has to be in the scope.
   *
   * @param query the selector of documents to remove.
   * @param firstMatchOnly states whether only the first matched documents has to be removed from this collection.
   */
  def uncheckedRemove[T](query: T, firstMatchOnly: Boolean = false)(implicit writer: RawBSONWriter[T], ec: ExecutionContext) : Unit

  /**
   * Remove the matched document(s) from the collection and wait for the [[reactivemongo.core.protocol.commands.LastError]] result.
   *
   * Please read the documentation about [[reactivemongo.core.protocol.commands.GetLastError]] to know how to use it properly.
   *
   * @tparam T the type of the selector of documents to remove. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][T] typeclass for handling it has to be in the scope.
   *
   * @param query the selector of documents to remove.
   * @param writeConcern the [[reactivemongo.core.protocol.commands.GetLastError]] command message to send in order to control how the documents are removed. Defaults to GetLastError().
   * @param firstMatchOnly states whether only the first matched documents has to be removed from this collection.
   *
   * @return a future [[reactivemongo.core.protocol.commands.LastError]] that can be used to check whether the removal was successful.
   */
  def remove[T](query: T, writeConcern: GetLastError = GetLastError(), firstMatchOnly: Boolean = false)(implicit writer: RawBSONWriter[T], ec: ExecutionContext) :Future[LastError]

  // concrete

  /**
   * Find the documents matching the given criteria.
   *
   * This method accepts any query and projection object, provided that there is an implicit [[reactivemongo.bson.handlers.RawBSONWriter]] typeclass for handling them in the scope.
   * You can use the typeclasses defined in [[reactivemongo.bson.handlers.DefaultBSONHandlers]] object.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam Qry the type of the query. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][Qry] typeclass for handling it has to be in the scope.
   * @tparam Pjn the type of the projection object. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][Pjn] typeclass for handling it has to be in the scope.
   * @tparam Rst the type of the matched documents. An implicit [[reactivemongo.bson.handlers.RawBSONReader]][Rst] typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   * @param projection Get only a subset of each matched documents. Defaults to None.
   * @param opts The query options (skip, batchSize, flags...).
   *
   * @return a cursor over the matched documents. You can get an enumerator for it, please see the [[reactivemongo.api.Cursor]] companion object.
   */
  def find[Qry, Pjn, Rst](query: Qry, projection: Pjn, opts: QueryOpts = QueryOpts())(implicit writer: RawBSONWriter[Qry], writer2: RawBSONWriter[Pjn], handler: BSONReaderHandler, reader: RawBSONReader[Rst], ec: ExecutionContext) :FlattenedCursor[Rst] =
    find(writer.write(query), Some(writer2.write(projection)), opts)

  /**
   * Find the documents matching the given criteria.
   *
   * This method accepts any query object, provided that there is an implicit [[reactivemongo.bson.handlers.RawBSONWriter]] typeclass for handling it in the scope.
   * You can use the typeclasses defined in [[reactivemongo.bson.handlers.DefaultBSONHandlers]] object.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam Qry the type of the query. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][Qry] typeclass for handling it has to be in the scope.
   * @tparam Rst the type of the matched documents. An implicit [[reactivemongo.bson.handlers.RawBSONReader]][Rst] typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   * @param opts The query options (skip, batchSize, flags...).
   *
   * @return a cursor over the matched documents. You can get an enumerator for it, please see the [[reactivemongo.api.Cursor]] companion object.
   */
  def find[Qry, Rst](query: Qry, opts: QueryOpts)(implicit writer: RawBSONWriter[Qry], handler: BSONReaderHandler, reader: RawBSONReader[Rst], ec: ExecutionContext) :FlattenedCursor[Rst] =
    find(writer.write(query), None, opts)

  /**
   * Find the documents matching the given criteria.
   *
   * This method accepts any query object, provided that there is an implicit [[reactivemongo.bson.handlers.RawBSONWriter]] typeclass for handling it in the scope.
   * You can use the typeclasses defined in [[reactivemongo.bson.handlers.DefaultBSONHandlers]] object.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam Qry the type of the query. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][Qry] typeclass for handling it has to be in the scope.
   * @tparam Rst the type of the matched documents. An implicit [[reactivemongo.bson.handlers.RawBSONReader]][Rst] typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   *
   * @return a cursor over the matched documents. You can get an enumerator for it, please see the [[reactivemongo.api.Cursor]] companion object.
   */
  def find[Qry, Rst](query: Qry)(implicit writer: RawBSONWriter[Qry], handler: BSONReaderHandler, reader: RawBSONReader[Rst], ec: ExecutionContext) :FlattenedCursor[Rst] =
    find(writer.write(query), None, QueryOpts())

  /**
   * Find the documents matching the given criteria, using the given [[reactivemongo.api.QueryBuilder]] instance.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam Rst the type of the matched documents. An implicit [[reactivemongo.bson.handlers.RawBSONReader]][Rst] typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   * @param opts The query options (skip, batchSize, flags...).
   *
   * @return a cursor over the matched documents. You can get an enumerator for it, please see the [[reactivemongo.api.Cursor]] companion object.
   */
  def find[Rst](query: QueryBuilder, opts: QueryOpts)(implicit handler: BSONReaderHandler, reader: RawBSONReader[Rst], ec: ExecutionContext) :FlattenedCursor[Rst] =
    find(query.makeMergedBuffer, None, opts)

  /**
   * Find the documents matching the given criteria, using the given [[reactivemongo.api.QueryBuilder]] instance.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam Rst the type of the matched documents. An implicit [[reactivemongo.bson.handlers.RawBSONReader]][Rst] typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   *
   * @return a cursor over the matched documents. You can get an enumerator for it, please see the [[reactivemongo.api.Cursor]] companion object.
   */
  def find[Rst](query: QueryBuilder)(implicit handler: BSONReaderHandler, reader: RawBSONReader[Rst], ec: ExecutionContext) :FlattenedCursor[Rst] =
    find(query.makeMergedBuffer, None, QueryOpts())

  /**
   * Inserts a document into the collection and wait for the [[reactivemongo.core.protocol.commands.LastError]] result.
   *
   * Please read the documentation about [[reactivemongo.core.protocol.commands.GetLastError]] to know how to use it properly.
   *
   * @tparam T the type of the document to insert. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][T] typeclass for handling it has to be in the scope.
   *
   * @param document the document to insert.
   *
   * @return a future [[reactivemongo.core.protocol.commands.LastError]] that can be used to check whether the insertion was successful.
   */
  def insert[T](document: T)(implicit writer: RawBSONWriter[T], ec: ExecutionContext) :Future[LastError] = insert(document, GetLastError())

  /**
   * Returns an iteratee that will insert the documents it feeds.
   *
   * This iteratee eventually gives the number of documents that have been inserted into the collection.
   *
   * @tparam T the type of the documents to insert. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][T] typeclass for handling it has to be in the scope.
   * @param bulkSize The number of documents per bulk.
   * @param bulkByteSize The maximum size for a bulk, in bytes.
   */
  def insertIteratee[T](bulkSize: Int = bulk.MaxDocs, bulkByteSize: Int = bulk.MaxBulkSize)(implicit writer: RawBSONWriter[T], ec: ExecutionContext) :Iteratee[T, Int] =
    Enumeratee.map { doc:T => writer.write(doc) } &>> bulk.iteratee(this, bulkSize, bulkByteSize)

  /**
   * Inserts the documents provided by the given enumerator into the collection and eventually returns the number of inserted documents.
   *
   *
   * @tparam T the type of the document to insert. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][T] typeclass for handling it has to be in the scope.
   *
   * @param enumerator An enumerator of `T`.
   * @param bulkSize The number of documents per bulk.
   * @param bulkByteSize The maximum size for a bulk, in bytes.
   *
   * @return a future [[reactivemongo.core.protocol.commands.LastError]] that can be used to check whether the insertion was successful.
   */
  def insert[T](enumerator: Enumerator[T], bulkSize: Int = bulk.MaxDocs, bulkByteSize: Int = bulk.MaxBulkSize)(implicit writer: RawBSONWriter[T], ec: ExecutionContext) :Future[Int] =
    enumerator |>>> insertIteratee(bulkSize, bulkByteSize)
}

/** A mixin that provides implementations for basic operations, following a failover strategy. */
trait FailoverBasicCollection {
  self: Collection =>

  val failoverStrategy: FailoverStrategy

  protected def find[Rst](query: ChannelBuffer, projection: Option[ChannelBuffer], opts: QueryOpts)(implicit handler: BSONReaderHandler, reader: RawBSONReader[Rst], ec: ExecutionContext) :FlattenedCursor[Rst] = {
    val op = Query(opts.flagsN, fullCollectionName, opts.skipN, opts.batchSizeN)
    if(projection.isDefined)
      query.writeBytes(projection.get)
    val requestMaker = RequestMaker(op, query)

    Cursor.flatten(Failover(requestMaker, db.connection.mongosystem, failoverStrategy).future.map { response =>
      val cursor = new DefaultCursor(response, db.connection, op, query, failoverStrategy)
      if( (opts.flagsN & QueryFlags.TailableCursor) != 0 )
        new TailableCursor(cursor)
      else cursor
    })
  }

  def insert[T](document: T, writeConcern: GetLastError)(implicit writer: RawBSONWriter[T], ec: ExecutionContext) :Future[LastError] = {
    val op = Insert(0, fullCollectionName)
    val bson = writer.write(document)
    val checkedWriteRequest = CheckedWriteRequest(op, bson, writeConcern)
    Failover(checkedWriteRequest, db.connection.mongosystem, failoverStrategy).future.mapEither(LastError(_))
  }

  def remove[T](query: T, writeConcern: GetLastError = GetLastError(), firstMatchOnly: Boolean = false)(implicit writer: RawBSONWriter[T], ec: ExecutionContext) :Future[LastError] = {
    val op = Delete(fullCollectionName, if(firstMatchOnly) 1 else 0)
    val bson = writer.write(query)
    val checkedWriteRequest = CheckedWriteRequest(op, bson, writeConcern)
    Failover(checkedWriteRequest, db.connection.mongosystem, failoverStrategy).future.mapEither(LastError(_))
  }

  def uncheckedRemove[T](query: T, firstMatchOnly: Boolean = false)(implicit writer: RawBSONWriter[T], ec: ExecutionContext) : Unit = {
    val op = Delete(fullCollectionName, if(firstMatchOnly) 1 else 0)
    val bson = writer.write(query)
    val message = RequestMaker(op, bson)
    db.connection.send(message)
  }

  def update[S, U](selector: S, update: U, writeConcern: GetLastError = GetLastError(), upsert: Boolean = false, multi: Boolean = false)(implicit selectorWriter: RawBSONWriter[S], updateWriter: RawBSONWriter[U], ec: ExecutionContext) :Future[LastError] = {
    val flags = 0 | (if(upsert) UpdateFlags.Upsert else 0) | (if(multi) UpdateFlags.MultiUpdate else 0)
    val op = Update(fullCollectionName, flags)
    val bson = selectorWriter.write(selector)
    bson.writeBytes(updateWriter.write(update))
    val checkedWriteRequest = CheckedWriteRequest(op, bson, writeConcern)
    Failover(checkedWriteRequest, db.connection.mongosystem, failoverStrategy).future.mapEither(LastError(_))
  }

  def uncheckedUpdate[S, U](selector: S, update: U, upsert: Boolean = false, multi: Boolean = false)(implicit selectorWriter: RawBSONWriter[S], updateWriter: RawBSONWriter[U]) :Unit = {
    val flags = 0 | (if(upsert) UpdateFlags.Upsert else 0) | (if(multi) UpdateFlags.MultiUpdate else 0)
    val op = Update(fullCollectionName, flags)
    val bson = selectorWriter.write(selector)
    bson.writeBytes(updateWriter.write(update))
    val message = RequestMaker(op, bson)
    db.connection.send(message)
  }

  def uncheckedInsert[T](document: T)(implicit writer: RawBSONWriter[T]) :Unit = {
    val op = Insert(0, fullCollectionName)
    val bson = writer.write(document)
    val message = RequestMaker(op, bson)
    db.connection.send(message)
  }
}

/** A mixin that provides commands about this Collection itself. */
trait CollectionMetaCommands {
  self: Collection =>

  /**
   * Creates this collection.
   *
   * The returned future will be completed with an error if this collection already exists.
   *
   * @param autoIndexId States if should automatically add an index on the _id field. By default, regular collections will have an indexed _id field, in contrast to capped collections.
   */
  def create(autoIndexId: Boolean = true)(implicit ec: ExecutionContext) :Future[Boolean] = db.command(new CreateCollection(name, None, if(autoIndexId) None else Some(false)))

  /**
   * Creates this collection as a capped one.
   *
   * The returned future will be completed with an error if this collection already exists.
   *
   * @param autoIndexId States if should automatically add an index on the _id field. By default, capped collections will NOT have an indexed _id field, in contrast to regular collections.
   */
  def createCapped(size: Long, maxDocuments: Option[Int], autoIndexId: Boolean = false)(implicit ec: ExecutionContext) :Future[Boolean] = db.command(new CreateCollection(name, Some(CappedOptions(size, maxDocuments)), if(autoIndexId) Some(true) else None))

  /**
   * Converts this collection to a capped one.
   *
   * @param size The size of this capped collection, in bytes.
   * @param maxDocuments The maximum number of documents this capped collection can contain.
   */
  def convertToCapped(size: Long, maxDocuments: Option[Int])(implicit ec: ExecutionContext) :Future[Boolean] = db.command(new ConvertToCapped(name, CappedOptions(size, maxDocuments)))

  /**
   * Returns various information about this collection.
   */
  def stats()(implicit ec: ExecutionContext) :Future[CollStatsResult] = db.command(new CollStats(name))

  /**
   * Returns various information about this collection.
   *
   * @param scale A scale factor (for example, to get all the sizes in kilobytes).
   */
  def stats(scale: Int)(implicit ec: ExecutionContext) :Future[CollStatsResult] = db.command(new CollStats(name, Some(scale)))

  /** Returns an index manager for this collection. */
  def indexesManager(implicit ec: ExecutionContext) = new IndexesManager(self.db).onCollection(name)
}

/** The default Collection implementation, that mixes in the collection traits. */
case class DefaultCollection(
  name: String,
  db: DB[DefaultCollection],
  failoverStrategy: FailoverStrategy = FailoverStrategy()) extends Collection with FailoverBasicCollection with CollectionMetaCommands

object Collection {
  def apply(name: String, db: DB[DefaultCollection], failoverStrategy: FailoverStrategy = FailoverStrategy()) :DefaultCollection = DefaultCollection(name, db, failoverStrategy)
}