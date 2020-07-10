/*
 * Copyright 2012-2013 Stephane Godbillon (@sgodbillon) and Zenexity
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivemongo.api.collections

import scala.util.Try
import scala.util.control.NonFatal

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api._
import reactivemongo.api.commands.CommandCodecs

import reactivemongo.core.errors.GenericDriverException

trait GenericCollectionProducer[P <: SerializationPack, +C <: GenericCollection[P]] extends CollectionProducer[C] {
  private[reactivemongo] val pack: P
}

/**
 * A Collection that provides default methods using a `SerializationPack`.
 *
 * Some methods of this collection accept instances of `Reader[T]`
 * and `Writer[T]`, that transform any `T` instance into a document,
 * compatible with the selected serialization pack, and vice-versa.
 *
 * @tparam P the serialization pack
 *
 * @define findDescription Finds the documents matching the given criteria (selector)
 * @define queryLink [[http://www.mongodb.org/display/DOCS/Querying MongoDB documentation]]
 * @define selectorParam the document selector
 * @define swriterParam the writer for the selector
 * @define selectorTParam The type of the selector. An implicit `Writer[S]` typeclass for handling it has to be in the scope.
 * @define returnQueryBuilder A query builder you can use to customize the query. You can obtain a cursor by calling the method [[reactivemongo.api.Cursor]] on this query builder.
 * @define implicitWriterT An implicit `Writer[T]` typeclass for handling it has to be in the scope
 * @define writeConcernParam the [[https://docs.mongodb.com/manual/reference/write-concern/ writer concern]] to be used
 * @define bypassDocumentValidationParam the flag to bypass document validation during the operation
 * @define writerParam the writer to create the document
 * @define upsertParam if true, creates a new document if no document is matching, otherwise if at least one document matches, an update is applied
 * @define returnWriteResult a future [[reactivemongo.api.commands.WriteResult]] that can be used to check whether the insertion was successful
 * @define updateParam the update to be applied
 * @define sortParam the document indicating the sort criteria
 * @define fieldsParam the [[http://docs.mongodb.org/manual/tutorial/project-fields-from-query-results/#read-operations-projection projection]] fields
 * @define modifierParam the modify operator to be applied
 * @define readConcernParam the read concern
 * @define firstOpParam the first [[https://docs.mongodb.com/manual/reference/operator/aggregation/ aggregation operator]] of the pipeline
 * @define otherOpsParam the sequence of MongoDB aggregation operations
 * @define explainParam if true indicates to return the information on the processing
 * @define allowDiskUseParam if true enables writing to temporary files
 * @define bypassParam if true enables to bypass document validation during the operation
 * @define readPrefParam the read preference for the result
 * @define aggregation [[http://docs.mongodb.org/manual/reference/command/aggregate/ Aggregates]] the matching documents
 * @define resultTParam The type of the result elements. An implicit `Reader[T]` typeclass for handling it has to be in the scope.
 * @define readerParam the result reader
 * @define cursorFlattenerParam the cursor flattener (by default use the builtin one)
 * @define cursorProducerParam the cursor producer
 * @define aggBatchSizeParam the batch size (for the aggregation cursor; if `None` use the default one)
 * @define aggregationPipelineFunction the function to create the aggregation pipeline using the aggregation framework depending on the collection type
 * @define orderedParam the [[https://docs.mongodb.com/manual/reference/method/db.collection.insert/#perform-an-unordered-insert ordered]] behaviour
 * @define collationParam the collation
 * @define arrayFiltersParam an array of filter documents that determines which array elements to modify for an update operation on an array field
 * @define hintParam the index to use (either the index name or the index document)
 * @define maxTimeParam the time limit for processing operations on a cursor (`maxTimeMS`)
 * @define useDefaultWC Uses the default write concern
 */
trait GenericCollection[P <: SerializationPack]
  extends Collection with PackSupport[P] with GenericCollectionWithCommands[P]
  with CollectionMetaCommands with InsertOps[P] with UpdateOps[P]
  with DeleteOps[P] with CountOp[P] with DistinctOp[P]
  with GenericCollectionWithDistinctOps[P]
  with FindAndModifyOps[P] with ChangeStreamOps[P]
  with AggregationOps[P] with GenericCollectionMetaCommands[P]
  with QueryBuilderFactory[P] { self =>

  /** Upper MongoDB version (used for version checks) */
  protected lazy val version = db.connectionState.metadata.maxWireVersion

  import AggregationFramework.{ Pipeline => AggregationPipeline }

  private[reactivemongo] implicit def packIdentityReader: pack.Reader[pack.Document] = pack.IdentityReader

  private[reactivemongo] implicit def packIdentityWriter: pack.Writer[pack.Document] = pack.IdentityWriter

  implicit protected lazy val unitReader: pack.Reader[Unit] =
    CommandCodecs.unitReader[pack.type](pack)

  /** Builder used to prepare queries */
  private[reactivemongo] lazy val genericQueryBuilder = new QueryBuilder(
    collection = this,
    failoverStrategy = this.failoverStrategy,
    readConcern = this.readConcern,
    readPreference = this.readPreference)

  // Required for ops (e.g. InsertOps, ...)
  private[reactivemongo] def session(): Option[Session] = db.session

  /**
   * Returns a new reference to the same collection,
   * with the given read preference.
   */
  def withReadPreference(pref: ReadPreference): GenericCollection[P]

  /**
   * $findDescription, with the projection applied.
   * @see $queryLink
   *
   * @tparam S $selectorTParam
   * @tparam P The type of the projection object. An implicit `Writer[P]` typeclass for handling it has to be in the scope.
   *
   * @param selector $selectorParam
   * @param swriter $swriterParam
   * @param pwriter the writer for the projection
   * @return $returnQueryBuilder
   */
  def find[S](selector: S)(implicit swriter: pack.Writer[S]): QueryBuilder = find(selector, Option.empty[pack.Document])

  /**
   * $findDescription, with the projection applied.
   * @see $queryLink
   *
   * @tparam S $selectorTParam
   * @tparam P The type of the projection object. An implicit `Writer[P]` typeclass for handling it has to be in the scope.
   *
   * @param selector $selectorParam
   * @param projection the projection document to select only a subset of each matching documents
   * @param swriter $swriterParam
   * @param pwriter the writer for the projection
   * @return $returnQueryBuilder
   */
  def find[S, J](selector: S, projection: Option[J])(implicit swriter: pack.Writer[S], pwriter: pack.Writer[J]): QueryBuilder = {
    val queryBuilder = genericQueryBuilder.filter(selector)

    projection.fold(queryBuilder) { queryBuilder.projection(_) }
  }

  /**
   * Counts the matching documents.
   * @see $queryLink
   *
   * @param selector $selectorParam
   * @param limit the maximum number of matching documents to count
   * @param skip the number of matching documents to skip before counting
   * @param hint the index to use (either the index name or the index document; see `hint(..)`)
   * @param readConcern $readConcernParam
   * @param readPreference $readPrefParam
   */
  def count(
    selector: Option[pack.Document] = None,
    limit: Option[Int] = None,
    skip: Int = 0,
    hint: Option[Hint] = None,
    readConcern: ReadConcern = this.readConcern,
    readPreference: ReadPreference = this.readPreference)(implicit ec: ExecutionContext): Future[Long] = countDocuments(
    selector, limit, skip, hint, readConcern, readPreference)

  /**
   * Returns an unordered builder for insert operations.
   * $useDefaultWC.
   *
   * @tparam T The type of the document to insert. $implicitWriterT.
   * @param ordered $orderedParam
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def one(coll: BSONCollection, singleDoc: BSONDocument)(
   *   implicit ec: ExecutionContext) =
   *   coll.insert.one(singleDoc)
   *
   * def many(coll: BSONCollection, multiInserts: Iterable[BSONDocument])(
   *   implicit ec: ExecutionContext) =
   *   coll.insert.many(multiInserts)
   * }}}
   */
  def insert: InsertBuilder = prepareInsert(false, writeConcern, false)

  /**
   * Returns a builder for insert operations.
   * $useDefaultWC.
   *
   * @tparam T The type of the document to insert. $implicitWriterT.
   * @param ordered $orderedParam
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def one(coll: BSONCollection, singleDoc: BSONDocument)(
   *   implicit ec: ExecutionContext) =
   *   coll.insert(ordered = true).one(singleDoc)
   *
   * def many(coll: BSONCollection, multiInserts: Iterable[BSONDocument])(
   *   implicit ec: ExecutionContext) =
   *   coll.insert(ordered = true).many(multiInserts)
   * }}}
   */
  def insert(ordered: Boolean): InsertBuilder =
    prepareInsert(ordered, writeConcern, false)

  /**
   * Returns an ordered builder for insert operations.
   *
   * @tparam T The type of the document to insert. $implicitWriterT.
   * @param writeConcern $writeConcernParam
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.WriteConcern
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def one(coll: BSONCollection, singleDoc: BSONDocument)(
   *   implicit ec: ExecutionContext) =
   *   coll.insert(writeConcern = WriteConcern.Default).one(singleDoc)
   *
   * def many(coll: BSONCollection, multiInserts: Iterable[BSONDocument])(
   *   implicit ec: ExecutionContext) =
   *   coll.insert(writeConcern = WriteConcern.Default).many(multiInserts)
   * }}}
   */
  def insert(writeConcern: WriteConcern): InsertBuilder =
    prepareInsert(false, writeConcern, false)

  /**
   * Returns a builder for insert operations.
   *
   * @tparam T The type of the document to insert. $implicitWriterT.
   *
   * @param ordered $orderedParam
   * @param writeConcern $writeConcernParam
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.WriteConcern
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def withDefaultWriteConcern(coll: BSONCollection, query: BSONDocument)(
   *   implicit ec: ExecutionContext) =
   *   coll.insert(true, WriteConcern.Journaled).one(query)
   * }}}
   */
  def insert(ordered: Boolean, writeConcern: WriteConcern): InsertBuilder =
    prepareInsert(ordered, writeConcern, false)

  /**
   * Returns a builder for insert operations.
   * $useDefaultWC.
   *
   * @tparam T The type of the document to insert. $implicitWriterT.
   *
   * @param ordered $orderedParam
   * @param bypassDocumentValidation $bypassDocumentValidationParam
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def withDefaultWriteConcern(coll: BSONCollection, query: BSONDocument)(
   *   implicit ec: ExecutionContext) =
   *   coll.insert(true, false).one(query)
   * }}}
   */
  def insert(
    ordered: Boolean,
    bypassDocumentValidation: Boolean): InsertBuilder =
    prepareInsert(ordered, this.writeConcern, bypassDocumentValidation)

  /**
   * Returns a builder for insert operations.
   *
   * @tparam T The type of the document to insert. $implicitWriterT.
   *
   * @param ordered $orderedParam
   * @param writeConcern $writeConcernParam
   * @param bypassDocumentValidation $bypassDocumentValidationParam
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.WriteConcern
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def withDefaultWriteConcern(coll: BSONCollection, query: BSONDocument)(
   *   implicit ec: ExecutionContext) =
   *   coll.insert(true, WriteConcern.Journaled, true).one(query)
   * }}}
   */
  def insert(
    ordered: Boolean,
    writeConcern: WriteConcern,
    bypassDocumentValidation: Boolean): InsertBuilder =
    prepareInsert(ordered, writeConcern, bypassDocumentValidation)

  /**
   * Returns an unordered update builder.
   * $useDefaultWC.
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def withDefaultWriteConcern(
   *   coll: BSONCollection,
   *   query: BSONDocument,
   *   update: BSONDocument
   * )(implicit ec: ExecutionContext) = {
   *   coll.update.one(query, update, upsert = false, multi = false)
   * }
   * }}}
   */
  def update: UpdateBuilder = prepareUpdate(false, writeConcern, false)

  /**
   * Returns an update builder.
   * $useDefaultWC.
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def withDefaultWriteConcern(
   *   coll: BSONCollection,
   *   query: BSONDocument,
   *   update: BSONDocument
   * )(implicit ec: ExecutionContext) = {
   *   coll.update(ordered = true).
   *     one(query, update, upsert = false, multi = false)
   * }
   * }}}
   *
   * @param ordered $orderedParam
   */
  def update(ordered: Boolean): UpdateBuilder =
    prepareUpdate(ordered, writeConcern, false)

  /**
   * Returns an ordered update builder.
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.WriteConcern
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def orderedUpdate(
   *   coll: BSONCollection,
   *   query: BSONDocument,
   *   update: BSONDocument,
   *   wc: WriteConcern
   * )(implicit ec: ExecutionContext) =
   *   coll.update(writeConcern = wc).one(query, update)
   * }}}
   *
   * @param writeConcern $writeConcernParam
   */
  def update(writeConcern: WriteConcern): UpdateBuilder =
    prepareUpdate(true, writeConcern, false)

  /**
   * Returns an update builder.
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.WriteConcern
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def myUpdate(
   *   coll: BSONCollection,
   *   query: BSONDocument,
   *   update: BSONDocument,
   *   wc: WriteConcern
   * )(implicit ec: ExecutionContext) =
   *   coll.update(ordered = false, writeConcern = wc).one(query, update)
   * }}}
   *
   * @param ordered $orderedParam
   * @param writeConcern $writeConcernParam
   */
  def update(
    ordered: Boolean,
    writeConcern: WriteConcern): UpdateBuilder =
    prepareUpdate(ordered, writeConcern, false)

  /**
   * Returns an update builder.
   * $useDefaultWC.
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def withDefaultWriteConcern(
   *   coll: BSONCollection,
   *   query: BSONDocument,
   *   update: BSONDocument
   * )(implicit ec: ExecutionContext) = coll.update(
   *   ordered = false,
   *   bypassDocumentValidation = true
   * ).one(query, update)
   * }}}
   *
   * @param ordered $orderedParam
   * @param writeConcern $writeConcernParam
   * @param bypassDocumentValidation $bypassDocumentValidationParam
   */
  def update(
    ordered: Boolean,
    bypassDocumentValidation: Boolean): UpdateBuilder =
    prepareUpdate(ordered, this.writeConcern, bypassDocumentValidation)

  /**
   * Returns an update builder.
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.WriteConcern
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def myUpdate(
   *   coll: BSONCollection,
   *   query: BSONDocument,
   *   update: BSONDocument,
   *   wc: WriteConcern
   * )(implicit ec: ExecutionContext) = coll.update(
   *   ordered = false,
   *   writeConcern = wc,
   *   bypassDocumentValidation = true
   * ).one(query, update)
   * }}}
   *
   * @param ordered $orderedParam
   * @param writeConcern $writeConcernParam
   * @param bypassDocumentValidation $bypassDocumentValidationParam
   */
  def update(
    ordered: Boolean,
    writeConcern: WriteConcern,
    bypassDocumentValidation: Boolean): UpdateBuilder =
    prepareUpdate(ordered, writeConcern, bypassDocumentValidation)

  /**
   * Returns an update modifier, to be used with `findAndModify`.
   *
   * @param update $updateParam
   * @param fetchNewObject the command result must be the new object instead of the old one.
   * @param upsert $upsertParam
   */
  def updateModifier[U](update: U, fetchNewObject: Boolean = false, upsert: Boolean = false)(implicit updateWriter: pack.Writer[U]): FindAndUpdateOp = new FindAndUpdateOp(pack.serialize(update, updateWriter), fetchNewObject, upsert)

  /** Returns a removal modifier, to be used with `findAndModify`. */
  @inline def removeModifier = FindAndRemoveOp

  /**
   * Applies a [[http://docs.mongodb.org/manual/reference/command/findAndModify/ findAndModify]] operation. See `findAndUpdate` and `findAndRemove` convenient functions.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.bson.{ BSONDocument, BSONDocumentReader }
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * case class Person(name: String, age: Int)
   *
   * def foo(coll: BSONCollection)(
   *   implicit ec: ExecutionContext, r: BSONDocumentReader[Person]) = {
   *   val updateOp = coll.updateModifier(
   *     BSONDocument(f"$$set" -> BSONDocument("age" -> 35)))
   *
   *   val personBeforeUpdate: Future[Option[Person]] =
   *     coll.findAndModify(BSONDocument("name" -> "Joline"), updateOp).
   *     map(_.result[Person])
   *
   *   val removedPerson: Future[Option[Person]] = coll.findAndModify(
   *     BSONDocument("name" -> "Jack"), coll.removeModifier).
   *     map(_.result[Person])
   *
   *   val _ = List(personBeforeUpdate, removedPerson)
   * }
   * }}}
   *
   * @param tparam S $selectorTParam
   *
   * @param selector $selectorParam
   * @param modifier $modifierParam
   * @param sort $sortParam (default: `None`)
   * @param fields $fieldsParam
   * @param bypassDocumentValidation $bypassDocumentValidationParam (default: `false`)
   * @param writeConcern $writeConcernParam
   * @param maxTime $maxTimeParam
   * @param collation $collationParam
   * @param arrayFilters $arrayFiltersParam
   * @param swriter $swriterParam
   */
  @SuppressWarnings(Array("MaxParameters"))
  def findAndModify[S](
    selector: S,
    modifier: FindAndModifyOp,
    sort: Option[pack.Document] = None,
    fields: Option[pack.Document] = None,
    bypassDocumentValidation: Boolean = false,
    writeConcern: WriteConcern = this.writeConcern,
    maxTime: Option[FiniteDuration] = None,
    collation: Option[Collation] = None,
    arrayFilters: Seq[pack.Document] = Seq.empty)(implicit swriter: pack.Writer[S], ec: ExecutionContext): Future[FindAndModifyResult] = {

    val op = prepareFindAndModify(
      selector = selector,
      modifier = modifier,
      sort = sort,
      fields = fields,
      bypassDocumentValidation = bypassDocumentValidation,
      writeConcern = writeConcern,
      maxTime = maxTime,
      collation = collation,
      arrayFilters = arrayFilters)

    op()
  }

  /**
   * Finds some matching document, and updates it (using `findAndModify`).
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def findPerson(coll: BSONCollection)(
   *   implicit ec: ExecutionContext): Future[Option[BSONDocument]] =
   *   coll.findAndUpdate(
   *     BSONDocument("name" -> "James"),
   *     BSONDocument(f"$$set" -> BSONDocument("age" -> 17)),
   *     fetchNewObject = true).map(_.value)
   *     // on success, return the update document: { "age": 17 }
   * }}}
   *
   * @tparam selectorTParam
   *
   * @param selector $selectorParam
   * @param update $updateParam
   * @param fetchNewObject the command result must be the new object instead of the old one (default: `false`)
   * @param upsert $upsertParam (default: `false`)
   * @param sort $sortParam (default: `None`)
   * @param fields $fieldsParam
   * @param bypassDocumentValidation $bypassDocumentValidationParam (default: `false`)
   * @param writeConcern $writeConcernParam
   * @param swriter $swriterParam
   * @param writer $writerParam
   * @param maxTime $maxTimeParam
   * @param collation $collationParam
   * @param arrayFilters $arrayFiltersParam
   */
  @SuppressWarnings(Array("MaxParameters"))
  def findAndUpdate[S, T](
    selector: S,
    update: T,
    fetchNewObject: Boolean = false,
    upsert: Boolean = false,
    sort: Option[pack.Document] = None,
    fields: Option[pack.Document] = None,
    bypassDocumentValidation: Boolean = false,
    writeConcern: WriteConcern = this.writeConcern,
    maxTime: Option[FiniteDuration] = None,
    collation: Option[Collation] = None,
    arrayFilters: Seq[pack.Document] = Seq.empty)(
    implicit
    swriter: pack.Writer[S],
    writer: pack.Writer[T],
    ec: ExecutionContext): Future[FindAndModifyResult] = {
    val updateOp = updateModifier(update, fetchNewObject, upsert)

    findAndModify(selector, updateOp, sort, fields,
      bypassDocumentValidation = bypassDocumentValidation,
      writeConcern = writeConcern,
      maxTime = maxTime,
      collation = collation,
      arrayFilters = arrayFilters)
  }

  /**
   * Finds some matching document, and removes it (using `findAndModify`).
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.bson.{ BSONDocument, BSONDocumentReader }
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * case class Person(name: String, age: Int)
   *
   * def removed(coll: BSONCollection)(
   *   implicit ec: ExecutionContext,
   *   r: BSONDocumentReader[Person]): Future[Option[Person]] =
   *   coll.findAndRemove(
   *     BSONDocument("name" -> "Foo")).map(_.result[Person])
   * }}}
   *
   * @tparam S $selectorTParam
   *
   * @param selector $selectorParam
   * @param modifier $modifierParam
   * @param sort $sortParam
   * @param fields $fieldsParam
   * @param writeConcern $writeConcernParam
   * @param maxTime $maxTimeParam
   * @param collation $collationParam
   * @param arrayFilters $arrayFiltersParam
   * @param swriter $swriterParam
   */
  def findAndRemove[S](
    selector: S,
    sort: Option[pack.Document] = None,
    fields: Option[pack.Document] = None,
    writeConcern: WriteConcern = this.writeConcern,
    maxTime: Option[FiniteDuration] = None,
    collation: Option[Collation] = None,
    arrayFilters: Seq[pack.Document] = Seq.empty)(implicit swriter: pack.Writer[S], ec: ExecutionContext): Future[FindAndModifyResult] = findAndModify[S](
    selector, removeModifier, sort, fields,
    bypassDocumentValidation = false,
    writeConcern = writeConcern,
    maxTime = maxTime,
    collation = collation,
    arrayFilters = arrayFilters)

  /**
   * $aggregation.
   *
   * @tparam T $resultTParam
   *
   * @param explain $explainParam of the pipeline
   * @param allowDiskUse $allowDiskUseParam
   * @param bypassDocumentValidation $bypassDocumentValidationParam (default: `false`)
   * @param readConcern $readConcernParam
   * @param readPreference $readPrefParam (default: primary)
   * @param batchSize $aggBatchSizeParam
   * @param f $aggregationPipelineFunction
   * @param reader $readerParam
   */
  def aggregateWith[T](
    explain: Boolean = false,
    allowDiskUse: Boolean = false,
    bypassDocumentValidation: Boolean = false,
    readConcern: ReadConcern = this.readConcern,
    readPreference: ReadPreference = ReadPreference.primary,
    batchSize: Option[Int] = None)(
    f: AggregationFramework => AggregationPipeline)(
    implicit
    reader: pack.Reader[T],
    cp: CursorProducer[T]): cp.ProducedCursor = {

    val pipeline = f(AggregationFramework)

    val aggregateCursor: Cursor.WithOps[T] = aggregatorContext[T](
      pipeline, explain, allowDiskUse,
      bypassDocumentValidation, readConcern, readPreference,
      this.writeConcern, batchSize).
      prepared[Cursor.WithOps](CursorProducer.defaultCursorProducer[T]).cursor

    cp.produce(aggregateCursor)
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/command/aggregate/ Aggregates]] the matching documents.
   *
   * {{{
   * import scala.concurrent.Future
   * import scala.concurrent.ExecutionContext.Implicits.global
   *
   * import reactivemongo.api.Cursor
   * import reactivemongo.api.bson._
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def populatedStates(cities: BSONCollection): Future[List[BSONDocument]] = {
   *   import cities.AggregationFramework
   *   import AggregationFramework.{ Group, Match, SumField }
   *
   *   cities.aggregatorContext[BSONDocument](
   *     List(Group(BSONString(f"$$state"))(
   *       "totalPop" -> SumField("population")),
   *         Match(BSONDocument("totalPop" ->
   *           BSONDocument(f"$$gte" -> 10000000L))))
   *   ).prepared.cursor.collect[List](
   *     maxDocs = 3,
   *     err = Cursor.FailOnError[List[BSONDocument]]()
   *   )
   * }
   * }}}
   *
   * @tparam T $resultTParam
   *
   * @param firstOperator $firstOpParam
   * @param otherOperators $otherOpsParam
   * @param cursor aggregation cursor option (optional)
   * @param explain $explainParam of the pipeline (default: `false`)
   * @param allowDiskUse $allowDiskUseParam (default: `false`)
   * @param bypassDocumentValidation $bypassDocumentValidationParam (default: `false`)
   * @param readConcern $readConcernParam
   * @param readPreference $readPrefParam
   * @param writeConcern $writeConcernParam
   * @param batchSize $aggBatchSizeParam
   * @param cursorOptions the options for the result cursor
   * @param maxTime $maxTimeParam
   * @param hint $hintParam
   * @param comment the [[https://docs.mongodb.com/manual/reference/method/cursor.comment/#cursor.comment comment]] to annotation the aggregation command
   * @param collation $collationParam
   * @param reader $readerParam
   * @param cp $cursorProducerParam
   */
  @SuppressWarnings(Array("MaxParameters"))
  def aggregatorContext[T](
    pipeline: List[PipelineOperator] = List.empty,
    explain: Boolean = false,
    allowDiskUse: Boolean = false,
    bypassDocumentValidation: Boolean = false,
    readConcern: ReadConcern = this.readConcern,
    readPreference: ReadPreference = this.readPreference,
    writeConcern: WriteConcern = this.writeConcern,
    batchSize: Option[Int] = None,
    cursorOptions: CursorOptions = CursorOptions.empty,
    maxTime: Option[FiniteDuration] = None,
    hint: Option[Hint] = None,
    comment: Option[String] = None,
    collation: Option[Collation] = None)(implicit reader: pack.Reader[T]): AggregatorContext[T] = {
    new AggregatorContext[T](
      pipeline,
      explain,
      allowDiskUse,
      bypassDocumentValidation,
      readConcern,
      writeConcern,
      readPreference,
      batchSize,
      cursorOptions,
      maxTime,
      reader,
      hint,
      comment,
      collation)
  }

  /**
   * Prepares an unordered [[https://docs.mongodb.com/manual/reference/command/delete/ delete]] builder.
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def doIt(coll: BSONCollection, query: BSONDocument)(
   *   implicit ec: ExecutionContext) = coll.delete.one(query)
   *
   * def equivalentTo(coll: BSONCollection) = coll.delete(false)
   * }}}
   */
  def delete: DeleteBuilder = prepareDelete(false, writeConcern)

  /**
   * Prepares a [[https://docs.mongodb.com/manual/reference/command/delete/ delete]] builder.
   *
   * @param ordered $orderedParam
   * @param writeConcern $writeConcernParam
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.bson.BSONDocument
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def foo(coll: BSONCollection, query: BSONDocument)(
   *   implicit ec: ExecutionContext) = coll.delete(true).one(query)
   * }}}
   */
  def delete(ordered: Boolean = true, writeConcern: WriteConcern = writeConcern): DeleteBuilder = prepareDelete(ordered, writeConcern)

  // --- Internals ---

  /** The read preference for the write operations (primary) */
  @inline protected def writePreference: ReadPreference = ReadPreference.Primary

  /** The default write concern */
  @inline protected def writeConcern = db.connection.options.writeConcern

  /** The default read preference */
  @inline def readPreference: ReadPreference

  /** The default read concern */
  @inline protected def readConcern = db.connection.options.readConcern

  @inline protected def defaultCursorBatchSize: Int = 101

  @SuppressWarnings(Array("TryGet"))
  protected def watchFailure[T](future: => Future[T]): Future[T] =
    Try(future).recover { case NonFatal(e) => Future.failed(e) }.get

  @inline protected def unsupportedVersion(metadata: reactivemongo.core.protocol.ProtocolMetadata) = new GenericDriverException(s"Unsupported MongoDB version: $metadata")

  override def toString: String = s"collection[${name}]"
}
