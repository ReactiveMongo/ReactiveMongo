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

import scala.collection.generic.CanBuildFrom

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api._
import reactivemongo.api.commands.{
  CommandCodecs,
  Collation,
  ImplicitCommandHelpers,
  UnitBox,
  UpdateWriteResult,
  ResolvedCollectionCommand,
  WriteConcern,
  WriteResult
}

import reactivemongo.core.protocol.MongoWireVersion
import reactivemongo.core.errors.ConnectionNotInitialized

trait GenericCollectionProducer[P <: SerializationPack with Singleton, +C <: GenericCollection[P]] extends CollectionProducer[C]

/**
 * A Collection that provides default methods using a `SerializationPack`
 * (e.g. the default [[reactivemongo.api.BSONSerializationPack]]).
 *
 * Some methods of this collection accept instances of `Reader[T]` and `Writer[T]`, that transform any `T` instance into a document, compatible with the selected serialization pack, and vice-versa.
 *
 * @tparam P the serialization pack
 *
 * @define findDescription Finds the documents matching the given criteria (selector)
 * @define queryLink [[http://www.mongodb.org/display/DOCS/Querying MongoDB documentation]]
 * @define selectorParam the document selector
 * @define swriterParam the writer for the selector
 * @define selectorTParam The type of the selector. An implicit `Writer[S]` typeclass for handling it has to be in the scope.
 * @define returnQueryBuilder A [[GenericQueryBuilder]] that you can use to to customize the query. You can obtain a cursor by calling the method [[reactivemongo.api.Cursor]] on this query builder.
 * @define implicitWriterT An implicit `Writer[T]` typeclass for handling it has to be in the scope
 * @define writeConcernParam the [[https://docs.mongodb.com/manual/reference/write-concern/ writer concern]] to be used
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
 */
trait GenericCollection[P <: SerializationPack with Singleton]
  extends Collection with GenericCollectionWithCommands[P]
  with CollectionMetaCommands with ImplicitCommandHelpers[P] with InsertOps[P]
  with UpdateOps[P] with DeleteOps[P] with CountOp[P] with DistinctOp[P] with ChangeStreamOps[P]
  with Aggregator[P] with GenericCollectionMetaCommands[P]
  with GenericCollectionWithQueryBuilder[P] with HintFactory[P] { self =>

  import scala.language.higherKinds

  val pack: P

  /** Upper MongoDB version (used for version checks) */
  protected lazy val version = db.connectionState.metadata.maxWireVersion

  protected val BatchCommands: BatchCommands[pack.type]
  import BatchCommands._

  /**
   * Alias for type of the aggregation framework,
   * depending on the type of the collection.
   *
   * @see [[reactivemongo.api.commands.AggregationFramework]]
   */
  type AggregationFramework = BatchCommands.AggregationFramework.type

  /**
   * Alias for [[reactivemongo.api.commands.AggregationFramework.PipelineOperator]]
   */
  type PipelineOperator = BatchCommands.AggregationFramework.PipelineOperator

  @deprecated("Internal: will be made private", "0.16.0")
  implicit def PackIdentityReader: pack.Reader[pack.Document] = pack.IdentityReader

  @deprecated("Internal: will be made private", "0.16.0")
  implicit def PackIdentityWriter: pack.Writer[pack.Document] = pack.IdentityWriter

  implicit protected lazy val unitBoxReader: pack.Reader[UnitBox.type] =
    CommandCodecs.unitBoxReader[pack.type](pack)

  /** Builder used to prepare queries */
  private[reactivemongo] lazy val genericQueryBuilder: GenericQueryBuilder[pack.type] = new CollectionQueryBuilder(failoverStrategy)

  /**
   * Returns a new reference to the same collection,
   * with the given read preference.
   */
  def withReadPreference(pref: ReadPreference): GenericCollection[P]

  /**
   * $findDescription.
   * @see $queryLink
   *
   * @tparam S $selectorTParam
   *
   * @param selector $selectorParam
   * @param swriter $swriterParam
   * @return $returnQueryBuilder
   */
  @deprecated("Use `find` with optional `projection`", "0.16.0")
  def find[S](selector: S)(implicit swriter: pack.Writer[S]): GenericQueryBuilder[pack.type] = genericQueryBuilder.query(selector)

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
  @deprecated("Use `find` with optional `projection`", "0.16.0")
  def find[S, J](selector: S, projection: J)(implicit swriter: pack.Writer[S], pwriter: pack.Writer[J]): GenericQueryBuilder[pack.type] = genericQueryBuilder.query(selector).projection(projection)

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
  def find[S, J](selector: S, projection: Option[J] = Option.empty)(implicit swriter: pack.Writer[S], pwriter: pack.Writer[J]): GenericQueryBuilder[pack.type] = {
    val queryBuilder: GenericQueryBuilder[pack.type] =
      genericQueryBuilder.query(selector)

    projection.fold(queryBuilder) { queryBuilder.projection(_) }
  }

  /**
   * Counts the matching documents.
   * @see $queryLink
   *
   * @tparam H The type of hint. An implicit `H => Hint` conversion has to be in the scope.
   *
   * @param selector $selectorParam (default: `None` to count all)
   * @param limit the maximum number of matching documents to count
   * @param skip the number of matching documents to skip before counting
   * @param hint the index to use (either the index name or the index document)
   */
  @deprecated("Use `count` with `readConcern` parameter", "0.16.0")
  def count[H](selector: Option[pack.Document] = None, limit: Int = 0, skip: Int = 0, hint: Option[H] = None)(implicit h: H => CountCommand.Hint, ec: ExecutionContext): Future[Int] = runValueCommand(CountCommand.Count(query = selector, limit, skip, hint.map(h)), readPreference)

  /**
   * Counts the matching documents.
   * @see $queryLink
   *
   * @param selector $selectorParam
   * @param limit the maximum number of matching documents to count
   * @param skip the number of matching documents to skip before counting
   * @param hint the index to use (either the index name or the index document; see `hint(..)`)
   * @param readConcern $readConcernParam
   */
  def count(
    selector: Option[pack.Document],
    limit: Option[Int],
    skip: Int,
    hint: Option[Hint[pack.type]],
    readConcern: ReadConcern)(implicit ec: ExecutionContext): Future[Long] =
    countDocuments(selector, limit, skip, hint, readConcern)

  @deprecated("Use `distinct` with `Collation`", "0.16.0")
  def distinct[T, M[_] <: Iterable[_]](
    key: String,
    selector: Option[pack.Document] = None,
    readConcern: ReadConcern = self.readConcern)(implicit reader: pack.NarrowValueReader[T], ec: ExecutionContext, cbf: CanBuildFrom[M[_], T, M[T]]): Future[M[T]] = distinctDocuments[T, M](key, selector, readConcern, collation = None)

  /**
   * Returns the distinct values for a specified field
   * across a single collection.
   *
   * @tparam T the element type of the distinct values
   * @tparam M the container, that must be a [[scala.collection.Iterable]]
   *
   * @param key the field for which to return distinct values
   * @param selector $selectorParam, that specifies the documents from which to retrieve the distinct values.
   * @param readConcern $readConcernParam
   * @param collation $collationParam
   *
   * {{{
   * val distinctStates = collection.distinct[String, Set](
   *   "state", None, ReadConcern.Local, None)
   * }}}
   */
  def distinct[T, M[_] <: Iterable[_]](
    key: String,
    @deprecatedName('query) selector: Option[pack.Document],
    readConcern: ReadConcern,
    collation: Option[Collation])(implicit
    reader: pack.NarrowValueReader[T],
    ec: ExecutionContext, cbf: CanBuildFrom[M[_], T, M[T]]): Future[M[T]] =
    distinctDocuments[T, M](key, selector, readConcern, collation)

  /**
   * Inserts a document into the collection and waits for the [[reactivemongo.api.commands.WriteResult]].
   *
   * @tparam T The type of the document to insert. $implicitWriterT.
   *
   * @param document the document to insert
   * @param writeConcern $writeConcernParam
   * @param writer $writerParam to be inserted
   * @return $returnWriteResult
   *
   * {{{
   * collection.insert(myDoc)
   *
   * // Equivalent to:
   * collection.insert(true, defaultWriteConcern).one(document)
   * }}}
   */
  @deprecated("Use `.insert(ordered = false).one(..)`", "0.16.1")
  def insert[T](document: T, writeConcern: WriteConcern = writeConcern)(implicit writer: pack.Writer[T], ec: ExecutionContext): Future[WriteResult] =
    prepareInsert(true, writeConcern).one(document)

  /**
   * Returns an unordered builder for insert operations.
   * Uses the default write concern.
   *
   * @tparam T The type of the document to insert. $implicitWriterT.
   * @param ordered $orderedParam
   *
   * {{{
   * collection.insert(ordered = true).one(singleDoc)
   *
   * collection.insert(ordered = true).many(multiInserts)
   * }}}
   */
  def insert: InsertBuilder = prepareInsert(false, writeConcern)

  /**
   * Returns a builder for insert operations.
   * Uses the default write concern.
   *
   * @tparam T The type of the document to insert. $implicitWriterT.
   * @param ordered $orderedParam
   *
   * {{{
   * collection.insert(ordered = true).one(singleDoc)
   *
   * collection.insert(ordered = true).many(multiInserts)
   * }}}
   */
  def insert(ordered: Boolean): InsertBuilder =
    prepareInsert(ordered, writeConcern)

  /**
   * Returns a builder for insert operations.
   *
   * @tparam T The type of the document to insert. $implicitWriterT.
   *
   * @param ordered $orderedParam
   * @param writeConcern $writeConcernParam
   *
   * {{{
   * collection.insert(true, aWriteConcern).one(singleDoc)
   *
   * collection.insert(true, aWriteConcern).many(multiInserts)
   * }}}
   */
  def insert(ordered: Boolean, writeConcern: WriteConcern): InsertBuilder =
    prepareInsert(ordered, writeConcern)

  /**
   * Updates one or more documents matching the given selector
   * with the given modifier or update object.
   *
   * @tparam S $selectorTParam
   * @tparam T The type of the modifier or update object. $implicitWriterT.
   *
   * @param selector the selector object, for finding the documents to update.
   * @param update the modifier object (with special keys like \$set) or replacement object.
   * @param writeConcern $writeConcernParam
   * @param upsert $upsertParam (defaults: `false`)
   * @param multi states whether the update may be done on all the matching documents (default: `false`)
   * @param swriter $swriterParam
   * @param writer $writerParam
   *
   * @return $returnWriteResult
   */
  @deprecated("Use `.update(ordered = false).one(..)`", "0.16.1")
  def update[S, T](selector: S, update: T, writeConcern: WriteConcern = writeConcern, upsert: Boolean = false, multi: Boolean = false)(implicit swriter: pack.Writer[S], writer: pack.Writer[T], ec: ExecutionContext): Future[UpdateWriteResult] = prepareUpdate(ordered = true, writeConcern = writeConcern).
    one(selector, update, upsert, multi)

  /**
   * Returns an unordered update builder.
   *
   * {{{
   * collection.update.one(query, update, upsert = false, multi = false)
   * }}}
   */
  def update: UpdateBuilder = prepareUpdate(false, writeConcern)

  /**
   * Returns an update builder.
   *
   * {{{
   * collection.update(ordered = true).
   *   one(query, update, upsert = false, multi = false)
   * }}}
   */
  def update(ordered: Boolean): UpdateBuilder =
    prepareUpdate(ordered, writeConcern)

  /**
   * Returns an update builder.
   *
   * {{{
   * collection.update(ordered, writeConcern).many(updates)
   * }}}
   */
  def update(
    ordered: Boolean,
    writeConcern: WriteConcern): UpdateBuilder =
    prepareUpdate(ordered, writeConcern)

  /**
   * Returns an update modifier, to be used with `findAndModify`.
   *
   * @param update $updateParam
   * @param fetchNewObject the command result must be the new object instead of the old one.
   * @param upsert $upsertParam
   */
  def updateModifier[U](update: U, fetchNewObject: Boolean = false, upsert: Boolean = false)(implicit updateWriter: pack.Writer[U]): BatchCommands.FindAndModifyCommand.Update = BatchCommands.FindAndModifyCommand.Update(update, fetchNewObject, upsert)

  /** Returns a removal modifier, to be used with `findAndModify`. */
  @deprecated("Internal: will be made private", "0.16.0")
  @transient lazy val removeModifier =
    BatchCommands.FindAndModifyCommand.Remove

  @deprecated("Use other `findAndModify`", "0.14.0")
  def findAndModify[S](selector: S, modifier: BatchCommands.FindAndModifyCommand.Modify, sort: Option[pack.Document] = None, fields: Option[pack.Document] = None)(implicit swriter: pack.Writer[S], ec: ExecutionContext): Future[BatchCommands.FindAndModifyCommand.FindAndModifyResult] = findAndModify[S](
    selector, modifier, sort, fields,
    bypassDocumentValidation = false,
    writeConcern = writeConcern,
    maxTime = Option.empty[FiniteDuration],
    collation = Option.empty[Collation],
    arrayFilters = Seq.empty[pack.Document])

  /**
   * Applies a [[http://docs.mongodb.org/manual/reference/command/findAndModify/ findAndModify]] operation. See [[findAndUpdate]] and [[findAndRemove]] convenient functions.
   *
   * {{{
   * val updateOp = collection.updateModifier(
   *   BSONDocument("\$set" -> BSONDocument("age" -> 35)))
   *
   * val personBeforeUpdate: Future[Option[Person]] =
   *   collection.findAndModify(BSONDocument("name" -> "Joline"), updateOp).
   *   map(_.result[Person])
   *
   * val removedPerson: Future[Option[Person]] = collection.findAndModify(
   *   BSONDocument("name" -> "Jack"), collection.removeModifier).
   *   map(_.result[Person])
   * }}}
   *
   * @param tparam S $selectorTParam
   *
   * @param selector $selectorParam
   * @param modifier $modifierParam
   * @param sort $sortParam (default: `None`)
   * @param fields $fieldsParam
   * @param bypassDocumentValidation
   * @param writeConcern $writeConcernParam
   * @param maxTime
   * @param collation $collationParam
   * @param arrayFilters
   * @param swriter $swriterParam
   */
  def findAndModify[S](
    selector: S,
    modifier: BatchCommands.FindAndModifyCommand.Modify,
    sort: Option[pack.Document],
    fields: Option[pack.Document],
    bypassDocumentValidation: Boolean,
    writeConcern: WriteConcern,
    maxTime: Option[FiniteDuration],
    collation: Option[Collation],
    arrayFilters: Seq[pack.Document])(implicit swriter: pack.Writer[S], ec: ExecutionContext): Future[BatchCommands.FindAndModifyCommand.FindAndModifyResult] = {

    import FindAndModifyCommand.{
      FindAndModify,
      ImplicitlyDocumentProducer => DP
    }

    implicit val writer =
      pack.writer[ResolvedCollectionCommand[FindAndModify]] {
        FindAndModifyCommand.serialize(version, db.session)
      }

    Future(FindAndModify(
      query = implicitly[DP](selector),
      modify = modifier,
      sort = sort.map(implicitly[DP](_)),
      fields = fields.map(implicitly[DP](_)),
      bypassDocumentValidation = bypassDocumentValidation,
      writeConcern = writeConcern,
      maxTimeMS = maxTime.flatMap { t =>
        val ms = t.toMillis

        if (ms < Int.MaxValue) {
          Some(ms.toInt)
        } else {
          Option.empty[Int]
        }
      },
      collation = collation,
      arrayFilters = arrayFilters.map(implicitly[DP](_)))).
      flatMap(runCommand(_, writePreference))
  }

  /**
   * Finds some matching document, and updates it (using `findAndModify`).
   *
   * {{{
   * val person: Future[BSONDocument] = collection.findAndUpdate(
   *   BSONDocument("name" -> "James"),
   *   BSONDocument("\$set" -> BSONDocument("age" -> 17)),
   *   fetchNewObject = true) // on success, return the update document:
   *                          // { "age": 17 }
   * }}}
   *
   * @tparam selectorTParam
   *
   * @param selector $selectorParam
   * @param update $updateParam
   * @param fetchNewObject the command result must be the new object instead of the old one.
   * @param upsert $upsertParam
   * @param sort $sortParam (default: `None`)
   * @param fields $fieldsParam
   * @param swriter $swriterParam
   * @param writer writerParam
   */
  def findAndUpdate[S, T](selector: S, update: T, fetchNewObject: Boolean = false, upsert: Boolean = false, sort: Option[pack.Document] = None, fields: Option[pack.Document] = None)(implicit swriter: pack.Writer[S], writer: pack.Writer[T], ec: ExecutionContext): Future[BatchCommands.FindAndModifyCommand.FindAndModifyResult] = {
    val updateOp = updateModifier(update, fetchNewObject, upsert)

    findAndModify(selector, updateOp, sort, fields,
      bypassDocumentValidation = false,
      writeConcern = writeConcern,
      maxTime = Option.empty[FiniteDuration],
      collation = Option.empty[Collation],
      arrayFilters = Seq.empty[pack.Document])

  }

  /**
   * Finds some matching document, and removes it (using `findAndModify`).
   *
   * {{{
   * val removed: Future[Person] = collection.findAndRemove(
   *   BSONDocument("name" -> "Foo")).map(_.result[Person])
   * }}}
   *
   * @tparam S $selectorTParam
   *
   * @param selector $selectorParam
   * @param modifier $modifierParam
   * @param sort $sortParam
   * @param fields $fieldsParam
   * @param swriter $swriterParam
   */
  def findAndRemove[S](selector: S, sort: Option[pack.Document] = None, fields: Option[pack.Document] = None)(implicit swriter: pack.Writer[S], ec: ExecutionContext): Future[BatchCommands.FindAndModifyCommand.FindAndModifyResult] = findAndModify[S](selector, removeModifier, sort, fields,
    bypassDocumentValidation = false,
    writeConcern = writeConcern,
    maxTime = Option.empty[FiniteDuration],
    collation = Option.empty[Collation],
    arrayFilters = Seq.empty[pack.Document])

  /**
   * $aggregation.
   *
   * @tparam T $resultTParam
   *
   * @param explain $explainParam of the pipeline
   * @param allowDiskUse $allowDiskUseParam
   * @param bypassDocumentValidation $bypassParam
   * @param readConcern $readConcernParam
   * @param readPreference $readPrefParam (default: primary)
   * @param batchSize $aggBatchSizeParam
   * @param f $aggregationPipelineFunction
   * @param reader $readerParam
   * @param cf $cursorFlattenerParam
   */
  @deprecated("Use [[aggregateWith]]", "0.16.0")
  def aggregateWith1[T](explain: Boolean = false, allowDiskUse: Boolean = false, bypassDocumentValidation: Boolean = false, readConcern: Option[ReadConcern] = None, readPreference: ReadPreference = ReadPreference.primary, batchSize: Option[Int] = None)(f: AggregationFramework => (PipelineOperator, List[PipelineOperator]))(implicit ec: ExecutionContext, reader: pack.Reader[T], cf: CursorFlattener[Cursor], cp: CursorProducer[T]): cp.ProducedCursor = aggregateWith[T](explain, allowDiskUse, bypassDocumentValidation, readConcern, readPreference, batchSize)(f)

  /**
   * $aggregation.
   *
   * @tparam T $resultTParam
   *
   * @param explain $explainParam of the pipeline
   * @param allowDiskUse $allowDiskUseParam
   * @param bypassDocumentValidation $bypassParam
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
    readConcern: Option[ReadConcern] = None,
    readPreference: ReadPreference = ReadPreference.primary,
    batchSize: Option[Int] = None)(
    f: AggregationFramework => (PipelineOperator, List[PipelineOperator]))(
    implicit
    reader: pack.Reader[T],
    cp: CursorProducer[T]): cp.ProducedCursor = {

    val (firstOp, otherOps) = f(BatchCommands.AggregationFramework)

    val aggregateCursor: Cursor.WithOps[T] = aggregatorContext[T](
      firstOp, otherOps, explain, allowDiskUse,
      bypassDocumentValidation, readConcern, readPreference, batchSize).
      prepared[Cursor.WithOps](CursorProducer.defaultCursorProducer[T]).cursor

    cp.produce(aggregateCursor)
  }

  @deprecated("Use aggregator context with optional writeConcern", "0.17.0")
  @inline def aggregatorContext[T](firstOperator: PipelineOperator, otherOperators: List[PipelineOperator], explain: Boolean, allowDiskUse: Boolean, bypassDocumentValidation: Boolean, readConcern: Option[ReadConcern], readPreference: ReadPreference, batchSize: Option[Int])(implicit reader: pack.Reader[T]): AggregatorContext[T] = aggregatorContext[T](firstOperator, otherOperators, explain, allowDiskUse, bypassDocumentValidation, readConcern, readPreference, this.writeConcern, batchSize, CursorOptions.empty)

  /**
   * [[http://docs.mongodb.org/manual/reference/command/aggregate/ Aggregates]] the matching documents.
   *
   * {{{
   * import scala.concurrent.Future
   * import scala.concurrent.ExecutionContext.Implicits.global
   *
   * import reactivemongo.bson._
   * import reactivemongo.api.collections.bson.BSONCollection
   *
   * def populatedStates(cities: BSONCollection): Future[List[BSONDocument]] = {
   *   import cities.BatchCommands.AggregationFramework
   *   import AggregationFramework.{ Group, Match, SumField }
   *
   *   cities.aggregatorContext[BSONDocument](Group(BSONString("\$state"))(
   *     "totalPop" -> SumField("population")), List(
   *       Match(document("totalPop" ->
   *         document("\$gte" -> 10000000L))))).
   *     prepared.cursor.collect[List]()
   * }
   * }}}
   *
   * @tparam T $resultTParam
   *
   * @param firstOperator $firstOpParam
   * @param otherOperators $otherOpsParam
   * @param cursor aggregation cursor option (optional)
   * @param explain $explainParam of the pipeline
   * @param allowDiskUse $allowDiskUseParam
   * @param bypassDocumentValidation $bypassParam
   * @param readConcern $readConcernParam
   * @param readPreference $readPrefParam
   * @param writeConcern $writeConcernParam
   * @param batchSize $aggBatchSizeParam
   * @param cursorOptions the options for the result cursor
   * @param maxTimeMS specifies a time limit in milliseconds for processing operations on a cursor.
   * @param reader $readerParam
   * @param cp $cursorProducerParam
   */
  def aggregatorContext[T](
    firstOperator: PipelineOperator,
    otherOperators: List[PipelineOperator] = Nil,
    explain: Boolean = false,
    allowDiskUse: Boolean = false,
    bypassDocumentValidation: Boolean = false,
    readConcern: Option[ReadConcern] = None,
    readPreference: ReadPreference = ReadPreference.primary,
    writeConcern: WriteConcern = this.writeConcern,
    batchSize: Option[Int] = None,
    cursorOptions: CursorOptions = CursorOptions.empty,
    maxTimeMS: Option[Long] = None)(implicit reader: pack.Reader[T]): AggregatorContext[T] = {
    new AggregatorContext[T](
      firstOperator,
      otherOperators,
      explain,
      allowDiskUse,
      bypassDocumentValidation,
      readConcern.getOrElse(self.readConcern),
      writeConcern,
      readPreference,
      batchSize,
      cursorOptions,
      maxTimeMS,
      reader)
  }

  /**
   * @tparam S $selectorTParam
   * @param writeConcern $writeConcernParam
   * @param swriter $swriterParam
   *
   * @return a future [[reactivemongo.api.commands.WriteResult]] that can be used to check whether the removal was successful
   *
   * {{{
   * // Equivalent to:
   * collection.delete(true, defaultWriteConcern).one(document, limit)
   * }}}
   */
  @deprecated("Use delete().one(selector, limit)", "0.13.1")
  def remove[S](selector: S, writeConcern: WriteConcern = writeConcern, firstMatchOnly: Boolean = false)(implicit swriter: pack.Writer[S], ec: ExecutionContext): Future[WriteResult] = {
    val metadata = db.connectionState.metadata

    if (metadata.maxWireVersion >= MongoWireVersion.V26) {
      val limit = if (firstMatchOnly) Some(1) else Option.empty[Int]
      prepareDelete(true, writeConcern).one(selector, limit)
    } else {
      // Mongo < 2.6
      Future.failed[WriteResult](new scala.RuntimeException(
        s"unsupported MongoDB version: $metadata"))
    }
  }

  /**
   * Prepares an unordered [[https://docs.mongodb.com/manual/reference/command/delete/ delete]] builder.
   *
   * {{{
   * collection.delete.one(document, limit)
   *
   * // Equivalent to .delete(false, defaultWriteConcern)
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
   * collection.delete(true, defaultWriteConcern).one(document, limit)
   * }}}
   */
  def delete(ordered: Boolean = true, writeConcern: WriteConcern = writeConcern): DeleteBuilder = prepareDelete(ordered, writeConcern)

  // --- Internals ---

  /** The read preference for the write operations (primary) */
  @inline protected def writePreference: ReadPreference = ReadPreference.Primary

  /** The default write concern */
  @inline protected def writeConcern = db.connection.options.writeConcern

  /** The default read preference */
  @inline def readPreference: ReadPreference = db.defaultReadPreference
  // TODO: Remove default value from this trait after next release

  /** The default read concern */
  @inline protected def readConcern = db.connection.options.readConcern

  @inline protected def defaultCursorBatchSize: Int = 101

  protected def watchFailure[T](future: => Future[T]): Future[T] =
    Try(future).recover { case NonFatal(e) => Future.failed(e) }.get

  @inline protected def MissingMetadata() =
    ConnectionNotInitialized.MissingMetadata(db.connection.history())

}
