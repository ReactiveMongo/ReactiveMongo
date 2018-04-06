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

import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import scala.collection.generic.CanBuildFrom

import scala.concurrent.{ ExecutionContext, Future }

import shaded.netty.buffer.ByteBuf

import reactivemongo.api._
import reactivemongo.api.commands.{
  UpdateWriteResult,
  WriteConcern,
  WriteResult
}

import reactivemongo.core.protocol.{
  Delete,
  Insert,
  MongoWireVersion,
  RequestMaker,
  Update,
  UpdateFlags
}
import reactivemongo.core.netty.{ BufferSequence, ChannelBufferWritableBuffer }
import reactivemongo.core.errors.{
  ConnectionNotInitialized,
  GenericDriverException
}

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
 */
trait GenericCollection[P <: SerializationPack with Singleton] extends Collection with GenericCollectionWithCommands[P] with CollectionMetaCommands with reactivemongo.api.commands.ImplicitCommandHelpers[P] with InsertOps[P] with UpdateOps[P] with DeleteOps[P] with Aggregator[P] with GenericCollectionMetaCommands[P] { self =>
  import scala.language.higherKinds

  val pack: P
  protected val BatchCommands: BatchCommands[pack.type]

  @inline protected def writePref = ReadPreference.Primary

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

  implicit def PackIdentityReader: pack.Reader[pack.Document] = pack.IdentityReader

  implicit def PackIdentityWriter: pack.Writer[pack.Document] = pack.IdentityWriter

  def failoverStrategy: FailoverStrategy
  def genericQueryBuilder: GenericQueryBuilder[pack.type]

  /** The default read preference */
  def readPreference: ReadPreference = db.defaultReadPreference
  // TODO: Remove default value from this trait after next release

  protected val defaultCursorBatchSize: Int = 101

  /**
   * Returns a new reference to the same collection,
   * with the given read preference.
   */
  def withReadPreference(pref: ReadPreference): GenericCollection[P]

  import BatchCommands._

  protected def watchFailure[T](future: => Future[T]): Future[T] =
    Try(future).recover { case NonFatal(e) => Future.failed(e) }.get

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
  def find[S, J](selector: S, projection: J)(implicit swriter: pack.Writer[S], pwriter: pack.Writer[J]): GenericQueryBuilder[pack.type] = genericQueryBuilder.query(selector).projection(projection)

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
  def count[H](selector: Option[pack.Document] = None, limit: Int = 0, skip: Int = 0, hint: Option[H] = None)(implicit h: H => CountCommand.Hint, ec: ExecutionContext): Future[Int] = runValueCommand(CountCommand.Count(query = selector, limit, skip, hint.map(h)), readPreference)

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
   *
   * {{{
   * val distinctStates = collection.distinct[String, Set]("state")
   * }}}
   */
  def distinct[T, M[_] <: Iterable[_]](key: String, selector: Option[pack.Document] = None, readConcern: ReadConcern = ReadConcern.Local)(implicit reader: pack.NarrowValueReader[T], ec: ExecutionContext, cbf: CanBuildFrom[M[_], T, M[T]]): Future[M[T]] = {
    implicit val widenReader = pack.widenReader(reader)
    val version = db.connection.metadata.
      fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion)

    Future(DistinctCommand.Distinct(
      key, selector, readConcern, version)).flatMap(runCommand(_, readPreference).flatMap {
      _.result[T, M] match {
        case Failure(cause)  => Future.failed[M[T]](cause)
        case Success(result) => Future.successful(result)
      }
    })
  }

  @inline protected def defaultWriteConcern = db.connection.options.writeConcern
  @inline protected def MissingMetadata() =
    ConnectionNotInitialized.MissingMetadata(db.connection.history())

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
   * collection.insert[MyDocType](true, defaultWriteConcern).one(document)
   * }}}
   */
  def insert[T](document: T, writeConcern: WriteConcern = defaultWriteConcern)(implicit writer: pack.Writer[T], ec: ExecutionContext): Future[WriteResult] =
    prepareInsert[T](true, writeConcern).one(document)

  /**
   * Returns a builder for insert operations.
   * Uses the default write concern.
   *
   * @tparam T The type of the document to insert. $implicitWriterT.
   * @param ordered $orderedParam
   *
   * {{{
   * collection.insert[MyDocType](ordered = true).one(singleDoc)
   *
   * collection.insert[MyDocType](ordered = true).one(multiDocs)
   * }}}
   */
  def insert[T: pack.Writer](ordered: Boolean): InsertBuilder[T] =
    prepareInsert[T](ordered, defaultWriteConcern) // TODO: Move Writer requirement to InsertBuilder

  /**
   * Returns a builder for insert operations.
   *
   * @tparam T The type of the document to insert. $implicitWriterT.
   *
   * @param ordered $orderedParam
   * @param writeConcern $writeConcernParam
   *
   * {{{
   * collection.insert[MyDocType](true, aWriteConcern).one(singleDoc)
   *
   * collection.insert[MyDocType](true, aWriteConcern).one(multiDocs)
   * }}}
   */
  def insert[T: pack.Writer](
    ordered: Boolean,
    writeConcern: WriteConcern): InsertBuilder[T] =
    prepareInsert[T](ordered, writeConcern)

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
  def update[S, T](selector: S, update: T, writeConcern: WriteConcern = defaultWriteConcern, upsert: Boolean = false, multi: Boolean = false)(implicit swriter: pack.Writer[S], writer: pack.Writer[T], ec: ExecutionContext): Future[UpdateWriteResult] = prepareUpdate(ordered = true, writeConcern = writeConcern).
    one(selector, update, upsert, multi)

  /**
   * Returns an update builder.
   *
   * {{{
   * // Equivalent to collection.update(query, update, ...)
   * collection.update(ordered = true).
   *   one(query, update, upsert = false, multi = false)
   * }}}
   */
  def update(ordered: Boolean): UpdateBuilder =
    prepareUpdate(ordered, defaultWriteConcern)

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
   * Returns an update modifier, to be used with [[findAndModify]].
   *
   * @param update $updateParam
   * @param fetchNewObject the command result must be the new object instead of the old one.
   * @param upsert $upsertParam
   */
  def updateModifier[U](update: U, fetchNewObject: Boolean = false, upsert: Boolean = false)(implicit updateWriter: pack.Writer[U]): BatchCommands.FindAndModifyCommand.Update = BatchCommands.FindAndModifyCommand.Update(update, fetchNewObject, upsert)

  /** Returns a removal modifier, to be used with [[findAndModify]]. */
  @transient lazy val removeModifier =
    BatchCommands.FindAndModifyCommand.Remove

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
   * @param swriter $swriterParam
   */
  def findAndModify[S](selector: S, modifier: BatchCommands.FindAndModifyCommand.Modify, sort: Option[pack.Document] = None, fields: Option[pack.Document] = None)(implicit swriter: pack.Writer[S], ec: ExecutionContext): Future[BatchCommands.FindAndModifyCommand.FindAndModifyResult] = {
    import FindAndModifyCommand.{ ImplicitlyDocumentProducer => DP }

    Future(BatchCommands.FindAndModifyCommand.FindAndModify(
      query = selector,
      modify = modifier,
      sort = sort.map(implicitly[DP](_)),
      fields = fields.map(implicitly[DP](_)))).flatMap(runCommand(_, writePref))
  }

  /**
   * Finds some matching document, and updates it (using [[findAndModify]]).
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
    findAndModify(selector, updateOp, sort, fields)
  }

  /**
   * Finds some matching document, and removes it (using [[findAndModify]]).
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
  def findAndRemove[S](selector: S, sort: Option[pack.Document] = None, fields: Option[pack.Document] = None)(implicit swriter: pack.Writer[S], ec: ExecutionContext): Future[BatchCommands.FindAndModifyCommand.FindAndModifyResult] = findAndModify[S](selector, removeModifier, sort, fields)

  /**
   * $aggregation.
   *
   * @param firstOperator $firstOpParam
   * @param otherOperators $otherOpsParam
   * @param explain $explainParam of the pipeline
   * @param allowDiskUse $allowDiskUseParam
   * @param bypassDocumentValidation $bypassParam
   * @param readConcern $readConcernParam
   */
  @deprecated("Use [[aggregateWith1]]", "0.12.7")
  def aggregate(firstOperator: PipelineOperator, otherOperators: List[PipelineOperator] = Nil, explain: Boolean = false, allowDiskUse: Boolean = false, bypassDocumentValidation: Boolean = false, readConcern: Option[ReadConcern] = None)(implicit ec: ExecutionContext): Future[BatchCommands.AggregationFramework.AggregationResult] = {
    import BatchCommands.AggregationFramework.Aggregate
    import BatchCommands.{ AggregateWriter, AggregateReader }

    def ver = db.connection.metadata.
      fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion)

    runWithResponse(Aggregate(
      firstOperator :: otherOperators, explain, allowDiskUse, None,
      ver, bypassDocumentValidation, readConcern), readPreference).map(_.value)
  }

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
  @deprecated("Use [[aggregateWith1]]", "0.12.3")
  def aggregateWith[T](explain: Boolean = false, allowDiskUse: Boolean = false, bypassDocumentValidation: Boolean = false, readConcern: Option[ReadConcern] = None, readPreference: ReadPreference = ReadPreference.primary, batchSize: Option[Int] = None)(f: AggregationFramework => (PipelineOperator, List[PipelineOperator]))(implicit ec: ExecutionContext, reader: pack.Reader[T], cf: CursorFlattener[Cursor]): Cursor[T] = {
    import CursorProducer.defaultCursorProducer
    aggregateWith1(explain, allowDiskUse, bypassDocumentValidation, readConcern, readPreference, batchSize)(f)
  }

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
  def aggregateWith1[T](explain: Boolean = false, allowDiskUse: Boolean = false, bypassDocumentValidation: Boolean = false, readConcern: Option[ReadConcern] = None, readPreference: ReadPreference = ReadPreference.primary, batchSize: Option[Int] = None)(f: AggregationFramework => (PipelineOperator, List[PipelineOperator]))(implicit ec: ExecutionContext, reader: pack.Reader[T], cf: CursorFlattener[Cursor], cp: CursorProducer[T]): cp.ProducedCursor = {
    val (firstOp, otherOps) = f(BatchCommands.AggregationFramework)
    val aggCursor = BatchCommands.AggregationFramework.
      Cursor(batchSize.getOrElse(defaultCursorBatchSize))

    val aggregateCursor: Cursor[T] = aggregate[T](firstOp, otherOps,
      Some(aggCursor), explain, allowDiskUse, bypassDocumentValidation,
      readConcern, readPreference)

    cp.produce(aggregateCursor)
  }

  /**
   * $aggregation.
   *
   * @tparam T $resultTParam
   *
   * @param firstOperator $firstOpParam
   * @param otherOperators $otherOpsParam
   * @param explain $explainParam of the pipeline
   * @param allowDiskUse $allowDiskUseParam
   * @param bypassDocumentValidation $bypassParam
   * @param readConcern $readConcernParam
   * @param readPreference $readPrefParam
   * @param batchSize $aggBatchSizeParam
   * @param reader $readerParam
   * @param cf $cursorFlattenerParam
   */
  @deprecated("Use [[aggregatorContext]]", "0.12.3")
  def aggregate1[T](firstOperator: PipelineOperator, otherOperators: List[PipelineOperator], explain: Boolean = false, allowDiskUse: Boolean = false, bypassDocumentValidation: Boolean = false, readConcern: Option[ReadConcern] = None, readPreference: ReadPreference = ReadPreference.primary, batchSize: Option[Int] = None)(implicit ec: ExecutionContext, reader: pack.Reader[T], cf: CursorFlattener[Cursor]): Cursor[T] = aggregatorContext[T](
    firstOperator, otherOperators,
    explain, allowDiskUse, bypassDocumentValidation,
    readConcern, readPreference, batchSize).
    prepared[Cursor](CursorProducer.defaultCursorProducer[T]).cursor

  /**
   * [[http://docs.mongodb.org/manual/reference/command/aggregate/ Aggregates]] the matching documents.
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
   * @param reader $readerParam
   * @param cf $cursorFlattenerParam
   */
  @deprecated("Use [[aggregatorContext]]", "0.12.3")
  def aggregate[T](firstOperator: PipelineOperator, otherOperators: List[PipelineOperator], cursor: Option[BatchCommands.AggregationFramework.Cursor], explain: Boolean, allowDiskUse: Boolean, bypassDocumentValidation: Boolean, readConcern: Option[ReadConcern], readPreference: ReadPreference)(implicit ec: ExecutionContext, reader: pack.Reader[T], cf: CursorFlattener[Cursor]): Cursor[T] = aggregate1[T](firstOperator, otherOperators, explain, allowDiskUse, bypassDocumentValidation, readConcern, readPreference, cursor.map(_.batchSize))

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
   * @param reader $readerParam
   * @param cp $cursorProducerParam
   */
  def aggregatorContext[T](firstOperator: PipelineOperator, otherOperators: List[PipelineOperator] = Nil, explain: Boolean = false, allowDiskUse: Boolean = false, bypassDocumentValidation: Boolean = false, readConcern: Option[ReadConcern] = None, readPreference: ReadPreference = ReadPreference.primary, batchSize: Option[Int] = None)(implicit reader: pack.Reader[T]): AggregatorContext[T] = new AggregatorContext[T](firstOperator, otherOperators, explain, allowDiskUse, bypassDocumentValidation, readConcern, readPreference, batchSize, reader)

  /**
   * @tparam S $selectorTParam
   * @param writeConcern $writeConcernParam
   * @param swriter $swriterParam
   *
   * @return a future [[reactivemongo.api.commands.WriteResult]] that can be used to check whether the removal was successful
   *
   * {{{
   * // Equivalent to:
   * collection.delete[MyDocType](true, defaultWriteConcern).one(document)
   * }}}
   */
  @deprecated("Use [[delete]]", "0.13.1")
  def remove[S](selector: S, writeConcern: WriteConcern = defaultWriteConcern, firstMatchOnly: Boolean = false)(implicit swriter: pack.Writer[S], ec: ExecutionContext): Future[WriteResult] = db.connection.metadata match {
    case Some(metadata) if (
      metadata.maxWireVersion >= MongoWireVersion.V26) => {
      val limit = if (firstMatchOnly) Some(1) else Option.empty[Int]
      delete(true, writeConcern).one(selector, limit)
    }

    case Some(metadata) => // Mongo < 2.6
      Future.failed[WriteResult](new scala.RuntimeException(
        s"unsupported MongoDB version: $metadata"))

    case _ =>
      Future.failed(MissingMetadata())
  }

  private def writeDoc[T](
    doc: T,
    writer: pack.Writer[T],
    buffer: ChannelBufferWritableBuffer = ChannelBufferWritableBuffer()) = {
    pack.serializeAndWrite(buffer, doc, writer)
    buffer
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/command/delete/ Deletes]] the matching document(s).
   *
   * @param ordered $orderedParam
   * @param writeConcern $writeConcernParam
   *
   */
  def delete[S](ordered: Boolean, writeConcern: WriteConcern): DeleteBuilder =
    prepareDelete(ordered, writeConcern)

  /**
   * Remove the matched document(s) from the collection without writeConcern.
   *
   * Please note that you cannot be sure that the matched documents have been effectively removed and when (hence the Unit return type).
   *
   * @tparam T the type of the selector of documents to remove. An implicit `Writer[T]` typeclass for handling it has to be in the scope.
   *
   * @param query the selector of documents to remove.
   * @param firstMatchOnly states whether only the first matched documents has to be removed from this collection.
   */
  @deprecated("Use [[remove]]", "0.12.0")
  def uncheckedRemove[T](query: T, firstMatchOnly: Boolean = false)(implicit writer: pack.Writer[T], ec: ExecutionContext): Unit = {
    val op = Delete(fullCollectionName, if (firstMatchOnly) 1 else 0)
    val bson = writeDoc(query, writer)
    val buf = bson.buffer
    val message = RequestMaker(op, BufferSequence(buf))

    db.connection.send(message)
  }

  /**
   * Updates one or more documents matching the given selector with the given modifier or update object.
   *
   * Please note that you cannot be sure that the matched documents have been effectively updated and when (hence the Unit return type).
   *
   * @tparam S the type of the selector object. An implicit `Writer[S]` typeclass for handling it has to be in the scope.
   * @tparam U the type of the modifier or update object. An implicit `Writer[U]` typeclass for handling it has to be in the scope.
   *
   * @param selector the selector object, for finding the documents to update.
   * @param update the modifier object (with special keys like \$set) or replacement object.
   * @param upsert states whether the update object should be inserted if no match found. Defaults to false.
   * @param multi states whether the update may be done on all the matching documents.
   */
  @deprecated("Use `update(Boolean)`", "0.12.0")
  def uncheckedUpdate[S, U](selector: S, update: U, upsert: Boolean = false, multi: Boolean = false)(implicit selectorWriter: pack.Writer[S], updateWriter: pack.Writer[U]): Unit = {
    val flags = 0 | (if (upsert) UpdateFlags.Upsert else 0) | (if (multi) UpdateFlags.MultiUpdate else 0)
    val op = Update(fullCollectionName, flags)
    val bson = {
      val b = writeDoc(selector, selectorWriter)
      writeDoc(update, updateWriter, b)
    }
    val buf = bson.buffer

    db.connection.send(RequestMaker(op, BufferSequence(buf)))
  }

  /**
   * Inserts a document into the collection without writeConcern.
   *
   * Please note that you cannot be sure that the document has been effectively written and when (hence the Unit return type).
   *
   * @tparam T the type of the document to insert. An implicit `Writer[T]` typeclass for handling it has to be in the scope.
   *
   * @param document the document to insert.
   */
  @deprecated("Use `insert[T](Boolean)`", "0.12.0")
  def uncheckedInsert[T](document: T)(implicit writer: pack.Writer[T]): Unit = {
    val op = Insert(0, fullCollectionName)
    val bson = writeDoc(document, writer)
    val buf = bson.buffer
    val message = RequestMaker(op, BufferSequence(buf))

    db.connection.send(message)
  }

  // ---

  import reactivemongo.core.nodeset.ProtocolMetadata
  import reactivemongo.api.commands.MultiBulkWriteResult

  /**
   * @param ordered $orderedParam
   */
  @deprecated("Use `insert[T](ordered).many(documents)`", "0.12.7")
  def bulkInsert(ordered: Boolean)(documents: ImplicitlyDocumentProducer*)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    db.connection.metadata.map { metadata =>
      bulkInsert(documents.toStream.map(_.produce), ordered, defaultWriteConcern, metadata.maxBulkSize, metadata.maxBsonSize)
    }.getOrElse(Future.failed(MissingMetadata()))

  /**
   * @param ordered $orderedParam
   */
  @deprecated("Use `insert[T](ordered, writeConcern).many(documents)`", "0.12.7")
  def bulkInsert(ordered: Boolean, writeConcern: WriteConcern)(documents: ImplicitlyDocumentProducer*)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    db.connection.metadata.map { metadata =>
      bulkInsert(documents.toStream.map(_.produce), ordered, writeConcern, metadata.maxBulkSize, metadata.maxBsonSize)
    }.getOrElse(Future.failed(MissingMetadata()))

  /**
   * @param ordered $orderedParam
   */
  @deprecated("Use `insert[T](ordered, writeConcern).many(documents)`", "0.12.7")
  def bulkInsert(ordered: Boolean, writeConcern: WriteConcern, bulkSize: Int, bulkByteSize: Int)(documents: ImplicitlyDocumentProducer*)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    bulkInsert(documents.toStream.map(_.produce), ordered, writeConcern, bulkSize, bulkByteSize)

  /**
   * @param ordered $orderedParam
   */
  @deprecated("Use `insert[T](ordered).many(documents)`", "0.12.7")
  def bulkInsert(documents: Stream[pack.Document], ordered: Boolean)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    bulkInsert(documents, ordered, defaultWriteConcern)

  /**
   * @param ordered $orderedParam
   */
  @deprecated("Use `insert[T](ordered).many(documents, writeConcern)`", "0.12.7")
  def bulkInsert(documents: Stream[pack.Document], ordered: Boolean, writeConcern: WriteConcern)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    db.connection.metadata.map { metadata =>
      bulkInsert(documents, ordered, writeConcern,
        metadata.maxBulkSize, metadata.maxBsonSize)

    }.getOrElse(Future.failed(MissingMetadata()))

  /**
   * @param ordered $orderedParam
   */
  @deprecated("Use `insert[T](ordered).many(documents, writeConcern)`", "0.12.7")
  def bulkInsert(documents: Stream[pack.Document], ordered: Boolean, writeConcern: WriteConcern = defaultWriteConcern, @deprecated("Unused", "0.12.7") bulkSize: Int, @deprecated("Unused", "0.12.7") bulkByteSize: Int)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] = prepareInsert[pack.Document](ordered, writeConcern).many(documents)

  @deprecated("Unused", "0.12.7")
  protected sealed trait BulkMaker[R, S <: BulkMaker[R, S]] {
    def fill(docs: Stream[pack.Document]): (Stream[pack.Document], Option[S]) = {
      @annotation.tailrec
      def loop(docs: Stream[pack.Document]): (Stream[pack.Document], Option[S]) = {
        if (docs.isEmpty) Stream.empty -> None
        else {
          val res = putOrIssueNewCommand(docs.head)
          if (res.isDefined) docs.tail -> res
          else loop(docs.tail)
        }
      }
      loop(docs)
    }

    def putOrIssueNewCommand(doc: pack.Document): Option[S]
    def result(): ByteBuf
    def send()(implicit ec: ExecutionContext): Future[R]
  }

  @deprecated("Unused", "0.12.7")
  protected object Mongo26WriteCommand {
    def insert(
      ordered: Boolean,
      writeConcern: WriteConcern,
      metadata: ProtocolMetadata): Mongo26WriteCommand =
      new Mongo26WriteCommand("insert", ordered, writeConcern, metadata)
  }

  @deprecated("Unused", "0.12.7")
  protected class Mongo26WriteCommand private (
    tpe: String,
    ordered: Boolean,
    writeConcern: WriteConcern,
    metadata: ProtocolMetadata) extends BulkMaker[WriteResult, Mongo26WriteCommand] {
    import reactivemongo.core.protocol.Query
    import reactivemongo.bson.lowlevel.LowLevelBsonDocWriter

    private var done = false
    private var docsN = 0
    private val buf = ChannelBufferWritableBuffer()
    private val writer = new LowLevelBsonDocWriter(buf)

    val thresholdDocs = metadata.maxBulkSize
    // minus 2 for the trailing '\0'
    val thresholdBytes = metadata.maxBsonSize - 2

    init()

    def putOrIssueNewCommand(doc: pack.Document): Option[Mongo26WriteCommand] = {
      if (done) {
        throw new scala.RuntimeException("violated assertion: Mongo26WriteCommand should not be used again after it is done")
      }

      if (docsN >= thresholdDocs) {
        closeIfNecessary()

        val nextCommand = new Mongo26WriteCommand(
          tpe, ordered, writeConcern, metadata)

        nextCommand.putOrIssueNewCommand(doc)
        Some(nextCommand)
      } else {
        val start = buf.index
        buf.writeByte(0x03)
        buf.writeCString(docsN.toString)

        val start2 = buf.index
        pack.writeToBuffer(buf, doc)

        val res = if (buf.index > thresholdBytes && docsN == 0) {
          // first and already out of bound
          throw new scala.RuntimeException(s"Mongo26WriteCommand could not accept doc of size = ${buf.index - start} bytes")
        } else if (buf.index > thresholdBytes) {
          val nextCommand = new Mongo26WriteCommand(
            tpe, ordered, writeConcern, metadata)

          nextCommand.buf.writeByte(0x03)
          nextCommand.buf.writeCString("0")
          nextCommand.buf.buffer.writeBytes(
            buf.buffer, start2, buf.index - start2)

          nextCommand.docsN = 1
          buf.buffer.readerIndex(0)
          buf.buffer.writerIndex(start)

          closeIfNecessary()

          Some(nextCommand)
        } else None

        docsN += 1

        res
      }
    }

    def result(): ByteBuf = {
      closeIfNecessary()
      buf.buffer
    }

    def send()(implicit ec: ExecutionContext): Future[WriteResult] = {
      val bson = result()
      val documents = BufferSequence(bson)
      val op = Query(0, db.name + ".$cmd", 0, 1)

      val cursor = DefaultCursor.query(pack, op, _ => documents, ReadPreference.primary, db.connection, failoverStrategy, true, fullCollectionName)(BatchCommands.DefaultWriteResultReader) //(Mongo26WriteCommand.DefaultWriteResultBufferReader)

      cursor.headOption.flatMap {
        case Some(wr) if (wr.inError || (wr.hasErrors && ordered)) => {
          Future.failed(WriteResult.lastError(wr).
            getOrElse[Exception](GenericDriverException(
              s"write failure: $wr")))
        }

        case Some(wr) => Future.successful(wr)

        case c => Future.failed(
          new GenericDriverException(s"no write result ? $c"))
      }
    }

    private def closeIfNecessary(): Unit = {
      if (!done) {
        done = true
        writer.close // array
        writer.close // doc
      }

      ()
    }

    private def init(): Unit = {
      writer.putString(tpe, name).putBoolean("ordered", ordered)
      putWriteConcern()
      writer.openArray("documents")
      ()
    }

    private def putWriteConcern(): Unit = {
      import reactivemongo.api.commands.GetLastError

      writer.openDocument("writeConcern")
      writeConcern.w match {
        case GetLastError.Majority =>
          writer.putString("w", "majority")

        case GetLastError.TagSet(tagSet) =>
          writer.putString("w", tagSet)

        case GetLastError.WaitForAcknowledgments(n) =>
          writer.putInt("w", n)

        case GetLastError.WaitForAknowledgments(n) =>
          writer.putInt("w", n)
      }

      if (writeConcern.j) writer.putBoolean("j", true)

      writeConcern.wtimeout foreach { writer.putInt("wtimeout", _) }

      writer.close
      ()
    }
  }
}
