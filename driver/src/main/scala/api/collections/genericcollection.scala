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

import shaded.netty.buffer.ChannelBuffer

import reactivemongo.api._
import reactivemongo.api.commands.{
  CursorFetcher,
  ResponseResult,
  WriteConcern
}
import reactivemongo.core.nodeset.ProtocolMetadata
import reactivemongo.core.protocol.{
  Delete,
  Query,
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

trait GenericCollectionWithCommands[P <: SerializationPack with Singleton] { self: GenericCollection[P] =>
  val pack: P

  import reactivemongo.api.commands.{
    BoxedAnyVal,
    CollectionCommand,
    Command,
    CommandWithResult,
    ResponseResult,
    ResolvedCollectionCommand
  }

  def runner = Command.run(pack, self.failoverStrategy)

  def runCommand[R, C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R], readPreference: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = runner(self, command, readPreference)

  @deprecated("Use the alternative with `ReadPreference`", "0.12-RC5")
  def runCommand[R, C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R])(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = runCommand[R, C](command, ReadPreference.primary)

  def runWithResponse[R, C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R], readPreference: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[ResponseResult[R]] = runner.withResponse(self, command, readPreference)

  @deprecated("Use the alternative with `ReadPreference`", "0.12-RC5")
  def runWithResponse[R, C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R])(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[ResponseResult[R]] = runWithResponse[R, C](command, ReadPreference.primary)

  def runCommand[C <: CollectionCommand](command: C)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): CursorFetcher[pack.type, Cursor] = runner(self, command)

  @deprecated("Use the alternative with `ReadPreference`", "0.12-RC5")
  def runValueCommand[A <: AnyVal, R <: BoxedAnyVal[A], C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R with BoxedAnyVal[A]])(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[A] = runner.unboxed(self, command, ReadPreference.primary)

  def runValueCommand[A <: AnyVal, R <: BoxedAnyVal[A], C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R with BoxedAnyVal[A]], rp: ReadPreference)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[A] = runner.unboxed(self, command, rp)
}

trait BatchCommands[P <: SerializationPack] {
  import reactivemongo.api.commands.{
    AggregationFramework => AC,
    CountCommand => CC,
    DistinctCommand => DistC,
    InsertCommand => IC,
    UpdateCommand => UC,
    DeleteCommand => DC,
    DefaultWriteResult,
    LastError,
    ResolvedCollectionCommand,
    FindAndModifyCommand => FMC
  }

  val pack: P

  val CountCommand: CC[pack.type]
  implicit def CountWriter: pack.Writer[ResolvedCollectionCommand[CountCommand.Count]]
  implicit def CountResultReader: pack.Reader[CountCommand.CountResult]

  val DistinctCommand: DistC[pack.type]
  implicit def DistinctWriter: pack.Writer[ResolvedCollectionCommand[DistinctCommand.Distinct]]
  implicit def DistinctResultReader: pack.Reader[DistinctCommand.DistinctResult]

  val InsertCommand: IC[pack.type]
  implicit def InsertWriter: pack.Writer[ResolvedCollectionCommand[InsertCommand.Insert]]

  val UpdateCommand: UC[pack.type]
  implicit def UpdateWriter: pack.Writer[ResolvedCollectionCommand[UpdateCommand.Update]]
  implicit def UpdateReader: pack.Reader[UpdateCommand.UpdateResult]

  val DeleteCommand: DC[pack.type]
  implicit def DeleteWriter: pack.Writer[ResolvedCollectionCommand[DeleteCommand.Delete]]

  val FindAndModifyCommand: FMC[pack.type]
  implicit def FindAndModifyWriter: pack.Writer[ResolvedCollectionCommand[FindAndModifyCommand.FindAndModify]]
  implicit def FindAndModifyReader: pack.Reader[FindAndModifyCommand.FindAndModifyResult]

  val AggregationFramework: AC[pack.type]
  implicit def AggregateWriter: pack.Writer[ResolvedCollectionCommand[AggregationFramework.Aggregate]]
  implicit def AggregateReader: pack.Reader[AggregationFramework.AggregationResult]

  implicit def DefaultWriteResultReader: pack.Reader[DefaultWriteResult]

  implicit def LastErrorReader: pack.Reader[LastError]
}

/**
 * A Collection that provides default methods using a `SerializationPack`
 * (e.g. the default [[reactivemongo.api.BSONSerializationPack]]).
 *
 * Some methods of this collection accept instances of `Reader[T]` and `Writer[T]`, that transform any `T` instance into a document, compatible with the selected serialization pack, and vice-versa.
 *
 * @tparam P the serialization pack
 */
trait GenericCollection[P <: SerializationPack with Singleton] extends Collection with GenericCollectionWithCommands[P] with CollectionMetaCommands with reactivemongo.api.commands.ImplicitCommandHelpers[P] { self =>
  val pack: P
  protected val BatchCommands: BatchCommands[pack.type]

  /** Alias for [[BatchCommands.AggregationFramework.PipelineOperator]] */
  type PipelineOperator = BatchCommands.AggregationFramework.PipelineOperator

  implicit def PackIdentityReader: pack.Reader[pack.Document] = pack.IdentityReader

  implicit def PackIdentityWriter: pack.Writer[pack.Document] = pack.IdentityWriter

  def failoverStrategy: FailoverStrategy
  def genericQueryBuilder: GenericQueryBuilder[pack.type]

  def readPreference: ReadPreference = db.defaultReadPreference

  /**
   * Returns a new reference to the same collection,
   * with the given read preference.
   */
  def withReadPreference(pref: ReadPreference): GenericCollection[P]

  import BatchCommands._
  import reactivemongo.api.commands.{
    MultiBulkWriteResult,
    UpdateWriteResult,
    WriteResult
  }

  private def writeDoc[T](doc: T, writer: pack.Writer[T]) = {
    val buffer = ChannelBufferWritableBuffer()
    pack.serializeAndWrite(buffer, doc, writer)
    buffer.buffer
  }

  protected def watchFailure[T](future: => Future[T]): Future[T] =
    Try(future).recover { case NonFatal(e) => Future.failed(e) }.get

  /**
   * Find the documents matching the given criteria.
   *
   * This method accepts any query and projection object, provided that there is an implicit `Writer[S]` typeclass for handling them in the scope.
   *
   * Please take a look at the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam S the type of the selector (the query). An implicit `Writer[S]` typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   *
   * @return a [[GenericQueryBuilder]] that you can use to to customize the query. You can obtain a cursor by calling the method [[reactivemongo.api.Cursor]] on this query builder.
   */
  def find[S](selector: S)(implicit swriter: pack.Writer[S]): GenericQueryBuilder[pack.type] = genericQueryBuilder.query(selector)

  /**
   * Find the documents matching the given criteria.
   *
   * This method accepts any selector and projection object, provided that there is an implicit `Writer[S]` typeclass for handling them in the scope.
   *
   * Please take a look at the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam S the type of the selector (the query). An implicit `Writer[S]` typeclass for handling it has to be in the scope.
   * @tparam P the type of the projection object. An implicit `Writer[P]` typeclass for handling it has to be in the scope.
   *
   * @param selector the query selector.
   * @param projection Get only a subset of each matched documents. Defaults to None.
   *
   * @return a [[GenericQueryBuilder]] that you can use to to customize the query. You can obtain a cursor by calling the method [[reactivemongo.api.Cursor]] on this query builder.
   */
  def find[S, P](selector: S, projection: P)(implicit swriter: pack.Writer[S], pwriter: pack.Writer[P]): GenericQueryBuilder[pack.type] = genericQueryBuilder.query(selector).projection(projection)

  /**
   * Count the documents matching the given criteria.
   *
   * This method accepts any query or hint, the scope provides instances of appropriate typeclasses.
   *
   * Please take a look at the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam H the type of hint. An implicit `H => Hint` conversion has to be in the scope.
   *
   * @param selector the query selector
   * @param limit the maximum number of matching documents to count
   * @param skip the number of matching documents to skip before counting
   * @param hint the index to use (either the index name or the index document)
   */
  def count[H](selector: Option[pack.Document] = None, limit: Int = 0, skip: Int = 0, hint: Option[H] = None)(implicit h: H => CountCommand.Hint, ec: ExecutionContext): Future[Int] = runValueCommand(CountCommand.Count(query = selector, limit, skip, hint.map(h)), readPreference)

  /**
   * Returns the distinct values for a specified field across a single collection.
   *
   * @tparam T the element type of the distinct values
   * @tparam M the container, that must be a [[scala.collection.Iterable]]
   * @param key the field for which to return distinct values
   * @param selector the query selector that specifies the documents from which to retrieve the distinct values.
   * @param readConcern the read concern
   *
   * {{{
   * val distinctStates = collection.distinct[String, Set]("state")
   * }}}
   */
  def distinct[T, M[_] <: Iterable[_]](key: String, selector: Option[pack.Document] = None, readConcern: ReadConcern = ReadConcern.Local)(implicit reader: pack.NarrowValueReader[T], ec: ExecutionContext, cbf: CanBuildFrom[M[_], T, M[T]]): Future[M[T]] = {
    implicit val widenReader = pack.widenReader(reader)
    val version = db.connection.metadata.
      fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion)

    runCommand(DistinctCommand.Distinct(
      key, selector, readConcern, version
    ), readPreference).flatMap {
      _.result[T, M] match {
        case Failure(cause)  => Future.failed[M[T]](cause)
        case Success(result) => Future.successful(result)
      }
    }
  }

  @inline private def defaultWriteConcern = db.connection.options.writeConcern
  @inline private def MissingMetadata() =
    ConnectionNotInitialized.MissingMetadata(db.connection.history())

  def bulkInsert(ordered: Boolean)(documents: ImplicitlyDocumentProducer*)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    db.connection.metadata.map { metadata =>
      bulkInsert(documents.toStream.map(_.produce), ordered, defaultWriteConcern, metadata.maxBulkSize, metadata.maxBsonSize)
    }.getOrElse(Future.failed(MissingMetadata()))

  def bulkInsert(ordered: Boolean, writeConcern: WriteConcern)(documents: ImplicitlyDocumentProducer*)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    db.connection.metadata.map { metadata =>
      bulkInsert(documents.toStream.map(_.produce), ordered, writeConcern, metadata.maxBulkSize, metadata.maxBsonSize)
    }.getOrElse(Future.failed(MissingMetadata()))

  def bulkInsert(ordered: Boolean, writeConcern: WriteConcern, bulkSize: Int, bulkByteSize: Int)(documents: ImplicitlyDocumentProducer*)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    bulkInsert(documents.toStream.map(_.produce), ordered, writeConcern, bulkSize, bulkByteSize)

  def bulkInsert(documents: Stream[pack.Document], ordered: Boolean)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    bulkInsert(documents, ordered, defaultWriteConcern)

  def bulkInsert(documents: Stream[pack.Document], ordered: Boolean, writeConcern: WriteConcern)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    db.connection.metadata.map { metadata =>
      bulkInsert(documents, ordered, writeConcern, metadata.maxBulkSize, metadata.maxBsonSize)
    }.getOrElse(Future.failed(MissingMetadata()))

  def bulkInsert(documents: Stream[pack.Document], ordered: Boolean, writeConcern: WriteConcern = defaultWriteConcern, bulkSize: Int, bulkByteSize: Int)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] = watchFailure {
    def createBulk[R, A <: BulkMaker[R, A]](docs: Stream[pack.Document], command: A with BulkMaker[R, A]): Future[List[R]] = {
      val (tail, nc) = command.fill(docs)

      command.send().flatMap { wr =>
        if (nc.isDefined) createBulk(tail, nc.get).map(wr2 => wr :: wr2)
        else Future.successful(List(wr)) // done
      }
    }

    if (!documents.isEmpty) {
      val havingMetadata = db.connection.metadata.
        fold(Future.failed[ProtocolMetadata](MissingMetadata()))(Future.successful)

      havingMetadata.flatMap { metadata =>
        if (metadata.maxWireVersion >= MongoWireVersion.V26) {
          createBulk(documents, Mongo26WriteCommand.insert(ordered, writeConcern, metadata)).map { _.foldLeft(MultiBulkWriteResult())(_ merge _) }
        } else {
          // Mongo 2.4
          Future.failed[MultiBulkWriteResult](new scala.RuntimeException(
            s"unsupported MongoDB version: $metadata"
          ))
        }
      }
    } else Future.successful(MultiBulkWriteResult(
      ok = true,
      n = 0,
      nModified = 0,
      upserted = Seq.empty,
      writeErrors = Seq.empty,
      writeConcernError = None,
      code = None,
      errmsg = None,
      totalN = 0
    ))
  }

  /**
   * Inserts a document into the collection and wait for the [[reactivemongo.api.commands.WriteResult]].
   *
   * Please read the documentation about [[reactivemongo.core.commands.GetLastError]] to know how to use it properly.
   *
   * @tparam T the type of the document to insert. An implicit `Writer[T]` typeclass for handling it has to be in the scope.
   *
   * @param document the document to insert.
   * @param writeConcern the [[reactivemongo.core.commands.GetLastError]] command message to send in order to control how the document is inserted. Defaults to GetLastError().
   *
   * @return a future [[reactivemongo.api.commands.WriteResult]] that can be used to check whether the insertion was successful.
   */
  def insert[T](document: T, writeConcern: WriteConcern = defaultWriteConcern)(implicit writer: pack.Writer[T], ec: ExecutionContext): Future[WriteResult] =
    db.connection.metadata match {
      case Some(metadata) if metadata.maxWireVersion >= MongoWireVersion.V26 =>
        Failover2(db.connection, failoverStrategy) { () =>
          runCommand(BatchCommands.InsertCommand.Insert(
            writeConcern = writeConcern
          )(document), readPreference).flatMap { wr =>
            val flattened = wr.flatten
            if (!flattened.ok) {
              // was ordered, with one doc => fail if has an error
              Future.failed(WriteResult.lastError(flattened).
                getOrElse[Exception](GenericDriverException(
                  s"fails to insert: $document"
                )))

            } else Future.successful(wr)
          }
        }.future

      case Some(metadata) => // Mongo < 2.6
        Future.failed[WriteResult](new scala.RuntimeException(
          s"unsupported MongoDB version: $metadata"
        ))

      case _ =>
        Future.failed(MissingMetadata())
    }

  /**
   * Updates one or more documents matching the given selector with the given modifier or update object.
   *
   * @tparam S the type of the selector object. An implicit `Writer[S]` typeclass for handling it has to be in the scope.
   * @tparam U the type of the modifier or update object. An implicit `Writer[U]` typeclass for handling it has to be in the scope.
   *
   * @param selector the selector object, for finding the documents to update.
   * @param update the modifier object (with special keys like \$set) or replacement object.
   * @param writeConcern the [[reactivemongo.core.commands.GetLastError]] command message to send in order to control how the documents are updated. Defaults to GetLastError().
   * @param upsert states whether the update objet should be inserted if no match found. Defaults to false.
   * @param multi states whether the update may be done on all the matching documents.
   *
   * @return a future [[reactivemongo.api.commands.WriteResult]] that can be used to check whether the update was successful.
   */
  def update[S, U](selector: S, update: U, writeConcern: WriteConcern = defaultWriteConcern, upsert: Boolean = false, multi: Boolean = false)(implicit selectorWriter: pack.Writer[S], updateWriter: pack.Writer[U], ec: ExecutionContext): Future[UpdateWriteResult] = db.connection.metadata match {
    case Some(metadata) if (
      metadata.maxWireVersion >= MongoWireVersion.V26
    ) => {
      import BatchCommands.UpdateCommand.{ Update, UpdateElement }

      Failover2(db.connection, failoverStrategy) { () =>
        runCommand(Update(writeConcern = writeConcern)(
          UpdateElement(selector, update, upsert, multi)
        ), readPreference).flatMap { wr =>
          val flattened = wr.flatten
          if (!flattened.ok) {
            // was ordered, with one doc => fail if has an error
            Future.failed(WriteResult.lastError(flattened).
              getOrElse[Exception](GenericDriverException(
                s"fails to update: $update"
              )))

          } else Future.successful(wr)
        }
      }.future
    }

    case Some(metadata) => // Mongo < 2.6
      Future.failed[UpdateWriteResult](new scala.RuntimeException(
        s"unsupported MongoDB version: $metadata"
      ))

    case _ => Future.failed(MissingMetadata())
  }

  /**
   * Returns an update modifier, to be used with [[findAndModify]].
   *
   * @param update the update to be applied
   * @param fetchNewObject the command result must be the new object instead of the old one.
   * @param upsert if true, creates a new document if no document matches the query, or if documents match the query, findAndModify performs an update
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
   * @param selector the query selector
   * @param modifier the modify operator to be applied
   * @param sort the optional document possibly indicating the sort criterias
   * @param fields the field [[http://docs.mongodb.org/manual/tutorial/project-fields-from-query-results/#read-operations-projection projection]]
   */
  def findAndModify[Q](selector: Q, modifier: BatchCommands.FindAndModifyCommand.Modify, sort: Option[pack.Document] = None, fields: Option[pack.Document] = None)(implicit selectorWriter: pack.Writer[Q], ec: ExecutionContext): Future[BatchCommands.FindAndModifyCommand.FindAndModifyResult] = {
    import FindAndModifyCommand.{ ImplicitlyDocumentProducer => DP }
    val command = BatchCommands.FindAndModifyCommand.FindAndModify(
      query = selector,
      modify = modifier,
      sort = sort.map(implicitly[DP](_)),
      fields = fields.map(implicitly[DP](_))
    )

    runCommand(command, readPreference)
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
   * @param selector the query selector
   * @param update the update to be applied
   * @param fetchNewObject the command result must be the new object instead of the old one.
   * @param upsert if true, creates a new document if no document matches the query, or if documents match the query, findAndModify performs an update
   * @param sort the optional document possibly indicating the sort criterias
   * @param fields the field [[http://docs.mongodb.org/manual/tutorial/project-fields-from-query-results/#read-operations-projection projection]]
   */
  def findAndUpdate[Q, U](selector: Q, update: U, fetchNewObject: Boolean = false, upsert: Boolean = false, sort: Option[pack.Document] = None, fields: Option[pack.Document] = None)(implicit selectorWriter: pack.Writer[Q], updateWriter: pack.Writer[U], ec: ExecutionContext): Future[BatchCommands.FindAndModifyCommand.FindAndModifyResult] = {
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
   * @param selector the query selector
   * @param modifier the modify operator to be applied
   * @param sort the optional document possibly indicating the sort criterias
   * @param fields the field [[http://docs.mongodb.org/manual/tutorial/project-fields-from-query-results/#read-operations-projection projection]]
   */
  def findAndRemove[Q](selector: Q, sort: Option[pack.Document] = None, fields: Option[pack.Document] = None)(implicit selectorWriter: pack.Writer[Q], ec: ExecutionContext): Future[BatchCommands.FindAndModifyCommand.FindAndModifyResult] = findAndModify[Q](selector, removeModifier, sort, fields)

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
   *   cities.aggregate(Group(BSONString("\$state"))(
   *     "totalPop" -> SumField("population")), List(
   *       Match(document("totalPop" ->
   *         document("\$gte" -> 10000000L))))).map(_.documents)
   * }
   * }}}
   *
   * @param firstOperator the first operator of the pipeline
   * @param otherOperators the sequence of MongoDB aggregation operations
   * @param explain specifies to return the information on the processing of the pipeline
   * @param allowDiskUse enables writing to temporary files
   * @param bypassDocumentValidation enables to bypass document validation during the operation
   * @param readConcern the read concern
   */
  def aggregate(firstOperator: PipelineOperator, otherOperators: List[PipelineOperator] = Nil, explain: Boolean = false, allowDiskUse: Boolean = false, bypassDocumentValidation: Boolean = false, readConcern: Option[ReadConcern] = None)(implicit ec: ExecutionContext): Future[BatchCommands.AggregationFramework.AggregationResult] = {
    import BatchCommands.AggregationFramework.Aggregate
    import BatchCommands.{ AggregateWriter, AggregateReader }

    def ver = db.connection.metadata.
      fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion)

    runWithResponse(Aggregate(
      firstOperator :: otherOperators, explain, allowDiskUse, None,
      ver, bypassDocumentValidation, readConcern
    ), readPreference).map(_.value)
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/command/aggregate/ Aggregates]] the matching documents.
   *
   * {{{
   * import scala.concurrent.Future
   * import scala.concurrent.ExecutionContext.Implicits.global
   *
   * import reactivemongo.bson._
   * import reactivemongo.api.Cursor
   * import reactivemongo.api.collections.bson.BSONCollection
   *
   * def populatedStates(cities: BSONCollection): Future[Cursor[BSONDocument]] =
   *   {
   *     import cities.BatchCommands.AggregationFramework
   *     import AggregationFramework.{
   *       Cursor => AggCursor, Group, Match, SumField
   *     }
   *
   *     val cursor = AggCursor(batchSize = 1) // initial batch size
   *
   *     cities.aggregate1[BSONDocument](Group(BSONString("\$state"))(
   *       "totalPop" -> SumField("population")), List(
   *         Match(document("totalPop" ->
   *           document("\$gte" -> 10000000L)))),
   *       cursor)
   *   }
   * }}}
   *
   * @tparam T the result type
   * @param firstOperator the first operator of the pipeline
   * @param otherOperators the sequence of MongoDB aggregation operations
   * @param cursor the cursor object for aggregation
   * @param explain specifies to return the information on the processing of the pipeline
   * @param allowDiskUse enables writing to temporary files
   * @param bypassDocumentValidation enables to bypass document validation during the operation
   * @param readConcern the read concern of the aggregation
   * @param readPreference the read preference for the result cursor
   *
   */
  def aggregate1[T](firstOperator: PipelineOperator, otherOperators: List[PipelineOperator], cursor: BatchCommands.AggregationFramework.Cursor, explain: Boolean = false, allowDiskUse: Boolean = false, bypassDocumentValidation: Boolean = false, readConcern: Option[ReadConcern] = None, readPreference: ReadPreference = ReadPreference.primary)(implicit ec: ExecutionContext, r: pack.Reader[T]): Future[Cursor[T]] = {
    import BatchCommands.AggregationFramework.{ Aggregate, AggregationResult }
    import BatchCommands.{ AggregateWriter, AggregateReader }

    import reactivemongo.core.netty.ChannelBufferWritableBuffer
    import reactivemongo.bson.buffer.WritableBuffer
    import reactivemongo.core.protocol.{ Reply, Response }

    def ver = db.connection.metadata.
      fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion)

    runWithResponse(Aggregate(
      firstOperator :: otherOperators, explain, allowDiskUse, Some(cursor),
      ver, bypassDocumentValidation, readConcern
    ), readPreference).flatMap[Cursor[T]] {
      case ResponseResult(response, numToReturn,
        AggregationResult(firstBatch, Some(resultCursor))) => Future {

        def docs = new ChannelBufferWritableBuffer().writeBytes(
          firstBatch.foldLeft[WritableBuffer](
          new ChannelBufferWritableBuffer()
        )(pack.writeToBuffer).
          toReadableBuffer
        ).buffer

        def resp = Response(
          response.header,
          Reply(0, resultCursor.cursorId, 0, firstBatch.size),
          docs, response.info
        )

        DefaultCursor.getMore[P, T](pack, resp,
          resultCursor, numToReturn, readPreference, db.
          connection, failoverStrategy, false)
      }

      case ResponseResult(response, _, _) => Future.failed[Cursor[T]](
        GenericDriverException(s"missing cursor: $response")
      )

    }
  }

  /**
   * Remove the matched document(s) from the collection and wait for the [[reactivemongo.api.commands.WriteResult]] result.
   *
   * Please read the documentation about [[reactivemongo.core.commands.GetLastError]] to know how to use it properly.
   *
   * @tparam T the type of the selector of documents to remove. An implicit `Writer[T]` typeclass for handling it has to be in the scope.
   *
   * @param query the selector of documents to remove.
   * @param writeConcern the [[reactivemongo.core.commands.GetLastError]] command message to send in order to control how the documents are removed. Defaults to GetLastError().
   * @param firstMatchOnly states whether only the first matched documents has to be removed from this collection.
   *
   * @return a future [[reactivemongo.api.commands.WriteResult]] that can be used to check whether the removal was successful.
   */
  def remove[T](query: T, writeConcern: WriteConcern = defaultWriteConcern, firstMatchOnly: Boolean = false)(implicit writer: pack.Writer[T], ec: ExecutionContext): Future[WriteResult] = db.connection.metadata match {
    case Some(metadata) if (
      metadata.maxWireVersion >= MongoWireVersion.V26
    ) => {
      import BatchCommands.DeleteCommand.{ Delete, DeleteElement }
      val limit = if (firstMatchOnly) 1 else 0

      Failover2(db.connection, failoverStrategy) { () =>
        runCommand(Delete(writeConcern = writeConcern)(
          DeleteElement(query, limit)
        ), readPreference).flatMap { wr =>
          val flattened = wr.flatten
          if (!flattened.ok) {
            // was ordered, with one doc => fail if has an error
            Future.failed(WriteResult.lastError(flattened).
              getOrElse[Exception](GenericDriverException(
                s"fails to remove: $query"
              )))
          } else Future.successful(wr)
        }
      }.future
    }

    case Some(metadata) => // Mongo < 2.6
      Future.failed[WriteResult](new scala.RuntimeException(
        s"unsupported MongoDB version: $metadata"
      ))

    case _ =>
      Future.failed(MissingMetadata())
  }

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
  def uncheckedRemove[T](query: T, firstMatchOnly: Boolean = false)(implicit writer: pack.Writer[T], ec: ExecutionContext): Unit = {
    val op = Delete(fullCollectionName, if (firstMatchOnly) 1 else 0)
    val bson = writeDoc(query, writer)
    val message = RequestMaker(op, BufferSequence(bson))
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
   * @param upsert states whether the update objet should be inserted if no match found. Defaults to false.
   * @param multi states whether the update may be done on all the matching documents.
   */
  def uncheckedUpdate[S, U](selector: S, update: U, upsert: Boolean = false, multi: Boolean = false)(implicit selectorWriter: pack.Writer[S], updateWriter: pack.Writer[U]): Unit = {
    val flags = 0 | (if (upsert) UpdateFlags.Upsert else 0) | (if (multi) UpdateFlags.MultiUpdate else 0)
    val op = Update(fullCollectionName, flags)
    val bson = writeDoc(selector, selectorWriter)
    bson.writeBytes(writeDoc(update, updateWriter))
    val message = RequestMaker(op, BufferSequence(bson))
    db.connection.send(message)
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
  def uncheckedInsert[T](document: T)(implicit writer: pack.Writer[T]): Unit = {
    val op = Insert(0, fullCollectionName)
    val bson = writeDoc(document, writer)
    val message = RequestMaker(op, BufferSequence(bson))
    db.connection.send(message)
  }

  protected object Mongo26WriteCommand {
    def insert(ordered: Boolean, writeConcern: WriteConcern, metadata: ProtocolMetadata): Mongo26WriteCommand = new Mongo26WriteCommand("insert", ordered, writeConcern, metadata)
  }

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
    def result(): ChannelBuffer
    def send()(implicit ec: ExecutionContext): Future[R]
  }

  protected class Mongo26WriteCommand private (tpe: String, ordered: Boolean, writeConcern: WriteConcern, metadata: ProtocolMetadata) extends BulkMaker[WriteResult, Mongo26WriteCommand] {
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
          tpe, ordered, writeConcern, metadata
        )

        nextCommand.putOrIssueNewCommand(doc)
        Some(nextCommand)
      } else {
        val start = buf.index
        buf.writeByte(0x03)
        buf.writeCString(docsN.toString)

        val start2 = buf.index
        pack.writeToBuffer(buf, doc)

        val result =
          if (buf.index > thresholdBytes && docsN == 0) {
            // first and already out of bound
            throw new scala.RuntimeException(s"Mongo26WriteCommand could not accept doc of size = ${buf.index - start} bytes")
          } else if (buf.index > thresholdBytes) {
            val nextCommand = new Mongo26WriteCommand(
              tpe, ordered, writeConcern, metadata
            )

            nextCommand.buf.writeByte(0x03)
            nextCommand.buf.writeCString("0")
            nextCommand.buf.buffer.
              writeBytes(buf.buffer, start2, buf.index - start2)

            nextCommand.docsN = 1
            buf.buffer.readerIndex(0)
            buf.buffer.writerIndex(start)

            closeIfNecessary()

            Some(nextCommand)
          } else None

        docsN += 1
        result
      }
    }

    def result(): ChannelBuffer = {
      closeIfNecessary()
      buf.buffer
    }

    def send()(implicit ec: ExecutionContext): Future[WriteResult] = {
      val documents = BufferSequence(result())
      val op = Query(0, db.name + ".$cmd", 0, 1)

      val cursor = DefaultCursor.query(pack, op, documents, ReadPreference.primary, db.connection, failoverStrategy, true)(BatchCommands.DefaultWriteResultReader) //(Mongo26WriteCommand.DefaultWriteResultBufferReader)

      cursor.headOption.flatMap {
        case Some(wr) if (wr.inError || (wr.hasErrors && ordered)) => {
          Future.failed(WriteResult.lastError(wr).
            getOrElse[Exception](GenericDriverException(
              s"write failure: $wr"
            )))
        }
        case Some(wr) => Future.successful(wr)
        case c => Future.failed(
          new GenericDriverException(s"no write result ? $c")
        )
      }
    }

    private def closeIfNecessary(): Unit =
      if (!done) {
        done = true
        writer.close // array
        writer.close // doc
      }

    private def init(): Unit = {
      writer.putString(tpe, name).putBoolean("ordered", ordered)
      putWriteConcern()
      writer.openArray("documents")
    }

    private def putWriteConcern(): Unit = {
      import reactivemongo.api.commands.GetLastError

      writer.openDocument("writeConcern")
      writeConcern.w match {
        case GetLastError.Majority =>
          writer.putString("w", "majority")

        case GetLastError.TagSet(tagSet) =>
          writer.putString("w", tagSet)

        case GetLastError.WaitForAknowledgments(n) =>
          writer.putInt("w", n)
      }

      if (writeConcern.j) writer.putBoolean("j", true)

      writeConcern.wtimeout foreach { writer.putInt("wtimeout", _) }

      writer.close
    }
  }
}
