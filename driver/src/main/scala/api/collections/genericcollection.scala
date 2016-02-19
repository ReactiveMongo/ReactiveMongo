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

import scala.collection.immutable.ListSet

import scala.concurrent.{ ExecutionContext, Future }

import org.jboss.netty.buffer.ChannelBuffer

import reactivemongo.api._
import reactivemongo.api.commands.{
  CursorFetcher,
  LastError,
  ResponseResult,
  WriteConcern
}
import reactivemongo.bson.buffer.{ ReadableBuffer, WritableBuffer }
import reactivemongo.core.nodeset.ProtocolMetadata
import reactivemongo.core.protocol.{
  CheckedWriteRequest,
  Delete,
  Query,
  Insert,
  MongoWireVersion,
  RequestMaker,
  Update,
  UpdateFlags
}
import reactivemongo.core.netty.{
  BufferSequence,
  ChannelBufferReadableBuffer,
  ChannelBufferWritableBuffer
}
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

  def runner = Command.run(pack)

  def runCommand[R, C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R])(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[R] =
    runner(self, command)

  def runWithResponse[R, C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R])(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[ResponseResult[R]] = runner.withResponse(self, command)

  def runCommand[C <: CollectionCommand](command: C)(implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): CursorFetcher[pack.type, Cursor] =
    runner(self, command)

  def runValueCommand[A <: AnyVal, R <: BoxedAnyVal[A], C <: CollectionCommand with CommandWithResult[R]](command: C with CommandWithResult[R with BoxedAnyVal[A]])(implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[A] =
    runner.unboxed(self, command)
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

  import BatchCommands._
  import reactivemongo.api.commands.{
    MultiBulkWriteResult,
    UpdateWriteResult,
    Upserted,
    WriteResult
  }

  private def writeDoc(doc: pack.Document): ChannelBuffer = {
    val buffer = ChannelBufferWritableBuffer()
    pack.writeToBuffer(buffer, doc)
    buffer.buffer
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
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
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
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam S the type of the selector (the query). An implicit `Writer[S]` typeclass for handling it has to be in the scope.
   * @tparam P the type of the projection object. An implicit `Writer[P]` typeclass for handling it has to be in the scope.
   *
   * @param selector the query selector.
   * @param projection Get only a subset of each matched documents. Defaults to None.
   *
   * @return a [[GenericQueryBuilder]] that you can use to to customize the query. You can obtain a cursor by calling the method [[reactivemongo.api.Cursor]] on this query builder.
   */
  def find[S, P](selector: S, projection: P)(implicit swriter: pack.Writer[S], pwriter: pack.Writer[P]): GenericQueryBuilder[pack.type] =
    genericQueryBuilder.query(selector).projection(projection)

  /**
   * Count the documents matching the given criteria.
   *
   * This method accepts any query or hint, the scope provides instances of appropriate typeclasses.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam H the type of hint. An implicit `H => Hint` conversion has to be in the scope.
   *
   * @param selector the query selector
   * @param limit the maximum number of matching documents to count
   * @param skip the number of matching documents to skip before counting
   * @param hint the index to use (either the index name or the index document)
   */
  def count[H](selector: Option[pack.Document] = None, limit: Int = 0, skip: Int = 0, hint: Option[H] = None)(implicit h: H => CountCommand.Hint, ec: ExecutionContext): Future[Int] = runValueCommand(CountCommand.Count(query = selector, limit, skip, hint.map(h)))

  /**
   * Returns the distinct values for a specified field across a single collection and returns the results in an array.
   * @param key the field for which to return distinct values
   * @param selector the query selector that specifies the documents from which to retrieve the distinct values.
   * @param readConcern the read concern
   */
  def distinct[T](key: String, selector: Option[pack.Document] = None, readConcern: ReadConcern = ReadConcern.Local)(implicit reader: pack.NarrowValueReader[T], ec: ExecutionContext): Future[ListSet[T]] = {
    implicit val widenReader = pack.widenReader(reader)
    val version = db.connection.metadata.
      fold[MongoWireVersion](MongoWireVersion.V30)(_.maxWireVersion)

    runCommand(DistinctCommand.Distinct(
      key, selector, readConcern, version)).flatMap {
      _.result[T] match {
        case Failure(cause)  => Future.failed[ListSet[T]](cause)
        case Success(result) => Future.successful(result)
      }
    }
  }

  @inline private def defaultWriteConcern = db.connection.options.writeConcern

  def bulkInsert(ordered: Boolean)(documents: ImplicitlyDocumentProducer*)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    db.connection.metadata.map { metadata =>
      bulkInsert(documents.toStream.map(_.produce), ordered, defaultWriteConcern, metadata.maxBulkSize, metadata.maxBsonSize)
    }.getOrElse(Future.failed(ConnectionNotInitialized.MissingMetadata))

  def bulkInsert(ordered: Boolean, writeConcern: WriteConcern)(documents: ImplicitlyDocumentProducer*)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    db.connection.metadata.map { metadata =>
      bulkInsert(documents.toStream.map(_.produce), ordered, writeConcern, metadata.maxBulkSize, metadata.maxBsonSize)
    }.getOrElse(Future.failed(ConnectionNotInitialized.MissingMetadata))

  def bulkInsert(ordered: Boolean, writeConcern: WriteConcern, bulkSize: Int, bulkByteSize: Int)(documents: ImplicitlyDocumentProducer*)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    bulkInsert(documents.toStream.map(_.produce), ordered, writeConcern, bulkSize, bulkByteSize)

  def bulkInsert(documents: Stream[pack.Document], ordered: Boolean)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    bulkInsert(documents, ordered, defaultWriteConcern)

  def bulkInsert(documents: Stream[pack.Document], ordered: Boolean, writeConcern: WriteConcern)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    db.connection.metadata.map { metadata =>
      bulkInsert(documents, ordered, writeConcern, metadata.maxBulkSize, metadata.maxBsonSize)
    }.getOrElse(Future.failed(ConnectionNotInitialized.MissingMetadata))

  def bulkInsert(documents: Stream[pack.Document], ordered: Boolean, writeConcern: WriteConcern = defaultWriteConcern, bulkSize: Int, bulkByteSize: Int)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] = watchFailure {
    def createBulk[R, A <: BulkMaker[R, A]](docs: Stream[pack.Document], command: A with BulkMaker[R, A]): Future[List[R]] = {
      val (tail, nc) = command.fill(docs)
      command.send().flatMap { wr =>
        if (nc.isDefined) createBulk(tail, nc.get).map(wr2 => wr :: wr2)
        else Future.successful(List(wr)) // done
      }
    }
    val metadata = db.connection.metadata
    if (!documents.isEmpty) {
      // TODO: Await maxTimeout?
      val havingMetadata = Failover2(db.connection, failoverStrategy) { () =>
        metadata.map(Future.successful).getOrElse(Future.failed(ConnectionNotInitialized.MissingMetadata))
      }.future
      havingMetadata.flatMap { metadata =>
        if (metadata.maxWireVersion >= MongoWireVersion.V26) {
          createBulk(documents, Mongo26WriteCommand.insert(ordered, writeConcern, metadata)).map { list =>
            list.foldLeft(MultiBulkWriteResult())((r, w) => r.merge(w))
          }
        } else {
          // Mongo 2.4 // TODO: Deprecate/remove
          createBulk(documents, new Mongo24BulkInsert(Insert(0, fullCollectionName), writeConcern, metadata)).map { list =>
            list.foldLeft(MultiBulkWriteResult())((r, w) => r.merge(w))
          }
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
      totalN = 0))
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
  def insert[T](document: T, writeConcern: WriteConcern = defaultWriteConcern)(implicit writer: pack.Writer[T], ec: ExecutionContext): Future[WriteResult] = {
    // TODO: Await maxTimeout
    Failover2(db.connection, failoverStrategy) { () =>
      db.connection.metadata match {
        case Some(metadata) if metadata.maxWireVersion >= MongoWireVersion.V26 =>
          runCommand(BatchCommands.InsertCommand.Insert(
            writeConcern = writeConcern)(document)).flatMap { wr =>
            val flattened = wr.flatten
            if (!flattened.ok) {
              // was ordered, with one doc => fail if has an error
              Future.failed(WriteResult.lastError(flattened).
                getOrElse[Exception](GenericDriverException(
                  s"fails to insert: $document")))

            } else Future.successful(wr)
          }

        case Some(_) => { // Mongo < 2.6 // TODO: Deprecates/remove
          val op = Insert(0, fullCollectionName)
          val bson = writeDoc(document, writer)
          val checkedWriteRequest = CheckedWriteRequest(op, BufferSequence(bson), writeConcern)
          db.connection.sendExpectingResponse(checkedWriteRequest).
            map(pack.readAndDeserialize(_, LastErrorReader))
        }

        case None =>
          Future.failed(ConnectionNotInitialized.MissingMetadata)
      }
    }.future
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
  def update[S, U](selector: S, update: U, writeConcern: WriteConcern = defaultWriteConcern, upsert: Boolean = false, multi: Boolean = false)(implicit selectorWriter: pack.Writer[S], updateWriter: pack.Writer[U], ec: ExecutionContext): Future[UpdateWriteResult] = Failover2(db.connection, failoverStrategy) { () =>
    // TODO: Await maxTimeout
    db.connection.metadata match {
      case Some(metadata) if (
        metadata.maxWireVersion >= MongoWireVersion.V26) => {
        import BatchCommands.UpdateCommand.{ Update, UpdateElement }

        runCommand(Update(writeConcern = writeConcern)(
          UpdateElement(selector, update, upsert, multi))).flatMap { wr =>
          val flattened = wr.flatten
          if (!flattened.ok) {
            // was ordered, with one doc => fail if has an error
            Future.failed(WriteResult.lastError(flattened).
              getOrElse[Exception](GenericDriverException(
                s"fails to update: $update")))

          } else Future.successful(wr)
        }
      }

      case Some(_) => { // Mongo < 2.6 // TODO: Deprecate/remove
        val flags = 0 | (if (upsert) UpdateFlags.Upsert else 0) | (if (multi) UpdateFlags.MultiUpdate else 0)
        val op = Update(fullCollectionName, flags)
        val bson = writeDoc(selector, selectorWriter)
        bson.writeBytes(writeDoc(update, updateWriter))
        val checkedWriteRequest =
          CheckedWriteRequest(op, BufferSequence(bson), writeConcern)

        db.connection.sendExpectingResponse(checkedWriteRequest).map { r =>
          val res = pack.readAndDeserialize(r, LastErrorReader)
          UpdateWriteResult(res.ok, res.n, res.n,
            res.upserted.map(Upserted(-1, _)).toSeq,
            Nil, None, res.code, res.errmsg)

        }
      }

      case None => Future.failed(ConnectionNotInitialized.MissingMetadata)
    }
  }.future

  /**
   * Returns an update modifier, to be used with [[findAndModify]].
   *
   * @param update the update to be applied
   * @param fetchNewObject the command result must be the new object instead of the old one.
   * @param upsert if true, creates a new document if no document matches the query, or if documents match the query, findAndModify performs an update
   */
  def updateModifier[U](update: U, fetchNewObject: Boolean = false, upsert: Boolean = false)(implicit updateWriter: pack.Writer[U]): BatchCommands.FindAndModifyCommand.Update = BatchCommands.FindAndModifyCommand.Update(update, fetchNewObject, upsert)

  /** Returns a removal modifier, to be used with [[findAndModify]]. */
  lazy val removeModifier = BatchCommands.FindAndModifyCommand.Remove

  /**
   * Applies a [[http://docs.mongodb.org/manual/reference/command/findAndModify/ findAndModify]] operation. See [[findAndUpdate]] and [[findAndRemove]] convenient functions.
   *
   * {{{
   * val updateOp = collection.updateModifier(
   *   BSONDocument("\$set" -> BSONDocument("age" -> 35)))
   *
   * val personBeforeUpdate: Future[Person] =
   *   collection.findAndModify(BSONDocument("name" -> "Joline"), updateOp).
   *   map(_.result[Person])
   *
   * val removedPerson: Future[Person] = collection.findAndModify(
   *   BSONDocument("name" -> "Jack"), collection.removeModifier)
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
      fields = fields.map(implicitly[DP](_)))

    runCommand(command)
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

    def ver = db.connection.metadata.fold[MongoWireVersion](
      MongoWireVersion.V26)(_.maxWireVersion)

    runWithResponse(Aggregate(
      firstOperator :: otherOperators, explain, allowDiskUse, None,
      ver, bypassDocumentValidation, readConcern)).map(_.value)
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
   * def populatedStates(col: BSONCollection): Future[Cursor[BSONDocument]] = {
   *   import cities.BatchCommands.AggregationFramework
   *   import AggregationFramework.{ Group, Match, SumField }
   *
   *   col.aggregate1[BSONDocument](Group(BSONString("\$state"))(
   *     "totalPop" -> SumField("population")), List(
   *       Match(document("totalPop" ->
   *         document("\$gte" -> 10000000L)))))
   * }
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

    def ver = db.connection.metadata.fold[MongoWireVersion](
      MongoWireVersion.V26)(_.maxWireVersion)

    runWithResponse(Aggregate(
      firstOperator :: otherOperators, explain, allowDiskUse, Some(cursor),
      ver, bypassDocumentValidation, readConcern)).flatMap[Cursor[T]] {
      case ResponseResult(response, numToReturn,
        AggregationResult(firstBatch, Some(resultCursor))) => Future {

        def docs = new ChannelBufferWritableBuffer().writeBytes(
          firstBatch.foldLeft[WritableBuffer](
            new ChannelBufferWritableBuffer())(pack.writeToBuffer).
            toReadableBuffer).buffer

        def resp = Response(response.header,
          Reply(0, resultCursor.cursorId, 0, firstBatch.size),
          docs, response.info)

        DefaultCursor.getMore[P, T](pack, resp,
          resultCursor, numToReturn, readPreference, db.
            connection, failoverStrategy, false)
      }

      case ResponseResult(response, _, _) => Future.failed[Cursor[T]](
        GenericDriverException(s"missing cursor: $response"))

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
  def remove[T](query: T, writeConcern: WriteConcern = defaultWriteConcern, firstMatchOnly: Boolean = false)(implicit writer: pack.Writer[T], ec: ExecutionContext): Future[WriteResult] =
    Failover2(db.connection, failoverStrategy) { () =>
      // TODO: Await maxTimeout
      db.connection.metadata match {
        case Some(metadata) if (
          metadata.maxWireVersion >= MongoWireVersion.V26) => {
          import BatchCommands.DeleteCommand.{ Delete, DeleteElement }
          val limit = if (firstMatchOnly) 1 else 0
          runCommand(Delete(writeConcern = writeConcern)(
            DeleteElement(query, limit))).flatMap { wr =>
            val flattened = wr.flatten
            if (!flattened.ok) {
              // was ordered, with one doc => fail if has an error
              Future.failed(WriteResult.lastError(flattened).
                getOrElse[Exception](GenericDriverException(
                  s"fails to remove: $query")))
            } else Future.successful(wr)
          }
        }

        case Some(_) => { // Mongo < 2.6 // TODO: Deprecate/remove
          val op = Delete(fullCollectionName, if (firstMatchOnly) 1 else 0)
          val bson = writeDoc(query, writer)
          val checkedWriteRequest = CheckedWriteRequest(op, BufferSequence(bson), writeConcern)
          db.connection.sendExpectingResponse(checkedWriteRequest).
            map(pack.readAndDeserialize(_, LastErrorReader))
        }

        case None =>
          Future.failed(ConnectionNotInitialized.MissingMetadata)
      }
    }.future

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
      if (done)
        throw new RuntimeException("violated assertion: Mongo26WriteCommand should not be used again after it is done")
      if (docsN >= thresholdDocs) {
        closeIfNecessary()
        val nextCommand = new Mongo26WriteCommand(tpe, ordered, writeConcern, metadata)
        nextCommand.putOrIssueNewCommand(doc)
        Some(nextCommand)
      } else {
        val start = buf.index
        buf.writeByte(0x03)
        buf.writeCString(docsN.toString)
        val start2 = buf.index
        pack.writeToBuffer(buf, doc)
        val result =
          if (buf.index > thresholdBytes && docsN == 0) // first and already out of bound
            throw new RuntimeException(s"Mongo26WriteCommand could not accept doc of size = ${buf.index - start} bytes")
          else if (buf.index > thresholdBytes) {
            val nextCommand = new Mongo26WriteCommand(tpe, ordered, writeConcern, metadata)
            nextCommand.buf.writeByte(0x03)
            nextCommand.buf.writeCString("0")
            nextCommand.buf.buffer.writeBytes(buf.buffer, start2, buf.index - start2)
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
              s"write failure: $wr")))
        }
        case Some(wr) => Future.successful(wr)
        case None => Future.failed(
          new GenericDriverException("no write result ?"))
      }
    }

    private def closeIfNecessary(): Unit =
      if (!done) {
        done = true
        writer.close // array
        writer.close // doc
      }

    private def init(): Unit = {
      writer.
        putString(tpe, name).
        putBoolean("ordered", ordered)
      putWriteConcern()
      writer.openArray("documents")
    }

    private def putWriteConcern(): Unit = {
      import reactivemongo.api.commands.GetLastError

      writer.openDocument("writeConcern")
      writeConcern.w match {
        case GetLastError.Majority                 => writer.putString("w", "majority")
        case GetLastError.TagSet(tagSet)           => writer.putString("w", tagSet)
        case GetLastError.WaitForAknowledgments(n) => writer.putInt("w", n)
      }

      if (writeConcern.j) writer.putBoolean("j", true)

      writeConcern.wtimeout foreach { writer.putInt("wtimeout", _) }

      writer.close
    }
  }

  protected class Mongo24BulkInsert(op: Insert, writeConcern: WriteConcern, metadata: ProtocolMetadata) extends BulkMaker[LastError, Mongo24BulkInsert] {
    private var done = false
    private var docsN = 0
    private val buf = ChannelBufferWritableBuffer()

    val thresholdDocs = metadata.maxBulkSize
    // max size for docs is max bson size minus 16 bytes for header, 4 bytes for flags, and 124 bytes max for fullCollectionName
    val thresholdBytes = metadata.maxBsonSize - (4 * 4 + 4 + 124)

    def putOrIssueNewCommand(doc: pack.Document): Option[Mongo24BulkInsert] = {
      if (done)
        throw new RuntimeException("violated assertion: Mongo24BulkInsert should not be used again after it is done")
      if (docsN >= thresholdDocs) {
        val nextBulk = new Mongo24BulkInsert(op, writeConcern, metadata)
        nextBulk.putOrIssueNewCommand(doc)
        Some(nextBulk)
      } else {
        val start = buf.index
        pack.writeToBuffer(buf, doc)
        if (buf.index > thresholdBytes) {
          if (docsN == 0) // first and already out of bound
            throw new RuntimeException("Mongo24BulkInsert could not accept doc of size = ${buf.index - start} bytes")
          val nextBulk = new Mongo24BulkInsert(op, writeConcern, metadata)
          nextBulk.buf.buffer.writeBytes(buf.buffer, start, buf.index - start)
          nextBulk.docsN = 1
          buf.buffer.readerIndex(0)
          buf.buffer.writerIndex(start)
          done = true
          Some(nextBulk)
        } else {
          docsN += 1
          None
        }
      }
    }

    def result(): ChannelBuffer = {
      done = true
      buf.buffer
    }

    def resultAsCheckedWriteRequest(op: Insert, writeConcern: WriteConcern) = {
      CheckedWriteRequest(op, BufferSequence(result()), writeConcern)
    }

    def send()(implicit ec: ExecutionContext): Future[LastError] = {
      val f = () => db.connection.sendExpectingResponse(
        resultAsCheckedWriteRequest(op, writeConcern))

      Failover2(db.connection, failoverStrategy)(f).future.
        map(pack.readAndDeserialize(_, LastErrorReader))
    }
  }
}
