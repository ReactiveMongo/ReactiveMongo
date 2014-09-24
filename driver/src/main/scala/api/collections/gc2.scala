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
package reactivemongo.api.collections2

import reactivemongo.bson.buffer.ReadableBuffer
import reactivemongo.bson.BSONValue
import reactivemongo.bson.buffer.DefaultBufferHandler
import reactivemongo.bson.BSONDocument
import reactivemongo.core.protocol._
import reactivemongo.core.netty._
import reactivemongo.bson.BSONDocumentWriter
import reactivemongo.bson.BSONDocumentReader
import reactivemongo.bson.buffer.ArrayBSONBuffer
import reactivemongo.core.commands.GetLastError
import reactivemongo.utils.EitherMappableFuture._
import play.api.libs.iteratee._
import scala.concurrent.{ ExecutionContext, Future }
import reactivemongo.core.commands.LastError
import reactivemongo.core.netty.BufferSequence
import org.jboss.netty.buffer.ChannelBuffer
import scala.util._
import scala.util.control.NonFatal
import reactivemongo.bson.buffer.WritableBuffer
import reactivemongo.core.commands.GetLastError
import reactivemongo.api._

trait GenericCollectionWithCommands[P <: SerializationPack with Singleton] { self: GenericCollection[P] =>
  val pack: P

  import reactivemongo.api.commands._

  def runner = Command.run(pack)

  def runCommand[R, C <: CollectionCommand with CommandWithResult[R]]
    (command: C with CommandWithResult[R])
    (implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R]): Future[R] =
    runner(self, command)

  def runCommand[C <: CollectionCommand]
    (command: C)
    (implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): CursorFetcher[pack.type, Cursor] =
    runner(self, command)

  def runValueCommand[A <: AnyVal, R <: BoxedAnyVal[A], C <: CollectionCommand with CommandWithResult[R]]
    (command: C with CommandWithResult[R with BoxedAnyVal[A]])
    (implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R]): Future[A] =
    runner.unboxed(self, command)
}

trait BatchCommands[P <: SerializationPack] {
  import reactivemongo.api.commands.{ InsertCommand => IC, UpdateCommand => UC, DeleteCommand => DC, DefaultWriteResult, LastError, ResolvedCollectionCommand }

  val pack: P

  val InsertCommand: IC[pack.type]
  implicit def InsertWriter: pack.Writer[ResolvedCollectionCommand[InsertCommand.Insert]]
  val UpdateCommand: UC[pack.type]
  implicit def UpdateWriter: pack.Writer[ResolvedCollectionCommand[UpdateCommand.Update]]
  implicit def UpdateReader: pack.Reader[UpdateCommand.UpdateResult]
  val DeleteCommand: DC[pack.type]
  implicit def DeleteWriter: pack.Writer[ResolvedCollectionCommand[DeleteCommand.Delete]]
  implicit def DefaultWriteResultReader: pack.Reader[DefaultWriteResult]

  implicit def LastErrorReader: pack.Reader[LastError]
}

/**
 * A Collection that provides default methods using a `Structure` (like [[reactivemongo.bson.BSONDocument]], or a Json document, etc.).
 *
 * Some methods of this collection accept instances of `Reader[T]` and `Writer[T]`, that transform any `T` instance into a `Structure` and vice-versa.
 * The default implementation of [[Collection]], [[reactivemongo.api.collections.default.BSONCollection]], extends this trait.
 *
 * @tparam Structure The structure that will be turned into BSON (and vice versa), usually a [[reactivemongo.bson.BSONDocument]] or a Json document.
 * @tparam Reader A `Reader[T]` that produces a `T` instance from a `Structure`.
 * @tparam Writer A `Writer[T]` that produces a `Structure` instance from a `T`.
 */
trait GenericCollection[P <: SerializationPack with Singleton] extends Collection with GenericCollectionWithCommands[P] with reactivemongo.api.commands.ImplicitCommandHelpers[P] { self =>
  val pack: P
  protected val BatchCommands: BatchCommands[pack.type]

  implicit def PackIdentityReader: pack.Reader[pack.Document] = pack.IdentityReader
  implicit def PackIdentityWriter: pack.Writer[pack.Document] = pack.IdentityWriter

  def failoverStrategy: FailoverStrategy
  def genericQueryBuilder: GenericQueryBuilder[pack.type]

  import BatchCommands._
  import reactivemongo.api.commands.{ MultiBulkWriteResult, UpdateWriteResult, WriteResult }


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

  protected def watchFailure[T](future: => Future[T]): Future[T] = Try(future).recover { case NonFatal(e) => Future.failed(e) }.get


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
  def find[S](selector: S)(implicit swriter: pack.Writer[S]): GenericQueryBuilder[pack.type] =
    genericQueryBuilder.query(selector)

  /**
   * Find the documents matching the given criteria.
   *
   * This method accepts any query and projection object, provided that there is an implicit `Writer[S]` typeclass for handling them in the scope.
   *
   * Please take a look to the [[http://www.mongodb.org/display/DOCS/Querying mongodb documentation]] to know how querying works.
   *
   * @tparam S the type of the selector (the query). An implicit `Writer[S]` typeclass for handling it has to be in the scope.
   * @tparam P the type of the projection object. An implicit `Writer[P]` typeclass for handling it has to be in the scope.
   *
   * @param query The selector query.
   * @param projection Get only a subset of each matched documents. Defaults to None.
   *
   * @return a [[GenericQueryBuilder]] that you can use to to customize the query. You can obtain a cursor by calling the method [[reactivemongo.api.Cursor]] on this query builder.
   */
  def find[S, P](selector: S, projection: P)(implicit swriter: pack.Writer[S], pwriter: pack.Writer[P]): GenericQueryBuilder[pack.type] =
    genericQueryBuilder.query(selector).projection(projection)

  def bulkInsert(ordered: Boolean)(documents: ImplicitlyDocumentProducer*)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    bulkInsert(documents.toStream.map(_.produce), ordered, GetLastError(), bulk.MaxDocs, bulk.MaxBulkSize)

  def bulkInsert(ordered: Boolean, writeConcern: GetLastError)(documents: ImplicitlyDocumentProducer*)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    bulkInsert(documents.toStream.map(_.produce), ordered, writeConcern, bulk.MaxDocs, bulk.MaxBulkSize)

  def bulkInsert(ordered: Boolean, writeConcern: GetLastError, bulkSize: Int, bulkByteSize: Int)(documents: ImplicitlyDocumentProducer*)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    bulkInsert(documents.toStream.map(_.produce), ordered, writeConcern, bulkSize, bulkByteSize)

  def bulkInsert(documents: Stream[pack.Document], ordered: Boolean, writeConcern: GetLastError = GetLastError(), bulkSize: Int = bulk.MaxDocs, bulkByteSize: Int = bulk.MaxBulkSize)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] = watchFailure {
    def createBulk(docs: Stream[pack.Document], command: Mongo26WriteCommand): Future[List[WriteResult]] = {
      val (tail, nc) = command.fill(docs)
      command.send().flatMap { wr =>
        if(nc.isDefined)
          createBulk(tail, nc.get).map(wr2 => wr :: wr2)
        else // done
          Future.successful(List(wr))
      }
    }
    createBulk(documents, Mongo26WriteCommand.insert(ordered, reactivemongo.api.commands.WriteConcern.DefaultWriteConcern)).map { list =>
      list.foldLeft(MultiBulkWriteResult())( (r, w) => r.merge(w) )}
  }

  /**
   * Inserts a document into the collection and wait for the [[reactivemongo.core.commands.LastError]] result.
   *
   * Please read the documentation about [[reactivemongo.core.commands.GetLastError]] to know how to use it properly.
   *
   * @tparam T the type of the document to insert. An implicit `Writer[T]` typeclass for handling it has to be in the scope.
   *
   * @param document the document to insert.
   * @param writeConcern the [[reactivemongo.core.commands.GetLastError]] command message to send in order to control how the document is inserted. Defaults to GetLastError().
   *
   * @return a future [[reactivemongo.core.commands.LastError]] that can be used to check whether the insertion was successful.
   */
  def insert[T](document: T, writeConcern: GetLastError = GetLastError())(implicit writer: pack.Writer[T], ec: ExecutionContext): Future[WriteResult] = {
    Failover2(db.connection, failoverStrategy) { () =>
      db.connection.wireVersion match {
        case Some(MongoWireVersionRange(_, MongoWireVersion(version))) if version >= MongoWireVersion.V26.value =>
          import reactivemongo.api.commands._
          println(s"insert the new way... pack = $pack")
          runCommand(BatchCommands.InsertCommand.Insert(document))
        case Some(_) =>
          println("insert the old way...")
          val op = Insert(0, fullCollectionName)
          val bson = writeDoc(document, writer)
          val checkedWriteRequest = CheckedWriteRequest(op, BufferSequence(bson), writeConcern)
          db.connection.sendExpectingResponse(checkedWriteRequest).map(pack.readAndDeserialize(_, LastErrorReader))
        case None =>
          println("failing")
          Future.failed(new IllegalStateException("connection not initialized yet"))
      }
    }.future
  }


   /*watchFailure {
    val op = Insert(0, fullCollectionName)
    val bson = writeDoc(document, writer)
    val checkedWriteRequest = CheckedWriteRequest(op, BufferSequence(bson), writeConcern)
    Failover(checkedWriteRequest, db.connection, failoverStrategy).future.mapEither(LastError.meaningful(_))
  }*/

  /**
   * Inserts a document into the collection and wait for the [[reactivemongo.core.commands.LastError]] result.
   *
   * Please read the documentation about [[reactivemongo.core.commands.GetLastError]] to know how to use it properly.
   *
   * @param document the document to insert.
   * @param writeConcern the [[reactivemongo.core.commands.GetLastError]] command message to send in order to control how the document is inserted. Defaults to GetLastError().
   *
   * @return a future [[reactivemongo.core.commands.LastError]] that can be used to check whether the insertion was successful.
   */
  def insert(document: pack.Document, writeConcern: GetLastError)(implicit ec: ExecutionContext): Future[WriteResult] =
    Failover2(db.connection, failoverStrategy) { () =>
      db.connection.wireVersion match {
        case Some(MongoWireVersionRange(_, MongoWireVersion(version))) if version >= MongoWireVersion.V26.value =>
          import reactivemongo.api.commands._
          println(s"insert the new way... pack = $pack, this = $self")
          runCommand(BatchCommands.InsertCommand.Insert(document))
        case Some(_) =>
          println("insert the old way...")
          val op = Insert(0, fullCollectionName)
          val bson = writeDoc(document)
          val checkedWriteRequest = CheckedWriteRequest(op, BufferSequence(bson), writeConcern)
          db.connection.sendExpectingResponse(checkedWriteRequest).map(pack.readAndDeserialize(_, LastErrorReader))
        case None =>
          println("failing")
          Future.failed(new IllegalStateException("connection not initialized yet"))
      }
    }.future

  /**
   * Inserts a document into the collection and wait for the [[reactivemongo.core.commands.LastError]] result.
   *
   * @param document the document to insert.
   *
   * @return a future [[reactivemongo.core.commands.LastError]] that can be used to check whether the insertion was successful.
   */
  def insert(document: pack.Document)(implicit ec: ExecutionContext): Future[WriteResult] = insert(document, GetLastError())

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
   * @return a future [[reactivemongo.core.commands.LastError]] that can be used to check whether the update was successful.
   */
  def update[S, U](selector: S, update: U, writeConcern: GetLastError = GetLastError(), upsert: Boolean = false, multi: Boolean = false)(implicit selectorWriter: pack.Writer[S], updateWriter: pack.Writer[U], ec: ExecutionContext): Future[WriteResult] = /*watchFailure {
    val flags = 0 | (if (upsert) UpdateFlags.Upsert else 0) | (if (multi) UpdateFlags.MultiUpdate else 0)
    val op = Update(fullCollectionName, flags)
    val bson = writeDoc(selector, selectorWriter)
    bson.writeBytes(writeDoc(update, updateWriter))
    val checkedWriteRequest = CheckedWriteRequest(op, BufferSequence(bson), writeConcern)
    Failover(checkedWriteRequest, db.connection, failoverStrategy).future.mapEither(LastError.meaningful(_))
  }*/
    Failover2(db.connection, failoverStrategy) { () =>
      db.connection.wireVersion match {
        case Some(MongoWireVersionRange(_, MongoWireVersion(version))) if version >= MongoWireVersion.V26.value =>
          import reactivemongo.api.commands._
          import BatchCommands.UpdateCommand.{ Update, UpdateElement }
          println("update the new way...")
          runCommand(Update(UpdateElement(selector, update)))
        case Some(_) =>
          println("update the old way...")
          val flags = 0 | (if (upsert) UpdateFlags.Upsert else 0) | (if (multi) UpdateFlags.MultiUpdate else 0)
          val op = Update(fullCollectionName, flags)
          val bson = writeDoc(selector, selectorWriter)
          bson.writeBytes(writeDoc(update, updateWriter))
          val checkedWriteRequest = CheckedWriteRequest(op, BufferSequence(bson), writeConcern)
          db.connection.sendExpectingResponse(checkedWriteRequest).map(pack.readAndDeserialize(_, LastErrorReader))
        case None =>
          println("update failing")
          Future.failed(new IllegalStateException("connection not initialized yet"))
      }
    }.future

  /**
   * Remove the matched document(s) from the collection and wait for the [[reactivemongo.core.commands.LastError]] result.
   *
   * Please read the documentation about [[reactivemongo.core.commands.GetLastError]] to know how to use it properly.
   *
   * @tparam T the type of the selector of documents to remove. An implicit `Writer[T]` typeclass for handling it has to be in the scope.
   *
   * @param query the selector of documents to remove.
   * @param writeConcern the [[reactivemongo.core.commands.GetLastError]] command message to send in order to control how the documents are removed. Defaults to GetLastError().
   * @param firstMatchOnly states whether only the first matched documents has to be removed from this collection.
   *
   * @return a future [[reactivemongo.core.commands.LastError]] that can be used to check whether the removal was successful.
   */
  def remove[T](query: T, writeConcern: GetLastError = GetLastError(), firstMatchOnly: Boolean = false)(implicit writer: pack.Writer[T], ec: ExecutionContext): Future[WriteResult] = /*watchFailure {
    val op = Delete(fullCollectionName, if (firstMatchOnly) 1 else 0)
    val bson = writeDoc(query, writer)
    val checkedWriteRequest = CheckedWriteRequest(op, BufferSequence(bson), writeConcern)
    Failover(checkedWriteRequest, db.connection, failoverStrategy).future.mapEither(LastError.meaningful(_))
  }*/
    Failover2(db.connection, failoverStrategy) { () =>
      db.connection.wireVersion match {
        case Some(MongoWireVersionRange(_, MongoWireVersion(version))) if version >= MongoWireVersion.V26.value =>
          import reactivemongo.api.commands._
          import BatchCommands.DeleteCommand.{ Delete, DeleteElement }
          println("delete the new way...")
          runCommand(Delete(DeleteElement(query, 1)))
        case Some(_) =>
          println("delete the old way...")
          val op = Delete(fullCollectionName, if (firstMatchOnly) 1 else 0)
          val bson = writeDoc(query, writer)
          val checkedWriteRequest = CheckedWriteRequest(op, BufferSequence(bson), writeConcern)
          db.connection.sendExpectingResponse(checkedWriteRequest).map(pack.readAndDeserialize(_, LastErrorReader))
        case None =>
          println("delete failing")
          Future.failed(new IllegalStateException("connection not initialized yet"))
      }
    }.future

  /*def bulkInsert2[T](enumerator: Enumerator[T], bulkSize: Int = bulk.MaxDocs, bulkByteSize: Int = bulk.MaxBulkSize)(implicit writer: pack.Writer[T], ec: ExecutionContext): Future[Int] =
    enumerator |>>> bulkInsertIteratee2(bulkSize, bulkByteSize)

  def bulkInsertIteratee2[T](bulkSize: Int = bulk.MaxDocs, bulkByteSize: Int = bulk.MaxBulkSize)(implicit writer: pack.Writer[T], ec: ExecutionContext): Iteratee[T, Int] =
    Enumeratee.map { doc: T => writeDoc(doc, writer) } &>> bulk.iteratee(this, bulkSize, bulkByteSize)*/

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

  import reactivemongo.api.commands.WriteConcern
  import reactivemongo.api.collections.BufferReader

  protected object Mongo26WriteCommand {
    def insert(ordered: Boolean, writeConcern: WriteConcern): Mongo26WriteCommand = new Mongo26WriteCommand("insert", ordered, writeConcern)
    def update(ordered: Boolean, writeConcern: WriteConcern): Mongo26WriteCommand = new Mongo26WriteCommand("update", ordered, writeConcern)
    def delete(ordered: Boolean, writeConcern: WriteConcern): Mongo26WriteCommand = new Mongo26WriteCommand("delete", ordered, writeConcern)

    private[Mongo26WriteCommand] object DefaultWriteResultBufferReader extends BufferReader[WriteResult] {
      def read(buffer: ReadableBuffer): WriteResult = {
        ???
        pack.readAndDeserialize(buffer, BatchCommands.DefaultWriteResultReader)
      }
    }
    private[Mongo26WriteCommand] object UpdateWriteResultBufferReader extends BufferReader[UpdateWriteResult] {
      def read(buffer: ReadableBuffer): UpdateWriteResult =
        pack.readAndDeserialize(buffer, BatchCommands.UpdateReader)
    }
  }

  protected class Mongo26WriteCommand private(tpe: String, ordered: Boolean, writeConcern: WriteConcern) {
    import reactivemongo.bson._
    import reactivemongo.bson.buffer._
    import reactivemongo.bson.lowlevel.LowLevelBsonDocWriter
    import reactivemongo.core.netty.ChannelBufferWritableBuffer

    private var done = false
    private var docsN = 0
    private val buf = ChannelBufferWritableBuffer()
    private val writer = new LowLevelBsonDocWriter(buf)

    // val thresholdBytes = 24022//15 * 1024 * 1024 - 2
    // val thresholdDocs = 100
    //val thresholdBytes = 1 * 1024 * 1024 - 2 - 1000
    //val thresholdBytes = 2048
    val thresholdDocs = 1000000
    val thresholdBytes = 23022

    init()

    def fill(docs: Stream[pack.Document]): (Stream[pack.Document], Option[Mongo26WriteCommand]) = {
      @scala.annotation.tailrec
      def loop(docs: Stream[pack.Document]): (Stream[pack.Document], Option[Mongo26WriteCommand]) = {
        if(docs.isEmpty)
          Stream.empty -> None
        else {
          val res = putOrIssueNewCommand(docs.head)
          if(res.isDefined)
            docs.tail -> res
          else loop(docs.tail)
        }
      }
      loop(docs)
    }

    def putOrIssueNewCommand(doc: pack.Document): Option[Mongo26WriteCommand] = {
      if(done)
        throw new RuntimeException("violated assertion: Mongo26WriteCommand should not be used again after it is done")
      if(docsN >= thresholdDocs) {
        val newmwc = new Mongo26WriteCommand(tpe, ordered, writeConcern)
        newmwc.putOrIssueNewCommand(doc)
        Some(newmwc)
      } else {
        val start = buf.index
        buf.writeByte(0x03)
        buf.writeCString(docsN.toString)
        val start2 = buf.index
        pack.writeToBuffer(buf, doc)
        val result =
          if(buf.index > thresholdBytes && docsN == 0) // first and already out of bound
            throw new RuntimeException("Mongo26WriteCommand could not accept doc of size = ${buf.index - start} bytes")
          else if(buf.index > thresholdBytes) {
            val newmwc = new Mongo26WriteCommand(tpe, ordered, writeConcern)
            newmwc.buf.writeByte(0x03)
            newmwc.buf.writeCString("0")
            newmwc.buf.buffer.writeBytes(buf.buffer, start2, buf.index - start2)
            newmwc.docsN = 1
            buf.buffer.readerIndex(0)
            buf.buffer.writerIndex(start)
            closeIfNecessary()
            Some(newmwc)
          } else None
        docsN += 1
        result
      }
    }

    // TODO remove
    def _debug(): Unit = {
      val rix = buf.buffer.readerIndex
      val wix = buf.buffer.writerIndex
      val doc = DefaultBufferHandler.BSONDocumentBufferHandler.read(new ChannelBufferReadableBuffer(buf.buffer))
      println(doc)
      println(BSONDocument.pretty(doc))
      buf.buffer.readerIndex(rix)
      buf.buffer.writerIndex(wix)
    }

    def result(): ChannelBuffer = {
      closeIfNecessary()
      buf.buffer
    }

    def send()(implicit ec: ExecutionContext): Future[WriteResult] = {
      val documents = BufferSequence(result())

      val op = Query(0, db.name + ".$cmd", 0, 1)

      val cursor = new DefaultCursor(op, documents, ReadPreference.primary, db.connection, failoverStrategy)(Mongo26WriteCommand.DefaultWriteResultBufferReader)

      cursor.headOption.flatMap {
        case Some(wr) if wr.inError => Future.failed(wr)
        case Some(wr) if wr.hasErrors && ordered => Future.failed(wr)
        case Some(wr) => Future.successful(wr)
        case None => Future.failed(new RuntimeException("no write result ?"))
      }
    }

    private def closeIfNecessary(): Unit =
      if(!done) {
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

      if(writeConcern.j)
        writer.putBoolean("j", true)

      if(writeConcern.wtimeout.isDefined)
        writer.putInt("wtimeout", writeConcern.wtimeout.get)

      writer.close
    }
  }
}

/**
 * A builder that helps to make a fine-tuned query to MongoDB.
 *
 * When the query is ready, you can call `cursor` to get a [[Cursor]], or `one` if you want to retrieve just one document.
 *
 */
trait GenericQueryBuilder[P <: SerializationPack] {
  val pack: P
  type Self <: GenericQueryBuilder[pack.type]

  def queryOption: Option[pack.Document]
  def sortOption: Option[pack.Document]
  def projectionOption: Option[pack.Document]
  def hintOption: Option[pack.Document]
  def explainFlag: Boolean
  def snapshotFlag: Boolean
  def commentString: Option[String]
  def options: QueryOpts
  def failover: FailoverStrategy
  def collection: Collection

  def merge: pack.Document

  //def structureReader: pack.Reader[pack.Document]

  def copy(
    queryOption: Option[pack.Document] = queryOption,
    sortOption: Option[pack.Document] = sortOption,
    projectionOption: Option[pack.Document] = projectionOption,
    hintOption: Option[pack.Document] = hintOption,
    explainFlag: Boolean = explainFlag,
    snapshotFlag: Boolean = snapshotFlag,
    commentString: Option[String] = commentString,
    options: QueryOpts = options,
    failover: FailoverStrategy = failover): Self

  private def write(document: pack.Document, buffer: ChannelBufferWritableBuffer = ChannelBufferWritableBuffer()): ChannelBufferWritableBuffer = {
    pack.writeToBuffer(buffer, document)
    buffer
  }

  /**
   * Sends this query and gets a [[Cursor]] of instances of `T`.
   *
   * An implicit `Reader[T]` must be present in the scope.
   */
  def cursor[T](implicit reader: pack.Reader[T], ec: ExecutionContext): Cursor[T] = cursor(ReadPreference.primary)

  /**
   * Makes a [[Cursor]] of this query, which can be enumerated.
   *
   * An implicit `Reader[T]` must be present in the scope.
   *
   * @param readPreference The ReadPreference for this request. If the ReadPreference implies that this request might be run on a Secondary, the slaveOk flag will be set.
   */
  def cursor[T](readPreference: ReadPreference)(implicit reader: pack.Reader[T], ec: ExecutionContext): Cursor[T] = {
    val documents = BufferSequence {
      val buffer = write(merge, ChannelBufferWritableBuffer())
      projectionOption.map { projection =>
        write(projection, buffer)
      }.getOrElse(buffer).buffer
    }

    val flags = if (readPreference.slaveOk) options.flagsN | QueryFlags.SlaveOk else options.flagsN

    val op = Query(flags, collection.fullCollectionName, options.skipN, options.batchSizeN)

    val br = new reactivemongo.api.collections.BufferReader[T] {
      def read(buffer: ReadableBuffer): T =
        pack.readAndDeserialize(buffer, reader)
    }
    new DefaultCursor(op, documents, readPreference, collection.db.connection, failover)(br)
  }

  /**
   * Sends this query and gets a future `Option[T]`.
   *
   * An implicit `Reader[T]` must be present in the scope.
   */
  def one[T](implicit reader: pack.Reader[T], ec: ExecutionContext): Future[Option[T]] = copy(options = options.batchSize(1)).cursor(reader, ec).headOption

  /**
   * Sends this query and gets a future `Option[T]`.
   *
   * An implicit `Reader[T]` must be present in the scope.
   *
   * @param readPreference The ReadPreference for this request. If the ReadPreference implies that this request might be run on a Secondary, the slaveOk flag will be set.
   */
  def one[T](readPreference: ReadPreference)(implicit reader: pack.Reader[T], ec: ExecutionContext): Future[Option[T]] = copy(options = options.batchSize(1)).cursor(readPreference)(reader, ec).headOption

  /**
   * Sets the query (the selector document).
   *
   * @tparam Qry The type of the query. An implicit `Writer[Qry]` typeclass for handling it has to be in the scope.
   */
  def query[Qry](selector: Qry)(implicit writer: pack.Writer[Qry]): Self = copy(queryOption = Some(
    pack.serialize(selector, writer)))

  /** Sets the query (the selector document). */
  def query(selector: pack.Document): Self = copy(queryOption = Some(selector))

  /** Sets the sorting document. */
  def sort(document: pack.Document): Self = copy(sortOption = Some(document))

  def options(options: QueryOpts): Self = copy(options = options)

  /* TODO /** Sets the sorting document. */
  def sort(sorters: (String, SortOrder)*): Self = copy(sortDoc = {
    if (sorters.size == 0)
      None
    else {
      val bson = BSONDocument(
        (for (sorter <- sorters) yield sorter._1 -> BSONInteger(
          sorter._2 match {
            case SortOrder.Ascending => 1
            case SortOrder.Descending => -1
          })).toStream)
      Some(bson)
    }
  })*/

  /**
   * Sets the projection document (for [[http://www.mongodb.org/display/DOCS/Retrieving+a+Subset+of+Fields retrieving only a subset of fields]]).
   *
   * @tparam Pjn The type of the projection. An implicit `Writer][Pjn]` typeclass for handling it has to be in the scope.
   */
  def projection[Pjn](p: Pjn)(implicit writer: pack.Writer[Pjn]): Self = copy(projectionOption = Some(
    pack.serialize(p, writer)))

  def projection(p: pack.Document): Self = copy(projectionOption = Some(p))

  /** Sets the hint document (a document that declares the index MongoDB should use for this query). */
  def hint(document: pack.Document): Self = copy(hintOption = Some(document))

  /** Sets the hint document (a document that declares the index MongoDB should use for this query). */
  // TODO def hint(indexName: String): Self = copy(hintOption = Some(BSONDocument(indexName -> BSONInteger(1))))

  //TODO def explain(flag: Boolean = true) :QueryBuilder = copy(explainFlag=flag)

  /** Toggles [[http://www.mongodb.org/display/DOCS/How+to+do+Snapshotted+Queries+in+the+Mongo+Database snapshot mode]]. */
  def snapshot(flag: Boolean = true): Self = copy(snapshotFlag = flag)

  /** Adds a comment to this query, that may appear in the MongoDB logs. */
  def comment(message: String): Self = copy(commentString = Some(message))
}



package bson {
  import reactivemongo.bson._
  import reactivemongo.api.commands.bson._

  object BSONBatchCommands extends BatchCommands[BSONSerializationPack.type] {
    val pack = BSONSerializationPack

    val InsertCommand = BSONInsertCommand
    implicit def InsertWriter = BSONInsertCommandImplicits.InsertWriter
    val UpdateCommand = BSONUpdateCommand
    implicit def UpdateWriter = BSONUpdateCommandImplicits.UpdateWriter
    implicit def UpdateReader = BSONUpdateCommandImplicits.UpdateResultReader
    val DeleteCommand = BSONDeleteCommand
    implicit def DeleteWriter = BSONDeleteCommandImplicits.DeleteWriter
    implicit def DefaultWriteResultReader = BSONCommonWriteCommandsImplicits.DefaultWriteResultReader

    implicit def LastErrorReader = BSONGetLastErrorImplicits.LastErrorReader
  }

  case class BSONCollection(val db: DB, val name: String, val failoverStrategy: FailoverStrategy) extends GenericCollection[BSONSerializationPack.type] {
    val pack = BSONSerializationPack
    val BatchCommands = BSONBatchCommands
    def genericQueryBuilder = BSONQueryBuilder(this, failoverStrategy)
  }

  case class BSONQueryBuilder(
    collection: Collection,
    failover: FailoverStrategy,
    queryOption: Option[BSONDocument] = None,
    sortOption: Option[BSONDocument] = None,
    projectionOption: Option[BSONDocument] = None,
    hintOption: Option[BSONDocument] = None,
    explainFlag: Boolean = false,
    snapshotFlag: Boolean = false,
    commentString: Option[String] = None,
    options: QueryOpts = QueryOpts()) extends GenericQueryBuilder[BSONSerializationPack.type] {
    import reactivemongo.utils.option

    type Self = BSONQueryBuilder
    val pack = BSONSerializationPack

    def copy(
      queryOption: Option[BSONDocument] = queryOption,
      sortOption: Option[BSONDocument] = sortOption,
      projectionOption: Option[BSONDocument] = projectionOption,
      hintOption: Option[BSONDocument] = hintOption,
      explainFlag: Boolean = explainFlag,
      snapshotFlag: Boolean = snapshotFlag,
      commentString: Option[String] = commentString,
      options: QueryOpts = options,
      failover: FailoverStrategy = failover): BSONQueryBuilder =
      BSONQueryBuilder(collection, failover, queryOption, sortOption, projectionOption, hintOption, explainFlag, snapshotFlag, commentString, options)

    def merge: BSONDocument =
      if (!sortOption.isDefined && !hintOption.isDefined && !explainFlag && !snapshotFlag && !commentString.isDefined)
        queryOption.getOrElse(BSONDocument())
      else
        BSONDocument(
          "$query" -> queryOption.getOrElse(BSONDocument()),
          "$orderby" -> sortOption,
          "$hint" -> hintOption,
          "$comment" -> commentString.map(BSONString(_)),
          "$explain" -> option(explainFlag, BSONBoolean(true)),
          "$snapshot" -> option(snapshotFlag, BSONBoolean(true)))

  }
}