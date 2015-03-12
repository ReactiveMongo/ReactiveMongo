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

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try
import scala.util.control.NonFatal
import org.jboss.netty.buffer.ChannelBuffer
import reactivemongo.api._
import reactivemongo.api.commands.{ LastError, WriteConcern }
import reactivemongo.bson.buffer.{ ReadableBuffer, WritableBuffer }
import reactivemongo.core.nodeset.ProtocolMetadata
import reactivemongo.core.protocol._
import reactivemongo.core.netty._
import reactivemongo.core.errors.ConnectionNotInitialized

trait GenericCollectionProducer[P <: SerializationPack with Singleton, +C <: GenericCollection[P]] extends CollectionProducer[C]

trait GenericCollectionWithCommands[P <: SerializationPack with Singleton] { self: GenericCollection[P] =>
  val pack: P

  import reactivemongo.api.commands._

  def runner = Command.run(pack)

  def runCommand[R, C <: CollectionCommand with CommandWithResult[R]]
    (command: C with CommandWithResult[R])
    (implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[R] =
    runner(self, command)

  def runCommand[C <: CollectionCommand]
    (command: C)
    (implicit writer: pack.Writer[ResolvedCollectionCommand[C]]): CursorFetcher[pack.type, Cursor] =
    runner(self, command)

  def runValueCommand[A <: AnyVal, R <: BoxedAnyVal[A], C <: CollectionCommand with CommandWithResult[R]]
    (command: C with CommandWithResult[R with BoxedAnyVal[A]])
    (implicit writer: pack.Writer[ResolvedCollectionCommand[C]], reader: pack.Reader[R], ec: ExecutionContext): Future[A] =
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
trait GenericCollection[P <: SerializationPack with Singleton] extends Collection with GenericCollectionWithCommands[P] with CollectionMetaCommands with reactivemongo.api.commands.ImplicitCommandHelpers[P] { self =>
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
    db.connection.metadata.map { metadata =>
      bulkInsert(documents.toStream.map(_.produce), ordered, WriteConcern.Default, metadata.maxBulkSize, metadata.maxBsonSize)
    }.getOrElse(Future.failed(ConnectionNotInitialized.MissingMetadata))

  def bulkInsert(ordered: Boolean, writeConcern: WriteConcern)(documents: ImplicitlyDocumentProducer*)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    db.connection.metadata.map { metadata =>
      bulkInsert(documents.toStream.map(_.produce), ordered, writeConcern, metadata.maxBulkSize, metadata.maxBsonSize)
    }.getOrElse(Future.failed(ConnectionNotInitialized.MissingMetadata))

  def bulkInsert(ordered: Boolean, writeConcern: WriteConcern, bulkSize: Int, bulkByteSize: Int)(documents: ImplicitlyDocumentProducer*)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    bulkInsert(documents.toStream.map(_.produce), ordered, writeConcern, bulkSize, bulkByteSize)

  def bulkInsert(documents: Stream[pack.Document], ordered: Boolean)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    bulkInsert(documents, ordered, WriteConcern.Default)

  def bulkInsert(documents: Stream[pack.Document], ordered: Boolean, writeConcern: WriteConcern)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] =
    db.connection.metadata.map { metadata =>
      bulkInsert(documents, ordered, writeConcern, metadata.maxBulkSize, metadata.maxBsonSize)
    }.getOrElse(Future.failed(ConnectionNotInitialized.MissingMetadata))

  def bulkInsert(documents: Stream[pack.Document], ordered: Boolean, writeConcern: WriteConcern = WriteConcern.Default, bulkSize: Int, bulkByteSize: Int)(implicit ec: ExecutionContext): Future[MultiBulkWriteResult] = watchFailure {
    def createBulk[R, A <: BulkMaker[R, A]](docs: Stream[pack.Document], command: A with BulkMaker[R, A]): Future[List[R]] =  {
      val (tail, nc) = command.fill(docs)
      command.send().
        flatMap { wr =>
          if(nc.isDefined)
            createBulk(tail, nc.get).map(wr2 => wr :: wr2)
          else // done
            Future.successful(List(wr))
        }
    }
    val metadata = db.connection.metadata
    if(!documents.isEmpty) {
      val havingMetadata = Failover2(db.connection, failoverStrategy) { () =>
        metadata.map(Future.successful).getOrElse(Future.failed(ConnectionNotInitialized.MissingMetadata))
      }.future
      havingMetadata.flatMap { metadata =>
        if(metadata.maxWireVersion >= MongoWireVersion.V26) {
          createBulk(documents, Mongo26WriteCommand.insert(ordered, writeConcern, metadata)).map { list =>
            list.foldLeft(MultiBulkWriteResult())( (r, w) => r.merge(w) )}
        } else {
          createBulk(documents, new Mongo24BulkInsert(Insert(0, fullCollectionName), writeConcern, metadata)).map { list =>
            list.foldLeft(MultiBulkWriteResult())( (r, w) => r.merge(w) )}
        }
      }
    } else {
      Future.successful(MultiBulkWriteResult(
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
  def insert[T](document: T, writeConcern: WriteConcern = WriteConcern.Default)(implicit writer: pack.Writer[T], ec: ExecutionContext): Future[WriteResult] = {
    Failover2(db.connection, failoverStrategy) { () =>
      import MongoWireVersion._
      db.connection.metadata match {
        case Some(metadata) if metadata.maxWireVersion >= MongoWireVersion.V26 =>
          import reactivemongo.api.commands._
          runCommand(BatchCommands.InsertCommand.Insert(document)).flatMap { wr =>
            val flattened = wr.flatten
            if(!flattened.ok) // was ordered, with one doc => fail if has an error
              Future.failed(flattened)
            else Future.successful(wr)
          }
        case Some(_) =>
          val op = Insert(0, fullCollectionName)
          val bson = writeDoc(document, writer)
          val checkedWriteRequest = CheckedWriteRequest(op, BufferSequence(bson), writeConcern)
          db.connection.sendExpectingResponse(checkedWriteRequest).map(pack.readAndDeserialize(_, LastErrorReader))
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
   * @return a future [[reactivemongo.core.commands.LastError]] that can be used to check whether the update was successful.
   */
  def update[S, U](selector: S, update: U, writeConcern: WriteConcern = WriteConcern.Default, upsert: Boolean = false, multi: Boolean = false)(implicit selectorWriter: pack.Writer[S], updateWriter: pack.Writer[U], ec: ExecutionContext): Future[WriteResult] =
    Failover2(db.connection, failoverStrategy) { () =>
      db.connection.metadata match {
        case Some(metadata) if metadata.maxWireVersion >= MongoWireVersion.V26 =>
          import reactivemongo.api.commands._
          import BatchCommands.UpdateCommand.{ Update, UpdateElement }
          runCommand(Update(UpdateElement(selector, update, upsert, multi))).
            flatMap { wr =>
              val flattened = wr.flatten
              if (!flattened.ok) {
                // was ordered, with one doc => fail if has an error
                Future.failed(flattened)
              } else Future.successful(wr)
            }

        case Some(_) =>
          val flags = 0 | (if (upsert) UpdateFlags.Upsert else 0) | (if (multi) UpdateFlags.MultiUpdate else 0)
          val op = Update(fullCollectionName, flags)
          val bson = writeDoc(selector, selectorWriter)
          bson.writeBytes(writeDoc(update, updateWriter))
          val checkedWriteRequest = CheckedWriteRequest(op, BufferSequence(bson), writeConcern)
          db.connection.sendExpectingResponse(checkedWriteRequest).map(pack.readAndDeserialize(_, LastErrorReader))
        case None =>
          Future.failed(ConnectionNotInitialized.MissingMetadata)
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
  def remove[T](query: T, writeConcern: WriteConcern = WriteConcern.Default, firstMatchOnly: Boolean = false)(implicit writer: pack.Writer[T], ec: ExecutionContext): Future[WriteResult] =
    Failover2(db.connection, failoverStrategy) { () =>
      db.connection.metadata match {
        case Some(metadata) if metadata.maxWireVersion >= MongoWireVersion.V26 =>
          import reactivemongo.api.commands._
          import BatchCommands.DeleteCommand.{ Delete, DeleteElement }
          runCommand(Delete(DeleteElement(query, 1))).flatMap { wr =>
            val flattened = wr.flatten
            if(!flattened.ok) // was ordered, with one doc => fail if has an error
              Future.failed(flattened)
            else Future.successful(wr)
          }
        case Some(_) =>
          val op = Delete(fullCollectionName, if (firstMatchOnly) 1 else 0)
          val bson = writeDoc(query, writer)
          val checkedWriteRequest = CheckedWriteRequest(op, BufferSequence(bson), writeConcern)
          db.connection.sendExpectingResponse(checkedWriteRequest).map(pack.readAndDeserialize(_, LastErrorReader))
        case None =>
          Future.failed(ConnectionNotInitialized.MissingMetadata)
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

  protected object Mongo26WriteCommand {
    def insert(ordered: Boolean, writeConcern: WriteConcern, metadata: ProtocolMetadata): Mongo26WriteCommand = new Mongo26WriteCommand("insert", ordered, writeConcern, metadata)
    def update(ordered: Boolean, writeConcern: WriteConcern, metadata: ProtocolMetadata): Mongo26WriteCommand = new Mongo26WriteCommand("update", ordered, writeConcern, metadata)
    def delete(ordered: Boolean, writeConcern: WriteConcern, metadata: ProtocolMetadata): Mongo26WriteCommand = new Mongo26WriteCommand("delete", ordered, writeConcern, metadata)
  }

  import reactivemongo.bson._
  import reactivemongo.bson.lowlevel.LowLevelBsonDocWriter
  import reactivemongo.core.netty.ChannelBufferWritableBuffer

  protected sealed trait BulkMaker[R, S <: BulkMaker[R, S]] {
    def fill(docs: Stream[pack.Document]): (Stream[pack.Document], Option[S]) = {
      @scala.annotation.tailrec
      def loop(docs: Stream[pack.Document]): (Stream[pack.Document], Option[S]) = {
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
    def putOrIssueNewCommand(doc: pack.Document): Option[S]
    def result(): ChannelBuffer
    def send()(implicit ec: ExecutionContext): Future[R]
  }

  protected class Mongo26WriteCommand private(tpe: String, ordered: Boolean, writeConcern: WriteConcern, metadata: ProtocolMetadata) extends BulkMaker[WriteResult, Mongo26WriteCommand] {
    private var done = false
    private var docsN = 0
    private val buf = ChannelBufferWritableBuffer()
    private val writer = new LowLevelBsonDocWriter(buf)

    val thresholdDocs = metadata.maxBulkSize
    // minus 2 for the trailing '\0'
    val thresholdBytes = metadata.maxBsonSize - 2

    init()

    def putOrIssueNewCommand(doc: pack.Document): Option[Mongo26WriteCommand] = {
      if(done)
        throw new RuntimeException("violated assertion: Mongo26WriteCommand should not be used again after it is done")
      if(docsN >= thresholdDocs) {
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
          if(buf.index > thresholdBytes && docsN == 0) // first and already out of bound
            throw new RuntimeException("Mongo26WriteCommand could not accept doc of size = ${buf.index - start} bytes")
          else if(buf.index > thresholdBytes) {
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

    // TODO remove
    def _debug(): Unit = {
      import reactivemongo.bson.buffer.DefaultBufferHandler
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

      val cursor = DefaultCursor(pack, op, documents, ReadPreference.primary, db.connection, failoverStrategy, true)(BatchCommands.DefaultWriteResultReader)//(Mongo26WriteCommand.DefaultWriteResultBufferReader)

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

  protected class Mongo24BulkInsert(op: Insert, writeConcern: WriteConcern, metadata: ProtocolMetadata) extends BulkMaker[LastError, Mongo24BulkInsert] {
    private var done = false
    private var docsN = 0
    private val buf = ChannelBufferWritableBuffer()

    val thresholdDocs = metadata.maxBulkSize
    // max size for docs is max bson size minus 16 bytes for header, 4 bytes for flags, and 124 bytes max for fullCollectionName
    val thresholdBytes = metadata.maxBsonSize - (4 * 4 + 4 + 124)

    def putOrIssueNewCommand(doc: pack.Document): Option[Mongo24BulkInsert] = {
      if(done)
        throw new RuntimeException("violated assertion: Mongo24BulkInsert should not be used again after it is done")
      if(docsN >= thresholdDocs) {
        val nextBulk = new Mongo24BulkInsert(op, writeConcern, metadata)
        nextBulk.putOrIssueNewCommand(doc)
        Some(nextBulk)
      } else {
        val start = buf.index
        pack.writeToBuffer(buf, doc)
        if(buf.index > thresholdBytes) {
          if(docsN == 0) // first and already out of bound
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
      val f = () => db.connection.sendExpectingResponse(resultAsCheckedWriteRequest(op, writeConcern))
      Failover2(db.connection, failoverStrategy)(f).future.map(pack.readAndDeserialize(_, LastErrorReader))
    }
  }
}
