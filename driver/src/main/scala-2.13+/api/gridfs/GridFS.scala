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
package reactivemongo.api.gridfs

import java.io.{ InputStream, OutputStream }
import java.util.Arrays
import java.security.MessageDigest

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.api.{
  Collection,
  Cursor,
  CursorProducer,
  DB,
  DBMetaCommands,
  FailingCursor,
  FailoverStrategy,
  QueryOpts,
  ReadPreference,
  Serialization,
  SerializationPack
}

import reactivemongo.api.commands.{
  CollStats,
  Command,
  CommandError,
  CommandCodecs,
  ResolvedCollectionCommand,
  WriteResult
}

import reactivemongo.core.errors.ReactiveMongoException

import reactivemongo.api.collections.{ GenericCollection, GenericQueryBuilder }

import reactivemongo.api.indexes.{ Index, IndexType }, IndexType.Ascending

import reactivemongo.api.gridfs.{ ReadFile => RF }

/**
 * A GridFS store.
 *
 * @define findDescription Finds the files matching the given selector
 * @define fileSelector the query to find the files
 * @define readFileParam the file to be read
 * @define fileReader fileReader a file reader automatically resolved if `Id` is a valid value
 */
sealed trait GridFS[P <: SerializationPack]
  extends GridFSSerialization[P] { self =>

  /* The database where this store is located. */
  protected def db: DB with DBMetaCommands

  /*
   * The prefix of this store.
   * The `files` and `chunks` collections will be actually
   * named `\${prefix}.files` and `\${prefix}.chunks`.
   */
  protected def prefix: String

  private[reactivemongo] val pack: P

  /* The `files` collection */
  private lazy val fileColl = new Collection {
    val db = self.db
    val name = s"${self.prefix}.files"
    val failoverStrategy = db.failoverStrategy
  }

  /* The `chunks` collection */
  private lazy val chunkColl = new Collection {
    val db = self.db
    val name = s"${self.prefix}.chunks"
    val failoverStrategy = db.failoverStrategy
  }

  private lazy val runner = Command.run[pack.type](pack, db.failoverStrategy)

  private lazy val builder = pack.newBuilder

  private lazy val decoder = pack.newDecoder

  import builder.{ document, elementProducer => elem }

  type ReadFile[Id <: P#Value] = RF[P, Id]

  /**
   * $findDescription.
   *
   * @param selector $fileSelector
   * @param r $fileReader
   *
   * @tparam S The type of the selector document. An implicit `Writer[S]` must be in the scope.
   * @tparam Id the type of the file ID to be read
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.gridfs.GridFS
   *
   * import reactivemongo.api.bson.{ BSONDocument, BSONValue }
   * import reactivemongo.api.bson.collection.{ BSONSerializationPack => Pack }
   *
   * def foo(gfs: GridFS[Pack.type], n: String)(implicit ec: ExecutionContext) =
   *   gfs.find[BSONDocument, BSONValue](
   *     BSONDocument("filename" -> n)).headOption
   * }}}
   */
  def find[S, Id <: pack.Value](selector: S)(implicit w: pack.Writer[S], r: FileReader[Id], cp: CursorProducer[ReadFile[Id]]): cp.ProducedCursor = try {
    val q = pack.serialize(selector, w)
    val query = new QueryBuilder(fileColl, db.failoverStrategy, Some(q), None)

    import r.reader

    query.cursor[ReadFile[Id]](defaultReadPreference)
  } catch {
    case NonFatal(cause) =>
      FailingCursor(db.connection, cause)
  }

  /**
   * $findDescription.
   *
   * @param selector $fileSelector
   * @param r $fileReader
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.gridfs.GridFS
   *
   * import reactivemongo.api.bson.{ BSONDocument, BSONValue }
   * import reactivemongo.api.bson.collection.{ BSONSerializationPack => Pack }
   *
   * def foo(gfs: GridFS[Pack.type], n: String)(implicit ec: ExecutionContext) =
   *   gfs.find(BSONDocument("filename" -> n)).headOption
   * }}}
   */
  def find(selector: pack.Document)(implicit ec: ExecutionContext, r: FileReader[pack.Value], cp: CursorProducer[ReadFile[pack.Value]]): cp.ProducedCursor = {
    implicit def w = pack.IdentityWriter
    find[pack.Document, pack.Value](selector)
  }

  /**
   * Returns a cursor for the chunks of the specified file.
   * The cursor walks the chunks orderly.
   *
   * @param file $readFileParam
   */
  def chunks(
    file: ReadFile[pack.Value],
    readPreference: ReadPreference = defaultReadPreference)(implicit cp: CursorProducer[Array[Byte]]): cp.ProducedCursor = {
    val selectorOpts = chunkSelector(file)
    val sortOpts = document(Seq(elem("n", builder.int(1))))
    implicit def reader = chunkReader

    val query = new QueryBuilder(
      chunkColl, db.failoverStrategy, Some(selectorOpts), Some(sortOpts))

    query.cursor[Array[Byte]](readPreference)
  }

  /**
   * Reads the given file and writes its contents to the given OutputStream.
   *
   * @param file $readFileParam
   */
  def readToOutputStream[Id <: pack.Value](file: ReadFile[Id], out: OutputStream, readPreference: ReadPreference = defaultReadPreference)(implicit ec: ExecutionContext): Future[Unit] = {
    val selectorOpts = chunkSelector(file)
    val sortOpts = document(Seq(elem("n", builder.int(1))))
    val query = new QueryBuilder(
      chunkColl, db.failoverStrategy, Some(selectorOpts), Some(sortOpts))

    implicit def r: pack.Reader[pack.Document] = pack.IdentityReader

    val cursor = query.cursor[pack.Document](readPreference)

    @inline def pushChunk(doc: pack.Document): Cursor.State[Unit] =
      decoder.binary(doc, "data") match {
        case Some(array) =>
          Cursor.Cont(out write array)

        case _ => {
          val errmsg = s"not a chunk! failed assertion: data field is missing: ${pack pretty doc}"

          logger.error(errmsg)
          Cursor.Fail(ReactiveMongoException(errmsg))
        }
      }

    cursor.foldWhile({})((_, doc) => pushChunk(doc), Cursor.FailOnError())
  }

  /** Writes the data provided by the given InputStream to the given file. */
  def writeFromInputStream[Id <: pack.Value](file: FileToSave[pack.type, Id], input: InputStream, chunkSize: Int = 262144)(implicit ec: ExecutionContext): Future[ReadFile[Id]] = {
    type M = MessageDigest

    lazy val digestInit = MessageDigest.getInstance("MD5")

    def digestUpdate(md: MessageDigest, chunk: Array[Byte]) = { md.update(chunk); md }

    def digestFinalize(md: MessageDigest) = Future(md.digest()).map(Some(_))

    case class Chunk(
      previous: Array[Byte],
      n: Int,
      md: M,
      length: Int) {
      def feed(chunk: Array[Byte]): Future[Chunk] = {
        val wholeChunk = self.concat(previous, chunk)

        val normalizedChunkNumber = wholeChunk.length / chunkSize

        logger.debug(s"wholeChunk size is ${wholeChunk.length} => ${normalizedChunkNumber}")

        val zipped =
          for (i <- 0 until normalizedChunkNumber)
            yield Arrays.copyOfRange(
            wholeChunk, i * chunkSize, (i + 1) * chunkSize) -> i

        val left = Arrays.copyOfRange(
          wholeChunk, normalizedChunkNumber * chunkSize, wholeChunk.length)

        Future.traverse(zipped) { ci =>
          writeChunk(n + ci._2, ci._1)
        }.map { _ =>
          logger.debug("all futures for the last given chunk are redeemed.")
          Chunk(
            if (left.isEmpty) Array.empty else left,
            n + normalizedChunkNumber,
            digestUpdate(md, chunk),
            length + chunk.length)
        }
      }

      import reactivemongo.util

      @inline def finish(): Future[ReadFile[Id]] =
        digestFinalize(md).map(_.map(util.hex2Str)).flatMap { md5Hex =>
          finalizeFile[Id](file, previous, n, chunkSize, length.toLong, md5Hex)
        }

      @inline def writeChunk(n: Int, bytes: Array[Byte]) =
        self.writeChunk(file.id, n, bytes)
    }

    val buffer = Array.ofDim[Byte](chunkSize)

    def go(previous: Chunk): Future[Chunk] =
      Future(input read buffer).flatMap {
        case n if n > 0 => {
          logger.debug(s"Processing new chunk from n=${previous.n}...\n")

          previous.feed(buffer take n).flatMap(go)
        }

        case _ =>
          Future.successful(previous)

      }

    go(Chunk(Array.empty, 0, digestInit, 0)).flatMap(_.finish)
  }

  /**
   * Removes a file from this store.
   * Note that if the file does not actually exist,
   * the returned future will not be hold an error.
   *
   * @param file the file entry to remove from this store
   */
  @inline def remove[Id <: pack.Value](file: BasicMetadata[Id])(implicit ec: ExecutionContext): Future[WriteResult] = remove(file.id)

  /**
   * Removes a file from this store.
   * Note that if the file does not actually exist,
   * the returned future will not be hold an error.
   *
   * @param id the file id to remove from this store
   */
  def remove[Id <: pack.Value](id: Id)(implicit ec: ExecutionContext): Future[WriteResult] = {
    import DeleteCommand.{ Delete, DeleteElement }

    implicit def resultReader = deleteReader

    val deleteChunkCmd = Delete(
      Seq(DeleteElement(
        q = document(Seq(elem("files_id", id))), 1, None)),
      ordered = false,
      writeConcern = defaultWriteConcern)

    val deleteFileCmd = Delete(
      Seq(DeleteElement(
        q = document(Seq(elem("_id", id))), 1, None)),
      ordered = false,
      writeConcern = defaultWriteConcern)

    for {
      _ <- runner(chunkColl, deleteChunkCmd, defaultReadPreference)
      r <- runner(fileColl, deleteFileCmd, defaultReadPreference)
    } yield r
  }

  /**
   * Creates the needed indexes on the GridFS collections
   * (`chunks` and `files`).
   *
   * Please note that you should really consider reading
   * [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this,
   * especially in production.
   *
   * @return A future containing true if the index was created, false if it already exists.
   */
  def ensureIndex()(implicit ec: ExecutionContext): Future[Boolean] = {
    val indexMngr = db.indexesManager

    for {
      _ <- create(chunkColl)
      c <- indexMngr.onCollection(chunkColl.name).ensure(Index(pack)(
        key = List("files_id" -> Ascending, "n" -> Ascending),
        name = None,
        unique = true,
        background = false,
        dropDups = false,
        sparse = false,
        expireAfterSeconds = None,
        storageEngine = None,
        weights = None,
        defaultLanguage = None,
        languageOverride = None,
        textIndexVersion = None,
        sphereIndexVersion = None,
        bits = None,
        min = None,
        max = None,
        bucketSize = None,
        collation = None,
        wildcardProjection = None,
        version = None, // let MongoDB decide
        partialFilter = None,
        options = builder.document(Seq.empty)))

      _ <- create(fileColl)
      f <- indexMngr.onCollection(fileColl.name).ensure(Index(pack)(
        key = List("filename" -> Ascending, "uploadDate" -> Ascending),
        name = None,
        unique = false,
        background = false,
        dropDups = false,
        sparse = false,
        expireAfterSeconds = None,
        storageEngine = None,
        weights = None,
        defaultLanguage = None,
        languageOverride = None,
        textIndexVersion = None,
        sphereIndexVersion = None,
        bits = None,
        min = None,
        max = None,
        bucketSize = None,
        collation = None,
        wildcardProjection = None,
        version = None, // let MongoDB decide
        partialFilter = None,
        options = builder.document(Seq.empty)))
    } yield (c && f)
  }

  /**
   * Returns whether the data related to this GridFS instance
   * exists on the database.
   */
  def exists(implicit ec: ExecutionContext): Future[Boolean] = (for {
    _ <- stats(chunkColl).filter { s => s.size > 0 || s.nindexes > 0 }
    _ <- stats(fileColl).filter { s => s.size > 0 || s.nindexes > 0 }
  } yield true).recover {
    case _ => false
  }

  // Dependent factories

  /**
   * Prepare the information to save a file.
   * The unique ID is automatically generated.
   */
  def fileToSave(
    _filename: Option[String] = None,
    _contentType: Option[String] = None,
    _uploadDate: Option[Long] = None,
    _metadata: pack.Document = document(Seq.empty)): FileToSave[pack.type, pack.Value] =
    new FileToSave[pack.type, pack.Value] {
      val pack: self.pack.type = self.pack
      val filename = _filename
      val contentType = _contentType
      val uploadDate = _uploadDate
      val metadata = _metadata
      val id = builder.generateObjectId()
    }

  /** Prepare the information to save a file. */
  def fileToSave[Id <: pack.Value](
    _filename: Option[String],
    _contentType: Option[String],
    _uploadDate: Option[Long],
    _metadata: pack.Document,
    _id: Id): FileToSave[pack.type, Id] =
    new FileToSave[pack.type, Id] {
      val pack: self.pack.type = self.pack
      val filename = _filename
      val contentType = _contentType
      val uploadDate = _uploadDate
      val metadata = _metadata
      val id = _id
    }

  // ---

  private[reactivemongo] def writeChunk(
    id: pack.Value,
    n: Int,
    bytes: Array[Byte])(implicit ec: ExecutionContext): Future[WriteResult] = {
    logger.debug(s"Writing chunk #$n @ file $id")

    val chunkDoc = document(Seq(
      elem("files_id", id),
      elem("n", builder.int(n)),
      elem("data", builder.binary(bytes))))

    val insertChunkCmd = InsertCommand.Insert(
      chunkDoc,
      Seq.empty[pack.Document],
      ordered = false,
      writeConcern = defaultWriteConcern)

    implicit def resultReader = insertReader

    runner(chunkColl, insertChunkCmd, defaultReadPreference)
  }

  private[reactivemongo] def finalizeFile[Id <: pack.Value](
    file: FileToSave[pack.type, Id],
    previous: Array[Byte],
    n: Int,
    chunkSize: Int,
    length: Long,
    md5Hex: Option[String])(implicit ec: ExecutionContext): Future[ReadFile[Id]] = {

    logger.debug(s"Writing last chunk #$n @ file ${file.id}")

    val uploadDate = file.uploadDate.getOrElse(System.nanoTime() / 1000000)
    val fileProps = Seq.newBuilder[pack.ElementProducer]

    fileProps ++= Seq(
      elem("_id", file.id),
      elem("chunkSize", builder.int(chunkSize)),
      elem("length", builder.long(length)),
      elem("uploadDate", builder.dateTime(uploadDate)),
      elem("metadata", file.metadata))

    file.filename.foreach { n =>
      fileProps += elem("filename", builder.string(n))
    }

    file.contentType.foreach { t =>
      fileProps += elem("contentType", builder.string(t))
    }

    md5Hex.foreach { hex =>
      fileProps += elem("md5", builder.string(hex))
    }

    for {
      _ <- writeChunk(file.id, n, previous)

      res <- {
        val fileDoc = document(fileProps.result())

        val insertFileCmd = InsertCommand.Insert(
          fileDoc,
          Seq.empty[pack.Document],
          ordered = false,
          writeConcern = defaultWriteConcern)

        implicit def resultReader = insertReader

        runner(fileColl, insertFileCmd, defaultReadPreference).map { _ =>
          RF[P, Id](pack)(
            _id = file.id,
            _contentType = file.contentType,
            _filename = file.filename,
            _uploadDate = file.uploadDate,
            _chunkSize = chunkSize,
            _length = length,
            _metadata = file.metadata,
            _md5 = md5Hex)
        }
      }
    } yield res
  }

  @inline private def chunkSelector(file: ReadFile[pack.Value]): pack.Document =
    document(Seq(
      elem("files_id", file.id),
      elem("n", document(Seq(
        elem(f"$$gte", builder.int(0)),
        elem(f"$$lte", builder.long(
          file.length / file.chunkSize + (
            if (file.length % file.chunkSize > 0) 1 else 0))))))))

  private lazy val chunkReader: pack.Reader[Array[Byte]] = {
    val decoder = pack.newDecoder

    pack.reader[Array[Byte]] { doc =>
      decoder.binary(doc, "data").get
    }
  }

  @inline private[reactivemongo] def defaultReadPreference =
    db.defaultReadPreference

  @inline private def defaultWriteConcern = db.connection.options.writeConcern

  // Coll creation

  private lazy val createCollCmd = reactivemongo.api.commands.Create()

  private implicit lazy val unitBoxReader =
    CommandCodecs.unitBoxReader[pack.type](pack)

  private implicit lazy val createWriter =
    reactivemongo.api.commands.CreateCollection.writer[pack.type](pack)

  private def create(coll: Collection)(implicit ec: ExecutionContext) =
    runner.unboxed(coll, createCollCmd, defaultReadPreference).recover {
      case CommandError.Code(48 /* already exists */ ) => ()

      case CommandError.Message(
        "collection already exists") => ()
    }

  // Coll stats

  private lazy val collStatsCmd = new CollStats()

  private implicit lazy val collStatsWriter = CollStats.writer[pack.type](pack)

  private implicit lazy val collStatsReader = CollStats.reader[pack.type](pack)

  @inline private def stats(coll: Collection)(implicit ec: ExecutionContext) =
    runner(coll, collStatsCmd, defaultReadPreference)

  // Insert command

  private object InsertCommand
    extends reactivemongo.api.commands.InsertCommand[pack.type] {
    val pack: self.pack.type = self.pack
  }

  private type InsertCmd = ResolvedCollectionCommand[InsertCommand.Insert]

  implicit private lazy val insertWriter: pack.Writer[InsertCmd] = {
    val underlying = reactivemongo.api.commands.InsertCommand.
      writer(pack)(InsertCommand)(db.session)

    pack.writer[InsertCmd](underlying)
  }

  private lazy val insertReader: pack.Reader[InsertCommand.InsertResult] =
    CommandCodecs.defaultWriteResultReader(pack)

  // Delete command

  private object DeleteCommand
    extends reactivemongo.api.commands.DeleteCommand[self.pack.type] {
    val pack: self.pack.type = self.pack
  }

  private type DeleteCmd = ResolvedCollectionCommand[DeleteCommand.Delete]

  implicit private lazy val deleteWriter: pack.Writer[DeleteCmd] =
    pack.writer(DeleteCommand.serialize)

  private lazy val deleteReader: pack.Reader[DeleteCommand.DeleteResult] =
    CommandCodecs.defaultWriteResultReader(pack)

  // ---

  private final class QueryBuilder(
    override val collection: Collection,
    val failoverStrategy: FailoverStrategy,
    val queryOption: Option[pack.Document],
    val sortOption: Option[pack.Document]) extends GenericQueryBuilder[pack.type] {
    type Self = QueryBuilder
    val pack: self.pack.type = self.pack

    protected lazy val version =
      collection.db.connectionState.metadata.maxWireVersion

    val projectionOption = Option.empty[pack.Document]
    val hintOption = Option.empty[pack.Document]
    val explainFlag = false
    val snapshotFlag = false
    val commentString = Option.empty[String]
    val maxTimeMsOption = Option.empty[Long]

    def options = QueryOpts()

    def copy(queryOption: Option[pack.Document], sortOption: Option[pack.Document], projectionOption: Option[pack.Document], hintOption: Option[pack.Document], explainFlag: Boolean, snapshotFlag: Boolean, commentString: Option[String], options: QueryOpts, failoverStrategy: FailoverStrategy, maxTimeMsOption: Option[Long]) = new QueryBuilder(this.collection, failoverStrategy, queryOption, sortOption)
  }

  // ---

  /* Concats two array - fast way */
  private def concat[T](a1: Array[T], a2: Array[T])(implicit m: Manifest[T]): Array[T] = {
    var i, j = 0
    val result = new Array[T](a1.length + a2.length)
    while (i < a1.length) {
      result(i) = a1(i)
      i = i + 1
    }
    while (j < a2.length) {
      result(i + j) = a2(j)
      j = j + 1
    }
    result
  }

  override def toString: String = s"GridFS(db = ${db.name}, files = ${fileColl.name}, chunks = ${chunkColl.name})"
}

object GridFS {
  import reactivemongo.api.CollectionProducer

  def apply[P <: SerializationPack with Singleton](
    _pack: P,
    db: DB with DBMetaCommands,
    prefix: String): GridFS[P] = {
    def _prefix = prefix
    def _db = db

    new GridFS[P] {
      val db = _db
      val prefix = _prefix
      val pack: P = _pack
    }
  }

  def apply(
    db: DB with DBMetaCommands,
    prefix: String): GridFS[Serialization.Pack] =
    apply[Serialization.Pack](
      Serialization.internalSerializationPack, db, prefix)

  def apply(db: DB with DBMetaCommands): GridFS[Serialization.Pack] =
    apply[Serialization.Pack](
      Serialization.internalSerializationPack, db, prefix = "fs")
}
