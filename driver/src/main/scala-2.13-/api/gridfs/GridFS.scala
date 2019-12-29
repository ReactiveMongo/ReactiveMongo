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
import scala.concurrent.{ ExecutionContext, Future }

import play.api.libs.iteratee.{ Concurrent, Enumerator, Iteratee }

import reactivemongo.bson.BSONDocumentWriter

import reactivemongo.api.{
  Cursor,
  CursorProducer,
  DB,
  DBMetaCommands,
  ReadPreference,
  Serialization,
  SerializationPack
}
import reactivemongo.api.commands.WriteResult

import reactivemongo.core.errors.ReactiveMongoException

import reactivemongo.api.gridfs.{ ReadFile => RF }

import reactivemongo.api.collections.{
  GenericCollection,
  GenericCollectionProducer
}
import reactivemongo.api.collections.bson.BSONCollectionProducer

import com.github.ghik.silencer.silent

/**
 * A GridFS store.
 * @param db The database where this store is located.
 * @param prefix The prefix of this store. The `files` and `chunks` collections will be actually named `\${prefix}.files` and `\${prefix}.chunks`.
 */
abstract class GridFS[P <: SerializationPack with Singleton] @deprecated("Internal: will be made private", "0.19.0") (
  db: DB with DBMetaCommands,
  prefix: String = "fs")(implicit producer: GenericCollectionProducer[P, GenericCollection[P]] = BSONCollectionProducer) { self =>
  import reactivemongo.api.indexes.{ Index, IndexType }, IndexType.Ascending

  @deprecated("Internal: will be made private", "0.19.0")
  lazy val pack: P = {
    @silent def eval: P = throw new UnsupportedOperationException(
      "Use `GridFS(..)` to create instance")

    eval
  }

  /** The `files` collection */
  lazy val files: GenericCollection[pack.type] = {
    @silent def eval: GenericCollection[pack.type] =
      throw new UnsupportedOperationException(
        "Use `GridFS(..)` to create instance")

    eval
  }

  /** The `chunks` collection */
  lazy val chunks: GenericCollection[pack.type] = {
    @silent def eval: GenericCollection[pack.type] =
      throw new UnsupportedOperationException(
        "Use `GridFS(..)` to create instance")

    eval
  }

  private val builder = pack.newBuilder
  private val decoder = pack.newDecoder

  import builder.{ document, elementProducer => elem }

  type ReadFile[Id <: P#Value] = RF[P, Id]

  @inline def defaultReadPreference: ReadPreference =
    db.connection.options.readPreference

  private implicit val docW: pack.Writer[pack.Document] = pack.IdentityWriter

  private lazy val chunkReader = {
    val decoder = pack.newDecoder

    pack.reader[Array[Byte]] { doc =>
      decoder.binary(doc, "data").get
    }
  }

  /**
   * Returns a cursor for the chunks of the specified file.
   * The cursor walks the chunks orderly.
   */
  @silent(".*(ec|readPreference)\\ .*is\\ never\\ used.*")
  def chunks(file: ReadFile[pack.Value], readPreference: ReadPreference = defaultReadPreference)(implicit ec: ExecutionContext, cp: CursorProducer[Array[Byte]]): cp.ProducedCursor = {
    val selector = document(Seq(
      elem("files_id", file.id),
      elem("n", document(Seq(
        elem(f"$$gte", builder.int(0)),
        elem(f"$$lte", builder.long(file.length / file.chunkSize + (
          if (file.length % file.chunkSize > 0) 1 else 0))))))))

    val sortOpts = document(Seq(elem("n", builder.int(1))))
    implicit def reader = chunkReader

    self.chunks.find(selector, Option.empty[pack.Document]).
      sort(sortOpts).cursor(defaultReadPreference)
  }

  /**
   * Finds the files matching the given selector.
   *
   * @param selector The document to select the files to return
   *
   * @tparam S The type of the selector document. An implicit `Writer[S]` must be in the scope.
   */
  @silent(".*ec\\ .*is\\ never\\ used.*")
  def find[S, T <: ReadFile[_]](selector: S)(implicit sWriter: pack.Writer[S], readFileReader: pack.Reader[T], @deprecatedName(Symbol("ctx")) ec: ExecutionContext, cp: CursorProducer[T]): cp.ProducedCursor = files.find(selector, Option.empty[pack.Document]).cursor(defaultReadPreference)

  @silent(".*ec\\ .*is\\ never\\ used.*")
  @inline def find(selector: pack.Document)(implicit ec: ExecutionContext, cp: CursorProducer[ReadFile[pack.Value]]): cp.ProducedCursor = {
    implicit def idTag = pack.IsValue
    implicit def readFileReader = RF.reader[P, pack.Value](pack)

    files.find(selector, Option.empty[pack.Document]).
      cursor(defaultReadPreference)
  }

  /**
   * Saves the content provided by the given enumerator with the given metadata.
   *
   * @param enumerator Producer of content.
   * @param file Metadata of the file to store.
   * @param chunkSize Size of the chunks. Defaults to 256kB.
   *
   * @return A future of a ReadFile[Id].
   */
  @deprecated("Will be moved to `reactivemongo.play.iteratees.GridFS`", "0.17.0")
  def save[Id <: pack.Value](enumerator: Enumerator[Array[Byte]], file: FileToSave[pack.type, Id], chunkSize: Int = 262144)(implicit readFileReader: pack.Reader[ReadFile[Id]], @deprecatedName(Symbol("ctx")) ec: ExecutionContext, @deprecated("Unused", "0.19.0") idProducer: IdProducer[Id], @deprecated("Unused", "0.19.0") docWriter: BSONDocumentWriter[file.pack.Document]): Future[ReadFile[Id]] = (enumerator |>>> iteratee(file, chunkSize)).flatMap(f => f)

  /**
   * Saves the content provided by the given enumerator with the given metadata,
   * with the MD5 computed.
   *
   * @param enumerator Producer of content.
   * @param file Metadata of the file to store.
   * @param chunkSize Size of the chunks. Defaults to 256kB.
   *
   * @return A future of a ReadFile[Id].
   */
  @deprecated("Will be moved to `reactivemongo.play.iteratees.GridFS`", "0.17.0")
  def saveWithMD5[Id <: pack.Value](enumerator: Enumerator[Array[Byte]], file: FileToSave[pack.type, Id], chunkSize: Int = 262144)(implicit readFileReader: pack.Reader[ReadFile[Id]], @deprecatedName('ctx) ec: ExecutionContext, @deprecated("Unused", "0.19.0") idProducer: IdProducer[Id], docWriter: BSONDocumentWriter[file.pack.Document]): Future[ReadFile[Id]] = (enumerator |>>> iterateeWithMD5(file, chunkSize)).flatMap(f => f)

  /** Concats two array - fast way */
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

  /**
   * Returns an `Iteratee` that will consume data to put into a GridFS store.
   *
   * @param file the metadata of the file to store.
   * @param chunkSize Size of the chunks. Defaults to 256kB.
   *
   * @tparam Id the type of the id of this file (generally `BSONObjectID` or `BSONValue`).
   */
  @deprecated("Use [[iterateeWithMD5]]", "0.12.0")
  def iteratee[Id <: pack.Value](file: FileToSave[pack.type, Id], chunkSize: Int = 262144)(implicit readFileReader: pack.Reader[ReadFile[Id]], @deprecatedName('ctx) ec: ExecutionContext, @deprecated("Unused", "0.19.0") idProducer: IdProducer[Id], @deprecated("Unused", "0.19.0") docWriter: BSONDocumentWriter[file.pack.Document]): Iteratee[Array[Byte], Future[ReadFile[Id]]] = {
    iterateeMaybeMD5[Id, Unit](file, {}, (_: Unit, _) => {},
      { _: Unit => Future.successful(Option.empty[Array[Byte]]) }, chunkSize)
  }

  /**
   * Returns an `Iteratee` that will consume data to put into a GridFS store,
   * computing the MD5.
   *
   * @param file the metadata of the file to store.
   * @param chunkSize Size of the chunks. Defaults to 256kB.
   *
   * @tparam Id the type of the id of this file (generally `BSONObjectID` or `BSONValue`).
   */
  @deprecated("Will be moved to `reactivemongo.play.iteratees.GridFS`", "0.12.0")
  def iterateeWithMD5[Id <: pack.Value](file: FileToSave[pack.type, Id], chunkSize: Int = 262144)(implicit readFileReader: pack.Reader[ReadFile[Id]], @deprecatedName(Symbol("ctx")) ec: ExecutionContext, @deprecated("Unused", "0.19.0") idProducer: IdProducer[Id], @deprecated("Unused", "0.19.0") docWriter: BSONDocumentWriter[file.pack.Document]): Iteratee[Array[Byte], Future[ReadFile[Id]]] = {
    import java.security.MessageDigest

    iterateeMaybeMD5[Id, MessageDigest](file, MessageDigest.getInstance("MD5"),
      { (md: MessageDigest, chunk) => md.update(chunk); md },
      { md: MessageDigest => Future(md.digest()).map(Some(_)) },
      chunkSize)
  }

  /**
   * Returns an `Iteratee` that will consume data to put into a GridFS store.
   *
   * @param file the metadata of the file to store.
   * @param digestInit the factory for the message digest
   * @param digestUpdate the function to update the digest
   * @param digestFinalize the function to finalize the digest
   * @param chunkSize Size of the chunks. Defaults to 256kB.
   *
   * @tparam Id the type of the id of this file (generally `BSONObjectID` or `BSONValue`).
   * @tparam M the type of the message digest
   */
  @silent(".*readFileReader\\ .*is\\ never\\ used.*")
  private def iterateeMaybeMD5[Id <: pack.Value, M](file: FileToSave[pack.type, Id], digestInit: => M, digestUpdate: (M, Array[Byte]) => M, digestFinalize: M => Future[Option[Array[Byte]]], chunkSize: Int)(implicit readFileReader: pack.Reader[ReadFile[Id]], @deprecatedName(Symbol("ctx")) ec: ExecutionContext, @deprecated("Unused", "0.19.0") idProducer: IdProducer[Id], @deprecated("Unused", "0.19.0") docWriter: BSONDocumentWriter[file.pack.Document]): Iteratee[Array[Byte], Future[ReadFile[Id]]] = {
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

    Iteratee.foldM(Chunk(Array.empty, 0, digestInit, 0)) {
      (previous, chunk: Array[Byte]) =>
        logger.debug(s"Processing new enumerated chunk from n=${previous.n}...\n")
        previous.feed(chunk)
    }.map(_.finish)
  }

  /**
   * Produces an enumerator of chunks of bytes from the `chunks` collection
   * matching the given file metadata.
   *
   * @param file the file to be read
   */
  @deprecated("Will be moved to `reactivemongo.play.iteratees.GridFS`", "0.17.0")
  def enumerate[Id <: pack.Value](file: ReadFile[Id])(implicit @deprecatedName('ctx) ec: ExecutionContext, @deprecated("Unused", "0.19.0") idProducer: IdProducer[Id]): Enumerator[Array[Byte]] = {
    val selectorOpts: pack.Document = builder.document(Seq(
      elem("files_id", file.id),
      elem("n", builder.document(Seq(
        elem(f"$$gte", builder.int(0)),
        elem(f"$$lte", builder.long(
          file.length / file.chunkSize + (
            if (file.length % file.chunkSize > 0) 1 else 0))))))))

    val sortOpts = builder.document(Seq(elem("n", builder.int(1))))
    val query = chunks.find(selectorOpts).sort(sortOpts)

    implicit def r: query.pack.Reader[pack.Document] = pack.IdentityReader

    val cursor = query.cursor[pack.Document](defaultReadPreference)

    @inline def pushChunk(chan: Concurrent.Channel[Array[Byte]], doc: pack.Document): Cursor.State[Unit] = decoder.binary(doc, "data") match {
      case Some(array) =>
        Cursor.Cont(chan push array)

      case _ => {
        val errmsg = s"not a chunk! failed assertion: data field is missing: ${pack pretty doc}"

        logger.error(errmsg)
        Cursor.Fail(ReactiveMongoException(errmsg))
      }
    }

    Concurrent.unicast[Array[Byte]] { chan =>
      cursor.foldWhile({})(
        (_, doc) => pushChunk(chan, doc),
        Cursor.FailOnError()).onComplete { case _ => chan.eofAndEnd() }
    }
  }

  /** Reads the given file and writes its contents to the given OutputStream */
  @silent(".*(IdProducer|enumerate).*")
  def readToOutputStream[Id <: pack.Value](file: ReadFile[Id], out: OutputStream)(implicit ec: ExecutionContext): Future[Unit] = {
    implicit val unused = null.asInstanceOf[IdProducer[Id]]

    enumerate(file) |>>> Iteratee.foreach { out.write(_) }
  }

  /** Writes the data provided by the given InputStream to the given file. */
  @silent(".*(idProducer|save).*")
  def writeFromInputStream[Id <: pack.Value](file: FileToSave[pack.type, Id], input: InputStream, chunkSize: Int = 262144)(implicit readFileReader: pack.Reader[ReadFile[Id]], @deprecatedName(Symbol("ctx")) ec: ExecutionContext, @deprecated("Unused", "0.19.0") idProducer: IdProducer[Id], docWriter: BSONDocumentWriter[pack.Document]): Future[ReadFile[Id]] = save(Enumerator.fromStream(input, chunkSize), file)

  /**
   * Removes a file from this store.
   * Note that if the file does not actually exist,
   * the returned future will not be hold an error.
   *
   * @param file the file entry to remove from this store
   */
  def remove[Id <: pack.Value](file: BasicMetadata[Id])(implicit @deprecatedName(Symbol("ctx")) ec: ExecutionContext, @deprecated("Unused", "0.19.0") idProducer: IdProducer[Id]): Future[WriteResult] = remove(file.id)

  /**
   * Removes a file from this store.
   * Note that if the file does not actually exist,
   * the returned future will not be hold an error.
   *
   * @param id the file id to remove from this store
   */
  @inline def remove(id: pack.Value)(implicit ec: ExecutionContext): Future[WriteResult] = {
    def chunkSelector = builder.document(Seq(elem("files_id", id)))
    def fileSelector = builder.document(Seq(elem("_id", id)))

    for {
      _ <- chunks.delete.one(chunkSelector)
      r <- files.delete.one(fileSelector)
    } yield r
  }

  /**
   * Creates the needed indexes on the GridFS collections
   * (`chunks` and `files`).
   *
   * Please note that you should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @return A future containing true if the index was created, false if it already exists.
   */
  def ensureIndex()(implicit @deprecatedName(Symbol("ctx")) ec: ExecutionContext): Future[Boolean] = for {
    _ <- chunks.create(failsIfExists = false)
    c <- chunks.indexesManager.ensure(Index(pack)(
      key = List("files_id" -> Ascending, "n" -> Ascending),
      name = None,
      unique = true,
      background = false,
      dropDups = false,
      sparse = false,
      version = None,
      partialFilter = None,
      options = builder.document(Seq.empty)))

    _ <- files.create(failsIfExists = false)
    f <- files.indexesManager.ensure(Index(pack)(
      key = List("filename" -> Ascending, "uploadDate" -> Ascending),
      name = None,
      unique = false,
      background = false,
      dropDups = false,
      sparse = false,
      version = None,
      partialFilter = None,
      options = builder.document(Seq.empty)))

  } yield (c && f)

  /**
   * Returns whether the data related to this GridFS instance
   * exists on the database.
   */
  def exists(implicit ec: ExecutionContext): Future[Boolean] = (for {
    _ <- chunks.stats().filter { s => s.size > 0 || s.nindexes > 0 }
    _ <- files.stats().filter { s => s.size > 0 || s.nindexes > 0 }
  } yield true).recover {
    case _ => false
  }

  private[reactivemongo] def writeChunk(
    id: pack.Value,
    n: Int,
    bytes: Array[Byte])(implicit ec: ExecutionContext): Future[WriteResult] = {
    logger.debug(s"Writing chunk #$n @ file $id")

    val doc = builder.document(Seq(
      elem("files_id", id),
      elem("n", builder.int(n)),
      elem("data", builder.binary(bytes))))

    chunks.insert.one(doc)
  }

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

  private[reactivemongo] def finalizeFile[Id <: pack.Value](
    file: FileToSave[pack.type, Id],
    previous: Array[Byte],
    n: Int,
    chunkSize: Int,
    length: Long,
    md5Hex: Option[String])(implicit ec: ExecutionContext): Future[ReadFile[Id]] = {
    val uploadDate = file.uploadDate.getOrElse(System.nanoTime() / 1000000)
    val fileProps = Seq.newBuilder[pack.ElementProducer]
    val len = length.toLong

    fileProps ++= Seq(
      elem("_id", file.id),
      elem("chunkSize", builder.int(chunkSize)),
      elem("length", builder.long(len)),
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
        val doc = builder.document(fileProps.result())

        files.insert.one(doc).map { _ =>
          def fchunkSize = chunkSize
          def flength = length

          new ReadFile[Id] {
            @transient val pack: self.pack.type = self.pack

            val id = file.id
            val contentType = file.contentType
            val filename = file.filename
            val uploadDate = file.uploadDate
            val chunkSize = fchunkSize
            val length = flength
            val md5 = md5Hex
            val metadata = file.metadata

            override def equals(that: Any): Boolean = that match {
              case other: ReadFile[_] =>
                this.tupled == other.tupled

              case _ =>
                false
            }

            override def hashCode: Int = tupled.hashCode

            override def toString: String = s"ReadFile${tupled.toString}"
          }
        }
      }
    } yield res
  }

  override def toString: String = s"GridFS(db = ${db.name}, files = ${files.name}, chunks = ${chunks.name})"
}

object GridFS extends LowPriorityGridFS {
  def apply[P <: SerializationPack with Singleton](
    _pack: P,
    db: DB with DBMetaCommands,
    prefix: String)(implicit producer: GenericCollectionProducer[P, GenericCollection[P]]): GridFS[P] =
    new GridFS(db, prefix)(producer) {
      override lazy val pack: P = _pack

      override lazy val files = db(prefix + ".files")(producer).
        asInstanceOf[GenericCollection[pack.type]]

      override lazy val chunks = db(prefix + ".chunks")(producer).
        asInstanceOf[GenericCollection[pack.type]]
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

private[gridfs] sealed trait LowPriorityGridFS {
  @deprecated("Use `GridFS(_pack, db, prefix)`", "0.19.0")
  def apply[P <: SerializationPack with Singleton](db: DB with DBMetaCommands, prefix: String = "fs")(implicit producer: GenericCollectionProducer[P, GenericCollection[P]] = BSONCollectionProducer): GridFS[P] = new GridFS(db, prefix)(producer) {
    override lazy val pack = producer.pack

    override lazy val files = db(prefix + ".files")(producer).
      asInstanceOf[GenericCollection[pack.type]]

    override lazy val chunks = db(prefix + ".chunks")(producer).
      asInstanceOf[GenericCollection[pack.type]]
  }
}
