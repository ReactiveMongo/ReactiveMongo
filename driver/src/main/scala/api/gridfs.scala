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

import reactivemongo.bson.{
  BSONBinary,
  BSONDateTime,
  BSONDocument,
  BSONDocumentReader,
  BSONDocumentWriter,
  BSONElement,
  BSONInteger,
  BSONLong,
  BSONNumberLike,
  BSONObjectID,
  BSONString,
  BSONValue,
  Producer,
  Subtype
}
import reactivemongo.api.{
  Cursor,
  CursorProducer,
  DB,
  DBMetaCommands,
  BSONSerializationPack,
  ReadPreference,
  SerializationPack
}
import reactivemongo.api.commands.WriteResult
import reactivemongo.util._
import reactivemongo.core.errors.ReactiveMongoException
import reactivemongo.core.netty.ChannelBufferWritableBuffer

import reactivemongo.api.collections.{
  GenericCollection,
  GenericCollectionProducer
}
import reactivemongo.api.collections.bson.BSONCollectionProducer

object `package` {
  private[gridfs] val logger = LazyLogger("reactivemongo.api.gridfs")

  type IdProducer[Id] = Tuple2[String, Id] => Producer[BSONElement]
}

object Implicits { // TODO: Move in a `ReadFile` companion object?
  /** A default `BSONReader` for `ReadFile`. */
  implicit object DefaultReadFileReader extends BSONDocumentReader[ReadFile[BSONSerializationPack.type, BSONValue]] {
    def read(doc: BSONDocument) = DefaultReadFile(
      doc.getAs[BSONValue]("_id").get,
      doc.getAs[BSONString]("contentType").map(_.value),
      doc.getAs[BSONString]("filename").map(_.value),
      doc.getAs[BSONNumberLike]("uploadDate").map(_.toLong),
      doc.getAs[BSONNumberLike]("chunkSize").map(_.toInt).get,
      doc.getAs[BSONNumberLike]("length").map(_.toLong).get,
      doc.getAs[BSONString]("md5").map(_.value),
      doc.getAs[BSONDocument]("metadata").getOrElse(BSONDocument()),
      doc)
  }
}

/** Metadata that cannot be customized. */
trait ComputedMetadata {
  /** Length of the file. */
  def length: Long

  /** Size of the chunks of this file. */
  def chunkSize: Int

  /** MD5 hash of this file. */
  def md5: Option[String]
}

/**
 * Common metadata.
 * @tparam Id Type of the id of this file (generally `BSONObjectID` or `BSONValue`).
 */
trait BasicMetadata[+Id] {
  /** Id of this file. */
  def id: Id

  /** Name of this file. */
  def filename: Option[String]

  /** Date when this file was uploaded. */
  def uploadDate: Option[Long]

  /** Content type of this file. */
  def contentType: Option[String]
}

/** Custom metadata (generic trait) */
trait CustomMetadata[P <: SerializationPack with Singleton] {
  val pack: P

  /** A BSONDocument holding all the metadata that are not standard. */
  def metadata: pack.Document
}

/**
 * A file that will be saved in a GridFS store.
 * @tparam Id Type of the id of this file (generally `BSONObjectID` or `BSONValue`).
 */
trait FileToSave[P <: SerializationPack with Singleton, +Id]
  extends BasicMetadata[Id] with CustomMetadata[P]

/** A BSON implementation of `FileToSave`. */
class DefaultFileToSave private[gridfs] (
  val filename: Option[String] = None,
  val contentType: Option[String] = None,
  val uploadDate: Option[Long] = None,
  val metadata: BSONDocument = BSONDocument.empty,
  val id: BSONValue = BSONObjectID.generate()) extends FileToSave[BSONSerializationPack.type, BSONValue] with Equals {

  val pack = BSONSerializationPack

  def canEqual(that: Any): Boolean = that match {
    case _: DefaultFileToSave => true
    case _                    => false
  }

  def copy(filename: Option[String] = this.filename, contentType: Option[String] = this.contentType, uploadDate: Option[Long] = this.uploadDate, metadata: BSONDocument = this.metadata, id: BSONValue = this.id) = new DefaultFileToSave(filename, contentType, uploadDate, metadata, id)

}

/** Factory of [[DefaultFileToSave]]. */
object DefaultFileToSave {
  def unapply(that: DefaultFileToSave): Option[(Option[String], Option[String], Option[Long], BSONDocument, BSONValue)] = Some((that.filename, that.contentType, that.uploadDate, that.metadata, that.id))

  /** For backward compatibility. */
  sealed trait FileName[T] extends (T => Option[String]) {
    def apply(name: T): Option[String]
  }

  object FileName {
    @deprecated(message = "The filename is now optional, pass it as an `Option[String]`.", since = "0.11.3")
    implicit object StringFileName extends FileName[String] {
      def apply(name: String) = Some(name)
    }

    implicit object OptionalFileName extends FileName[Option[String]] {
      def apply(name: Option[String]) = name
    }

    implicit object SomeFileName extends FileName[Some[String]] {
      def apply(name: Some[String]) = name
    }

    implicit object NoFileName extends FileName[None.type] {
      def apply(name: None.type) = Option.empty[String]
    }
  }

  def apply[N](
    filename: N,
    contentType: Option[String] = None,
    uploadDate: Option[Long] = None,
    metadata: BSONDocument = BSONDocument.empty,
    id: BSONValue = BSONObjectID.generate())(implicit naming: FileName[N]): DefaultFileToSave = new DefaultFileToSave(naming(filename), contentType, uploadDate, metadata, id)

}

/**
 * A file read from a GridFS store.
 * @tparam Id Type of the id of this file (generally `BSONObjectID` or `BSONValue`).
 */
trait ReadFile[P <: SerializationPack with Singleton, +Id] extends BasicMetadata[Id] with CustomMetadata[P] with ComputedMetadata

/** A BSON implementation of `ReadFile`. */
@SerialVersionUID(930238403L)
case class DefaultReadFile(
  id: BSONValue,
  contentType: Option[String],
  filename: Option[String],
  uploadDate: Option[Long],
  chunkSize: Int,
  length: Long,
  md5: Option[String],
  metadata: BSONDocument,
  original: BSONDocument) extends ReadFile[BSONSerializationPack.type, BSONValue] {
  @transient val pack = BSONSerializationPack
}

/**
 * A GridFS store.
 * @param db The database where this store is located.
 * @param prefix The prefix of this store. The `files` and `chunks` collections will be actually named `prefix.files` and `prefix.chunks`.
 */
class GridFS[P <: SerializationPack with Singleton](db: DB with DBMetaCommands, prefix: String = "fs")(implicit producer: GenericCollectionProducer[P, GenericCollection[P]] = BSONCollectionProducer) { self =>
  import reactivemongo.api.indexes.{ Index, IndexType }, IndexType.Ascending

  type ReadFile[Id <: pack.Value] = reactivemongo.api.gridfs.ReadFile[P, Id]

  /** The `files` collection */
  val files = db(prefix + ".files")(producer)

  /** The `chunks` collection */
  val chunks = db(prefix + ".chunks")(producer)

  val pack: files.pack.type = files.pack

  @inline def defaultReadPreference: ReadPreference =
    db.connection.options.readPreference

  /**
   * Finds the files matching the given selector.
   *
   * @param selector The document to select the files to return
   *
   * @tparam S The type of the selector document. An implicit `Writer[S]` must be in the scope.
   */
  def find[S, T <: ReadFile[_]](selector: S)(implicit sWriter: pack.Writer[S], readFileReader: pack.Reader[T], ctx: ExecutionContext, cp: CursorProducer[T]): cp.ProducedCursor = files.find(selector).cursor(defaultReadPreference)

  /**
   * Saves the content provided by the given enumerator with the given metadata.
   *
   * @param enumerator Producer of content.
   * @param file Metadata of the file to store.
   * @param chunkSize Size of the chunks. Defaults to 256kB.
   *
   * @return A future of a ReadFile[Id].
   */
  def save[Id <: pack.Value](enumerator: Enumerator[Array[Byte]], file: FileToSave[pack.type, Id], chunkSize: Int = 262144)(implicit readFileReader: pack.Reader[ReadFile[Id]], ctx: ExecutionContext, idProducer: IdProducer[Id], docWriter: BSONDocumentWriter[file.pack.Document]): Future[ReadFile[Id]] = (enumerator |>>> iteratee(file, chunkSize)).flatMap(f => f)

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
  def saveWithMD5[Id <: pack.Value](enumerator: Enumerator[Array[Byte]], file: FileToSave[pack.type, Id], chunkSize: Int = 262144)(implicit readFileReader: pack.Reader[ReadFile[Id]], ctx: ExecutionContext, idProducer: IdProducer[Id], docWriter: BSONDocumentWriter[file.pack.Document]): Future[ReadFile[Id]] = (enumerator |>>> iterateeWithMD5(file, chunkSize)).flatMap(f => f)

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
  def iteratee[Id <: pack.Value](file: FileToSave[pack.type, Id], chunkSize: Int = 262144)(implicit readFileReader: pack.Reader[ReadFile[Id]], ctx: ExecutionContext, idProducer: IdProducer[Id], docWriter: BSONDocumentWriter[file.pack.Document]): Iteratee[Array[Byte], Future[ReadFile[Id]]] =
    iterateeMaybeMD5[Id, Unit](file, {}, (_: Unit, chunk) => {},
      { _: Unit => Future.successful(Option.empty[Array[Byte]]) }, chunkSize)

  /**
   * Returns an `Iteratee` that will consume data to put into a GridFS store,
   * computing the MD5.
   *
   * @param file the metadata of the file to store.
   * @param chunkSize Size of the chunks. Defaults to 256kB.
   *
   * @tparam Id the type of the id of this file (generally `BSONObjectID` or `BSONValue`).
   */
  @deprecated("May be moved to the separate iteratee module", "0.12.0")
  def iterateeWithMD5[Id <: pack.Value](file: FileToSave[pack.type, Id], chunkSize: Int = 262144)(implicit readFileReader: pack.Reader[ReadFile[Id]], ctx: ExecutionContext, idProducer: IdProducer[Id], docWriter: BSONDocumentWriter[file.pack.Document]): Iteratee[Array[Byte], Future[ReadFile[Id]]] = {
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
  private def iterateeMaybeMD5[Id <: pack.Value, M](file: FileToSave[pack.type, Id], digestInit: => M, digestUpdate: (M, Array[Byte]) => M, digestFinalize: M => Future[Option[Array[Byte]]], chunkSize: Int)(implicit readFileReader: pack.Reader[ReadFile[Id]], ctx: ExecutionContext, idProducer: IdProducer[Id], docWriter: BSONDocumentWriter[file.pack.Document]): Iteratee[Array[Byte], Future[ReadFile[Id]]] = {
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

      import reactivemongo.api.collections.bson.{
        BSONCollection,
        BSONCollectionProducer
      }
      import reactivemongo.bson.utils.Converters

      def finish(): Future[ReadFile[Id]] = {
        logger.debug(s"Writing last chunk #$n")

        val uploadDate = file.uploadDate.getOrElse(System.currentTimeMillis)

        for {
          f <- writeChunk(n, previous)
          md5 <- digestFinalize(md)
          bson = BSONDocument(idProducer("_id" -> file.id)) ++ (
            "filename" -> file.filename.map(BSONString(_)),
            "chunkSize" -> BSONInteger(chunkSize),
            "length" -> BSONLong(length.toLong),
            "uploadDate" -> BSONDateTime(uploadDate),
            "contentType" -> file.contentType.map(BSONString(_)),
            "md5" -> md5.map(Converters.hex2Str),
            "metadata" -> option(!pack.isEmpty(file.metadata), file.metadata))
          res <- files.as[BSONCollection]().insert(bson).map { _ =>
            val buf = ChannelBufferWritableBuffer()
            BSONSerializationPack.writeToBuffer(buf, bson)
            pack.readAndDeserialize(buf.toReadableBuffer, readFileReader)
          }
        } yield res
      }

      def writeChunk(n: Int, array: Array[Byte]) = {
        logger.debug(s"Writing chunk #$n")

        val bson = BSONDocument(
          "files_id" -> file.id,
          "n" -> BSONInteger(n),
          "data" -> BSONBinary(array, Subtype.GenericBinarySubtype))

        chunks.as[BSONCollection]().insert(bson)
      }
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
  def enumerate[Id <: pack.Value](file: ReadFile[Id])(implicit ctx: ExecutionContext, idProducer: IdProducer[Id]): Enumerator[Array[Byte]] = {
    import reactivemongo.api.collections.bson.{
      BSONCollection,
      BSONCollectionProducer
    }

    def selector = BSONDocument(idProducer("files_id" -> file.id)) ++ (
      "n" -> BSONDocument(
        "$gte" -> 0,
        "$lte" -> BSONLong(file.length / file.chunkSize + (
          if (file.length % file.chunkSize > 0) 1 else 0))))

    @inline def cursor = chunks.as[BSONCollection]().find(selector).
      sort(BSONDocument("n" -> 1)).cursor[BSONDocument](defaultReadPreference)

    @inline def pushChunk(chan: Concurrent.Channel[Array[Byte]], doc: BSONDocument): Cursor.State[Unit] = doc.get("data") match {
      case Some(BSONBinary(data, _)) => {
        val array = new Array[Byte](data.readable)
        data.slice(data.readable).readBytes(array)
        Cursor.Cont(chan push array)
      }

      case _ => {
        val errmsg = s"not a chunk! failed assertion: data field is missing: ${BSONDocument pretty doc}"

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
  def readToOutputStream[Id <: pack.Value](file: ReadFile[Id], out: OutputStream)(implicit ctx: ExecutionContext, idProducer: IdProducer[Id]): Future[Unit] = enumerate(file) |>>> Iteratee.foreach { out.write(_) }

  /** Writes the data provided by the given InputStream to the given file. */
  def writeFromInputStream[Id <: pack.Value](file: FileToSave[pack.type, Id], input: InputStream, chunkSize: Int = 262144)(implicit readFileReader: pack.Reader[ReadFile[Id]], ctx: ExecutionContext, idProducer: IdProducer[Id], docWriter: BSONDocumentWriter[file.pack.Document]): Future[ReadFile[Id]] = save(Enumerator.fromStream(input, chunkSize), file)

  /**
   * Removes a file from this store.
   * Note that if the file does not actually exist, the returned future will not be hold an error.
   *
   * @param file The file entry to remove from this store.
   */
  def remove[Id <: pack.Value](file: BasicMetadata[Id])(implicit ctx: ExecutionContext, idProducer: IdProducer[Id]): Future[WriteResult] = remove(file.id)

  /**
   * Removes a file from this store.
   * Note that if the file does not actually exist, the returned future will not be hold an error.
   *
   * @param id The file id to remove from this store.
   */
  def remove[Id <: pack.Value](id: Id)(implicit ctx: ExecutionContext, idProducer: IdProducer[Id]): Future[WriteResult] = {
    import reactivemongo.api.collections.bson.{
      BSONCollection,
      BSONCollectionProducer
    }

    chunks.as[BSONCollection]().
      remove(BSONDocument(idProducer("files_id" -> id))).flatMap { _ =>
        files.as[BSONCollection]().remove(BSONDocument(idProducer("_id" -> id)))
      }
  }

  /**
   * Creates the needed index on the `chunks` collection, if none.
   *
   * Please note that you should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @return A future containing true if the index was created, false if it already exists.
   */
  def ensureIndex()(implicit ctx: ExecutionContext): Future[Boolean] =
    db.indexesManager.onCollection(prefix + ".chunks").ensure(Index(List("files_id" -> Ascending, "n" -> Ascending), unique = true))
}

object GridFS {
  def apply[P <: SerializationPack with Singleton](db: DB with DBMetaCommands, prefix: String = "fs")(implicit producer: GenericCollectionProducer[P, GenericCollection[P]] = BSONCollectionProducer) = new GridFS(db, prefix)(producer)
}
