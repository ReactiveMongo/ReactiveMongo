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

import play.api.libs.iteratee.{ Enumeratee, Enumerator, Iteratee }

import reactivemongo.bson._
import reactivemongo.api.{
  CursorProducer, DB, DBMetaCommands, BSONSerializationPack, SerializationPack
}
import reactivemongo.api.commands.WriteResult
import reactivemongo.utils._
import reactivemongo.core.netty.ChannelBufferWritableBuffer

import reactivemongo.api.collections.{
  GenericCollection, GenericCollectionProducer
}
import reactivemongo.api.collections.bson.BSONCollectionProducer

object `package` {
  private[gridfs] val logger = LazyLogger("reactivemongo.api.gridfs")

  type IdProducer[Id] = Tuple2[String, Id] => Producer[BSONElement]
}

object Implicits { // TODO: Move in a `ReadFile` companion object?
  /** A default `BSONReader` for `ReadFile`. */
  implicit object DefaultReadFileReader extends BSONDocumentReader[ReadFile[BSONSerializationPack.type, BSONValue]] {
    import DefaultBSONHandlers._

    def read(doc: BSONDocument) = DefaultReadFile(
      doc.getAs[BSONValue]("_id").get,
      doc.getAs[BSONString]("contentType").map(_.value),
      doc.getAs[BSONString]("filename").map(_.value).get,
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
  def filename: String

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
trait FileToSave[P <: SerializationPack with Singleton, +Id] extends BasicMetadata[Id] with CustomMetadata[P]

/** A BSON implementation of `FileToSave`. */
case class DefaultFileToSave(
  filename: String,
  contentType: Option[String] = None,
  uploadDate: Option[Long] = None,
  metadata: BSONDocument = BSONDocument(),
  id: BSONValue = BSONObjectID.generate)
    extends FileToSave[BSONSerializationPack.type, BSONValue] {
  val pack = BSONSerializationPack
}

/**
 * A file read from a GridFS store.
 * @tparam Id Type of the id of this file (generally `BSONObjectID` or `BSONValue`).
 */
trait ReadFile[P <: SerializationPack with Singleton, +Id] extends BasicMetadata[Id] with CustomMetadata[P] with ComputedMetadata

/** A BSON implementation of `ReadFile`. */
case class DefaultReadFile(
  id: BSONValue,
  contentType: Option[String],
  filename: String,
  uploadDate: Option[Long],
  chunkSize: Int,
  length: Long,
  md5: Option[String],
  metadata: BSONDocument,
  original: BSONDocument) extends ReadFile[BSONSerializationPack.type, BSONValue] {
  val pack = BSONSerializationPack
}

/**
 * A GridFS store.
 * @param db The database where this store is located.
 * @param prefix The prefix of this store. The `files` and `chunks` collections will be actually named `prefix.files` and `prefix.chunks`.
 */
class GridFS[P <: SerializationPack with Singleton](db: DB with DBMetaCommands, prefix: String = "fs")(implicit producer: GenericCollectionProducer[P, GenericCollection[P]] = BSONCollectionProducer) {
  import reactivemongo.api.indexes.Index
  import reactivemongo.api.indexes.IndexType.Ascending

  /** The `files` collection */
  val files = db(prefix + ".files")(producer)

  /** The `chunks` collection */
  val chunks = db(prefix + ".chunks")(producer)

  val pack: files.pack.type = files.pack

  /**
   * Finds the files matching the given selector.
   *
   * @param selector The document to select the files to return
   *
   * @tparam S The type of the selector document. An implicit `Writer[S]` must be in the scope.
   */
  def find[S, T <: ReadFile[P, _]](selector: S)(implicit sWriter: pack.Writer[S], readFileReader: pack.Reader[T], ctx: ExecutionContext, cp: CursorProducer[T]): cp.ProducedCursor = files.find(selector).cursor

  /**
   * Saves the content provided by the given enumerator with the given metadata.
   *
   * @param enumerator Producer of content.
   * @param file Metadata of the file to store.
   * @param chunkSize Size of the chunks. Defaults to 256kB.
   *
   * @return A future of a ReadFile[Id].
   */
  def save[Id <: pack.Value](enumerator: Enumerator[Array[Byte]], file: FileToSave[pack.type, Id], chunkSize: Int = 262144)(implicit readFileReader: pack.Reader[ReadFile[P, Id]], ctx: ExecutionContext, idProducer: IdProducer[Id], docWriter: BSONDocumentWriter[file.pack.Document]): Future[ReadFile[P, Id]] = (enumerator |>>> iteratee(file, chunkSize)).flatMap(f => f)

  /**
   * Gets an `Iteratee` that will consume data to put into a GridFS store.
   * @param file Metadata of the file to store.
   * @param chunkSize Size of the chunks. Defaults to 256kB.
   *
   * @tparam Id Type of the id of this file (generally `BSONObjectID` or `BSONValue`).
   *
   * @return An `Iteratee` that will consume data to put into a GridFS store.
   */
  def iteratee[Id <: pack.Value](file: FileToSave[pack.type, Id], chunkSize: Int = 262144)(implicit readFileReader: pack.Reader[ReadFile[P, Id]], ctx: ExecutionContext, idProducer: IdProducer[Id], docWriter: BSONDocumentWriter[file.pack.Document]): Iteratee[Array[Byte], Future[ReadFile[P, Id]]] = {
    implicit val ec = db.connection.actorSystem
    import java.security.MessageDigest

    case class Chunk(
      previous: Array[Byte] = new Array(0),
      n: Int = 0,
      md: MessageDigest = MessageDigest.getInstance("MD5"),
      length: Int = 0) {

      def feed(chunk: Array[Byte]): Future[Chunk] = {
        val wholeChunk = concat(previous, chunk)

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
            md, //{ md.update(chunk) ; md },
            length + chunk.length)
        }
      }

      import reactivemongo.api.collections.bson.{
        BSONCollection, BSONCollectionProducer
      }

      def finish(): Future[ReadFile[P, Id]] = {
        import DefaultBSONHandlers._

        logger.debug(s"writing last chunk (n=$n)!")

        val uploadDate = file.uploadDate.getOrElse(System.currentTimeMillis)

        writeChunk(n, previous).flatMap { f =>
          val bson = BSONDocument(idProducer("_id" -> file.id)) ++ (
            "filename" -> BSONString(file.filename),
            "chunkSize" -> BSONInteger(chunkSize),
            "length" -> BSONLong(length),
            "uploadDate" -> BSONDateTime(uploadDate),
            "contentType" -> file.contentType.map(BSONString(_)),
            "metadata" -> option(!pack.isEmpty(file.metadata), file.metadata))

          files.as[BSONCollection]().insert(bson).map { _ =>
            val buf = ChannelBufferWritableBuffer()
            BSONSerializationPack.writeToBuffer(buf, bson)
            pack.readAndDeserialize(buf.toReadableBuffer, readFileReader)
          }
        }
      }

      def writeChunk(n: Int, array: Array[Byte]) = {
        logger.debug(s"writing chunk $n")

        val bson = {
          import DefaultBSONHandlers._
          BSONDocument(
            "files_id" -> file.id,
            "n" -> BSONInteger(n),
            "data" -> BSONBinary(array, Subtype.GenericBinarySubtype))
        }
        chunks.as[BSONCollection]().insert(bson)
      }
    }

    Iteratee.foldM(Chunk()) { (previous, chunk: Array[Byte]) =>
      logger.debug(s"processing new enumerated chunk from n=${previous.n}...\n")
      previous.feed(chunk)
    }.map(_.finish)
  }

  /** Produces an enumerator of chunks of bytes from the `chunks` collection matching the given file metadata. */
  def enumerate[Id <: pack.Value](file: ReadFile[P, Id])(implicit ctx: ExecutionContext, idProducer: IdProducer[Id]): Enumerator[Array[Byte]] = {
    import reactivemongo.api.collections.bson.{
      BSONCollection, BSONCollectionProducer
    }
    import DefaultBSONHandlers._

    val selector = BSONDocument(
      "$query" -> (BSONDocument(idProducer("files_id" -> file.id)) ++ (
        "n" -> BSONDocument(
          "$gte" -> BSONInteger(0),
          "$lte" -> BSONLong(file.length / file.chunkSize + (
            if (file.length % file.chunkSize > 0) 1 else 0))))),
      "$orderby" -> BSONDocument("n" -> BSONInteger(1)))

    val cursor = chunks.as[BSONCollection]().find(selector).cursor
    cursor.enumerate() &> Enumeratee.map { doc =>
      doc.get("data").flatMap {
        case BSONBinary(data, _) => {
          val array = new Array[Byte](data.readable)
          data.slice(data.readable).readBytes(array)
          Some(array)
        }
        case _ => None
      }.getOrElse {
        logger.error("not a chunk! failed assertion: data field is missing")
        throw new RuntimeException("not a chunk! failed assertion: data field is missing")
      }
    }
  }

  /** Reads the given file and writes its contents to the given OutputStream */
  def readToOutputStream[Id <: pack.Value](file: ReadFile[P, Id], out: OutputStream)(implicit ctx: ExecutionContext, idProducer: IdProducer[Id]): Future[Unit] = enumerate(file) |>>> Iteratee.foreach { out.write(_) }

  /** Writes the data provided by the given InputStream to the given file. */
  def writeFromInputStream[Id <: pack.Value](file: FileToSave[pack.type, Id], input: InputStream, chunkSize: Int = 262144)(implicit readFileReader: pack.Reader[ReadFile[P, Id]], ctx: ExecutionContext, idProducer: IdProducer[Id], docWriter: BSONDocumentWriter[file.pack.Document]): Future[ReadFile[P, Id]] = save(Enumerator.fromStream(input, chunkSize), file)

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
      BSONCollection, BSONCollectionProducer
    }
    import DefaultBSONHandlers._

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
