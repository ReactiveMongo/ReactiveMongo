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

import java.io._
import java.util.Arrays
import play.api.libs.iteratee._
import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.core.commands.LastError
import reactivemongo.utils._
import scala.concurrent.{ ExecutionContext, Future }
import reactivemongo.core.netty.ChannelBufferWritableBuffer
import reactivemongo.api.collections.GenericCollectionProducer
import reactivemongo.api.collections.GenericCollection

object `package` {
  private[gridfs] val logger = LazyLogger("reactivemongo.api.gridfs")
}

object Implicits {
  /** A default `BSONReader` for `ReadFile`. */
  implicit object DefaultReadFileReader extends BSONDocumentReader[ReadFile[BSONValue]] {
    import DefaultBSONHandlers._
    def read(doc: BSONDocument) = {
      DefaultReadFile(
        doc.getAs[BSONValue]("_id").get,
        doc.getAs[BSONString]("contentType").map(_.value),
        doc.getAs[BSONString]("filename").map(_.value).get,
        doc.getAs[BSONNumberLike]("uploadDate").map(_.toLong),
        doc.getAs[BSONNumberLike]("chunkSize").map(_.toInt).get,
        doc.getAs[BSONNumberLike]("length").map(_.toInt).get,
        doc.getAs[BSONString]("md5").map(_.value),
        doc.getAs[BSONDocument]("metadata").getOrElse(BSONDocument()),
        doc)
    }
  }
}

/** Metadata that cannot be customized. */
trait ComputedMetadata {
  /** Length of the file. */
  def length: Int
  /** Size of the chunks of this file. */
  def chunkSize: Int
  /** MD5 hash of this file. */
  def md5: Option[String]
}

/**
 * Common metadata.
 * @tparam Id Type of the id of this file (generally `BSONObjectID` or `BSONValue`).
 */
trait BasicMetadata[+Id <: BSONValue] {
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
trait CustomMetadata {
  /** A BSONDocument holding all the metadata that are not standard. */
  def metadata: BSONDocument
}

/**
 * A file that will be saved in a GridFS store.
 * @tparam Id Type of the id of this file (generally `BSONObjectID` or `BSONValue`).
 */
trait FileToSave[+Id <: BSONValue] extends BasicMetadata[Id] with CustomMetadata

/** A default implementation of `FileToSave[BSONValue]`. */
case class DefaultFileToSave(
  filename: String,
  contentType: Option[String] = None,
  uploadDate: Option[Long] = None,
  metadata: BSONDocument = BSONDocument(),
  id: BSONValue = BSONObjectID.generate) extends FileToSave[BSONValue]

/**
 * A file read from a GridFS store.
 * @tparam Id Type of the id of this file (generally `BSONObjectID` or `BSONValue`).
 */
trait ReadFile[+Id <: BSONValue] extends BasicMetadata[Id] with CustomMetadata with ComputedMetadata

/** A default implementation of `ReadFile[BSONValue]`. */
case class DefaultReadFile(
  id: BSONValue,
  contentType: Option[String],
  filename: String,
  uploadDate: Option[Long],
  chunkSize: Int,
  length: Int,
  md5: Option[String],
  metadata: BSONDocument,
  original: BSONDocument) extends ReadFile[BSONValue]

/**
 * A GridFS store.
 * @param db The database where this store is located.
 * @param prefix The prefix of this store. The `files` and `chunks` collections will be actually named `prefix.files` and `prefix.chunks`.
 */
class GridFS[Structure, Reader[_], Writer[_]](db: DB with DBMetaCommands, prefix: String = "fs")(implicit producer: GenericCollectionProducer[Structure, Reader, Writer, GenericCollection[Structure, Reader, Writer]] = collections.default.BSONCollectionProducer) {
  import indexes._
  import IndexType._

  /** The `files` collection */
  def files[C <: Collection](implicit collProducer: CollectionProducer[C] = producer) = db(prefix + ".files")(collProducer)
  /** The `chunks` collection */
  def chunks[C <: Collection](implicit collProducer: CollectionProducer[C] = producer) = db(prefix + ".chunks")(collProducer)

  /**
   * Finds the files matching the given selector.
   *
   * @param selector The document to select the files to return
   *
   * @tparam S The type of the selector document. An implicit `Writer[S]` must be in the scope.
   */ // TODO More generic deserializers ?
  def find[S, T <: ReadFile[_]](selector: S)(implicit sWriter: Writer[S], readFileReader: Reader[T], ctx: ExecutionContext): Cursor[T] = {
    files.find(selector).cursor
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
  def save[Id <: BSONValue](enumerator: Enumerator[Array[Byte]], file: FileToSave[Id], chunkSize: Int = 262144)(implicit readFileReader: Reader[ReadFile[Id]], ctx: ExecutionContext): Future[ReadFile[Id]] = {
    (enumerator |>>> iteratee(file, chunkSize)).flatMap(f => f)
  }

  import reactivemongo.api.collections.default.{ BSONCollection, BSONCollectionProducer }

  /**
   * Gets an `Iteratee` that will consume data to put into a GridFS store.
   * @param file Metadata of the file to store.
   * @param chunkSize Size of the chunks. Defaults to 256kB.
   *
   * @tparam Id Type of the id of this file (generally `BSONObjectID` or `BSONValue`).
   *
   * @return An `Iteratee` that will consume data to put into a GridFS store.
   */
  def iteratee[Id <: BSONValue](file: FileToSave[Id], chunkSize: Int = 262144)(implicit readFileReader: Reader[ReadFile[Id]], ctx: ExecutionContext): Iteratee[Array[Byte], Future[ReadFile[Id]]] = {
    implicit val ec = db.connection.actorSystem

    case class Chunk(
        previous: Array[Byte] = new Array(0),
        n: Int = 0,
        md: java.security.MessageDigest = java.security.MessageDigest.getInstance("MD5"),
        length: Int = 0) {
      def feed(chunk: Array[Byte]): Future[Chunk] = {
        val wholeChunk = concat(previous, chunk)

        val normalizedChunkNumber = wholeChunk.length / chunkSize

        logger.debug("wholeChunk size is " + wholeChunk.length + " => " + normalizedChunkNumber)

        val zipped = for (i <- 0 until normalizedChunkNumber) yield Arrays.copyOfRange(wholeChunk, i * chunkSize, (i + 1) * chunkSize) -> i

        val left = Arrays.copyOfRange(wholeChunk, normalizedChunkNumber * chunkSize, wholeChunk.length)

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
      def finish(): Future[ReadFile[Id]] = {
        import DefaultBSONHandlers._
        logger.debug("writing last chunk (n=" + n + ")!")
        val uploadDate = file.uploadDate.getOrElse(System.currentTimeMillis)
        writeChunk(n, previous).flatMap { f =>
          val bson = BSONDocument(
            "_id" -> file.id.asInstanceOf[BSONValue],
            "filename" -> BSONString(file.filename),
            "chunkSize" -> BSONInteger(chunkSize),
            "length" -> BSONInteger(length),
            "uploadDate" -> BSONDateTime(uploadDate),
            "contentType" -> file.contentType.map(BSONString(_)),
            "metadata" -> option(!file.metadata.isEmpty, file.metadata))
          files[BSONCollection].insert(bson).map(_ =>
            files(producer).BufferReaderInstance(readFileReader).read(
              files[BSONCollection].StructureBufferWriter.write(bson, ChannelBufferWritableBuffer()).toReadableBuffer))
        }
      }
      def writeChunk(n: Int, array: Array[Byte]) = {
        logger.debug("writing chunk " + n)
        val bson = {
          import DefaultBSONHandlers._
          BSONDocument(
            "files_id" -> file.id.asInstanceOf[BSONValue],
            "n" -> BSONInteger(n),
            "data" -> BSONBinary(array, Subtype.GenericBinarySubtype))
        }
        chunks[BSONCollection].insert(bson)
      }
    }

    Iteratee.foldM(Chunk()) { (previous, chunk: Array[Byte]) =>
      logger.debug("processing new enumerated chunk from n=" + previous.n + "...\n")
      previous.feed(chunk)
    }.mapDone(_.finish)
  }

  /** Produces an enumerator of chunks of bytes from the `chunks` collection matching the given file metadata. */
  def enumerate(file: ReadFile[_ <: BSONValue])(implicit ctx: ExecutionContext): Enumerator[Array[Byte]] = {
    import DefaultBSONHandlers._
    val selector = BSONDocument(
      "$query" -> BSONDocument(
        "files_id" -> file.id,
        "n" -> BSONDocument(
          "$gte" -> BSONInteger(0),
          "$lte" -> BSONInteger(file.length / file.chunkSize + (if (file.length % file.chunkSize > 0) 1 else 0)))),
      "$orderby" -> BSONDocument(
        "n" -> BSONInteger(1)))

    val cursor = chunks[BSONCollection].find(selector).cursor
    cursor.enumerate &> (Enumeratee.map { doc =>
      doc.get("data").flatMap {
        case BSONBinary(data, _) => {
          val array = new Array[Byte](data.readable)
          data.slice(data.readable).readBytes(array) //Some(data.array()) // TODO TODO TODO
          Some(array)
        }
        case _ => None
      }.getOrElse {
        logger.error("not a chunk! failed assertion: data field is missing")
        throw new RuntimeException("not a chunk! failed assertion: data field is missing")
      }
    })
  }

  /** Reads the given file and writes its contents to the given OutputStream */
  def readToOutputStream(file: ReadFile[_ <: BSONValue], out: OutputStream)(implicit ctx: ExecutionContext): Future[Unit] = {
    enumerate(file) |>>> Iteratee.foreach { chunk =>
      out.write(chunk)
    }
  }

  /** Writes the data provided by the given InputStream to the given file. */
  def writeFromInputStream[Id <: BSONValue](file: FileToSave[Id], input: InputStream, chunkSize: Int = 262144)(implicit readFileReader: Reader[ReadFile[Id]], ctx: ExecutionContext): Future[ReadFile[Id]] = {
    save(Enumerator.fromStream(input, chunkSize), file)
  }

  /**
   * Removes a file from this store.
   * Note that if the file does not actually exist, the returned future will not be hold an error.
   *
   * @param file The file entry to remove from this store.
   */
  def remove[Id <: BSONValue](file: BasicMetadata[Id])(implicit ctx: ExecutionContext): Future[LastError] = remove(file.id)

  /**
   * Removes a file from this store.
   * Note that if the file does not actually exist, the returned future will not be hold an error.
   *
   * @param id The file id to remove from this store.
   */
  def remove(id: BSONValue)(implicit ctx: ExecutionContext): Future[LastError] = {
    import DefaultBSONHandlers._
    chunks[BSONCollection].remove(BSONDocument("files_id" -> id)).flatMap { _ =>
      files[BSONCollection].remove(BSONDocument("_id" -> id))
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
  def apply[Structure, Reader[_], Writer[_]](db: DB with DBMetaCommands, prefix: String = "fs")(implicit producer: GenericCollectionProducer[Structure, Reader, Writer, GenericCollection[Structure, Reader, Writer]] = collections.default.BSONCollectionProducer) =
    new GridFS(db, prefix)(producer)
}