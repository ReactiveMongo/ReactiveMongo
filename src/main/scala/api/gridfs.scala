package reactivemongo.api.gridfs

import java.io._
import java.util.Arrays
import org.jboss.netty.buffer.ChannelBuffer
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.iteratee._
import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.bson.handlers._
import reactivemongo.bson.handlers.DefaultBSONHandlers._
import reactivemongo.core.commands.{GetLastError, LastError}
import reactivemongo.core.protocol.Response
import reactivemongo.utils._
import reactivemongo.utils.buffers._
import scala.concurrent.{Future, ExecutionContext}
import scala.util.Random

/**
 * A file's metadata.
 */
trait FileEntry {
  /** File name */
  val filename: String
  /** length of the file */
  val length: Int
  /** size of the chunks of this file */
  val chunkSize: Int
  /** the date when this file was uploaded. */
  val uploadDate: Option[Long]
  /** the MD5 hash of this file. */
  val md5: Option[String]
  /** mimetype of this file. */
  val contentType: Option[String]
  /** the GridFS store of this file. */
  val gridFS: GridFS
}

/** A read file's metadata */
trait ReadFileEntry extends FileEntry {
  /** The id of this file. */
  val id: BSONValue

  import ReadFileEntry.logger

  /** Produces an enumerator of chunks of bytes from the `chunks` collection. */
  def enumerate(implicit ctx: ExecutionContext) :Enumerator[Array[Byte]] = {
    val selector = BSONDocument(
      "$query" -> BSONDocument(
        "files_id" -> id,
        "n" -> BSONDocument(
          "$gte" -> BSONInteger(0),
          "$lte" -> BSONInteger( length/chunkSize + (if(length % chunkSize > 0) 1 else 0) )
        )
      ),
      "$orderby" -> BSONDocument(
        "n" -> BSONInteger(1)
      )
    )
    val cursor = gridFS.chunks.find(selector)
    cursor.enumerate &> (Enumeratee.map { doc =>
      doc.get("data").flatMap {
        case BSONBinary(data, _) => Some(data.array())
        case _ => None
      }.getOrElse {
        logger.error("not a chunk! failed assertion: data field is missing")
        throw new RuntimeException("not a chunk! failed assertion: data field is missing")
      }
    })
  }

  /**
   * Helper to write the contents of this file into an OutputStream.
   *
   * Basically it's just an enumerator produced by the `enumerate` method that is applied to an Iteratee that writes the consumed chunks into the given stream.
   */
  def readContent(os: OutputStream)(implicit ctx: ExecutionContext) :Future[Iteratee[Array[Byte],Unit]] = {
    enumerate(ctx)(Iteratee.foreach { chunk =>
      os.write(chunk)
    })
  }
}

case class DefaultReadFileEntry(
  id: BSONValue,
  filename: String,
  length: Int,
  chunkSize: Int,
  uploadDate: Option[Long],
  md5: Option[String],
  contentType: Option[String],
  gridFS: GridFS
) extends ReadFileEntry

object ReadFileEntry {
  private val logger = LazyLogger(LoggerFactory.getLogger("ReadFileEntry"))
  // TODO
  def bsonReader(gFS: GridFS) = new Reader(gFS)

  class Reader(gFS: GridFS) extends BSONReader[DefaultReadFileEntry] {
    def fromBSON(doc: BSONDocument) = {
      val document = doc.toTraversable
      DefaultReadFileEntry(
        document.get("_id").getOrElse(throw new RuntimeException("_id is mandatory for a stored gridfs file!")),
        document.get("filename").flatMap {
          case BSONString(name) => Some(name)
          case _ => None
        }.getOrElse(""),
        document.get("length").flatMap {
          case BSONInteger(i) => Some(i)
          case _ => None
        }.getOrElse(throw new RuntimeException("length is mandatory for a stored gridfs file!")),
        document.get("chunkSize").flatMap {
          case BSONInteger(i) => Some(i)
          case _ => None
        }.getOrElse(throw new RuntimeException("chunkSize is mandatory for a stored gridfs file!")),
        document.get("uploadDate").flatMap {
          case BSONDateTime(time) => Some(time)
          case _ => None
        },
        document.get("md5").flatMap {
          case BSONString(m) => Some(m)
          case _ => None
        },
        document.get("contentType").flatMap {
          case BSONString(ct) => Some(ct)
          case _ => None
        },
        gFS
      )
    }
  }
}

/**
 * A file to write.
 *
 * @param id The id of the file to write. If an id is provided, the matching file metadata will be replaced.
 * @param name The name of the file to write.
 */
case class FileToWrite(
  id: Option[BSONValue],
  name: String,
  contentType: Option[String]
) {
  import FileToWrite.logger

  /**
   * Returns an Iteratee[Array[Byte], Future[PutResult]] that will consume chunks of bytes, normalize their size and write them into the `chunks` collection.
   *
   * @param gfs The GridFS store.
   * @param chunkSize The size of the chunks to be written. Defaults to 256k (GridFS Spec).
   */
  def iteratee(gfs: GridFS, chunkSize: Int = 262144)(implicit ctx: ExecutionContext): Iteratee[Array[Byte], Future[ReadFileEntry]] = {
    implicit val ec = MongoConnection.system

    val files_id = id.getOrElse(BSONObjectID.generate)

    case class Chunk(
      previous: Array[Byte] = new Array(0),
      n: Int = 0,
      md: java.security.MessageDigest = java.security.MessageDigest.getInstance("MD5"),
      length: Int = 0
    ) {
      def feed(chunk: Array[Byte]) :Future[Chunk] = {
        val wholeChunk = concat(previous, chunk)

        val normalizedChunkNumber = wholeChunk.length / chunkSize

        logger.debug("wholeChunk size is " + wholeChunk.length + " => " + normalizedChunkNumber)

        val zipped = for(i <- 0 until normalizedChunkNumber) yield
          Arrays.copyOfRange(wholeChunk, i * chunkSize, (i + 1) * chunkSize) -> i

        val left = Arrays.copyOfRange(wholeChunk, normalizedChunkNumber * chunkSize, wholeChunk.length)

        Future.traverse(zipped) { ci =>
          writeChunk(n + ci._2, ci._1)
        }.map { _ =>
          logger.debug("all futures for the last given chunk are redeemed.")
          Chunk(
            if(left.isEmpty) Array.empty else left,
            n + normalizedChunkNumber,
            md,//{ md.update(chunk) ; md },
            length + chunk.length
          )
        }
      }
      def finish() :Future[ReadFileEntry] = {
        logger.debug("writing last chunk (n=" + n + ")!")
        val uploadDate = System.currentTimeMillis
        writeChunk(n, previous).flatMap { _ =>
          val bson = BSONDocument(
            "_id"         -> files_id,
            "filename"    -> BSONString(name),
            "chunkSize"   -> BSONInteger(chunkSize),
            "length"      -> BSONInteger(length),
            "uploadDate"  -> BSONDateTime(uploadDate),
            "contentType" -> contentType.map(BSONString(_))
          )
          gfs.files.insert(bson).map(_ => DefaultReadFileEntry(
            files_id,
            name,
            length,
            chunkSize,
            Some(uploadDate),
            None, // TODO
            contentType,
            gfs
          ))
        }
      }
      def writeChunk(n: Int, array: Array[Byte]) = {
        logger.debug("writing chunk " + n)
        val bson = BSONDocument(
          "files_id" -> files_id,
          "n"        -> BSONInteger(n),
          "data"     -> new BSONBinary(array, Subtype.GenericBinarySubtype)
        )
        gfs.chunks.insert(bson)
      }
    }

    Iteratee.fold1(Chunk()) { (previous, chunk :Array[Byte]) =>
      logger.debug("processing new enumerated chunk from n=" + previous.n + "...\n")
      previous.feed(chunk)
    }.mapDone(_.finish)
  }
}

object FileToWrite {
  private val logger = LazyLogger(LoggerFactory.getLogger("FileToWrite"))
}

/**
 * A helper class to make GridFS queries.
 *
 * GridFS creates two collections, `files` and `chunks`, that store respectively the files metadata ant their chunks.
 * These collections are prefixed by a customizable name (usually "fs") followed by a dot character.
 *
 * So, if the database is "media" and the GridFS prefix is "photos", the two collections are:
 *   - media.photos.files
 *   - media.photos.chunks
 *
 * @param db The database where the GridFS collections are.
 * @param prefix The prefix of this GridFS. Defaults to "fs".
 */
case class GridFS(db: DB[Collection] with DBMetaCommands, prefix: String = "fs") {
  import indexes._

  /** The `files` collection */
  val files = db(prefix + ".files")
  /** The `chunks` collection */
  val chunks = db(prefix + ".chunks")

  /**
   * Finds the files matching the given selector.
   *
   * @tparam S the type of the selector document. An implicit [[reactivemongo.bson.handlers.RawBSONWriter]][S] must be in the scope.
   *
   * @param selector The document to select the files to return
   */
  def find[S](selector: S)(implicit sWriter: RawBSONWriter[S], ctx: ExecutionContext) :Cursor[ReadFileEntry] = {
    implicit val rfeReader = ReadFileEntry.bsonReader(this)
    files.find(selector)
  }

  /**
   * Saves a file with the given name.
   *
   * If an id is provided, the matching file metadata will be replaced.
   *
   * @param name the file name.
   * @param id an id for the new file. If none is provided, a new ObjectId will be generated.
   *
   * @return an iteratee to be applied to an enumerator of chunks of bytes.
   */
  def save(name: String, id: Option[BSONValue], contentType: Option[String] = None)(implicit ctx: ExecutionContext) :Iteratee[Array[Byte], Future[ReadFileEntry]] = FileToWrite(id, name, contentType).iteratee(this)

  /**
   * Removes a file from this store.
   * Note that if the file does not actually exist, the returned future will not be hold an error.
   *
   * @param file The file entry to remove from this store.
   */
  def remove(file: ReadFileEntry)(implicit ctx: ExecutionContext) :Future[LastError] = remove(file.id)

  /**
   * Removes a file from this store.
   * Note that if the file does not actually exist, the returned future will not be hold an error.
   *
   * @param id The file id to remove from this store.
   */
  def remove(id: BSONValue)(implicit ctx: ExecutionContext) :Future[LastError] = {
    chunks.remove(BSONDocument("files_id" -> id)).flatMap { _ =>
      files.remove(BSONDocument("_id" -> id))
    }
  }

  /**
   * Creates the needed index on the `chunks` collection, if none.
   *
   * Please note that you should really consider reading [[http://www.mongodb.org/display/DOCS/Indexes]] before doing this, especially in production.
   *
   * @return a future containing true if the index was created, false if it already exists.
   */
  def ensureIndex()(implicit ctx: ExecutionContext) :Future[Boolean] =
    db.indexesManager.onCollection("chunks").ensure(Index( List("files_id" -> true, "n" -> true), unique = true ))
}