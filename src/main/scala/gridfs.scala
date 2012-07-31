package org.asyncmongo.gridfs

import akka.dispatch.Future

import java.io._
import java.util.Arrays

import org.asyncmongo.api._
import org.asyncmongo.bson._
import org.asyncmongo.handlers._
import org.asyncmongo.handlers.DefaultBSONHandlers._
import org.asyncmongo.protocol.commands.GetLastError
import org.asyncmongo.protocol.Response
import org.asyncmongo.utils.Converters
import org.asyncmongo.utils.ArrayUtils

import org.jboss.netty.buffer.ChannelBuffer
import org.slf4j.{Logger, LoggerFactory}

import play.api.libs.concurrent.{AkkaPromise, Promise}
import play.api.libs.iteratee._

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

  /** Produces an enumerator of chunks of bytes from the ''chunks'' collection. */
  def enumerate() :Enumerator[Array[Byte]] = {
    val selector = Bson(
      "$query" -> Bson(
        "files_id" -> id,
        "n" -> Bson(
          "$gte" -> BSONInteger(0),
          "$lte" -> BSONInteger( length/chunkSize + (if(length % chunkSize > 0) 1 else 0) )
        ).toDocument
      ).toDocument,
      "$orderby" -> Bson(
        "n" -> BSONInteger(1)
      ).toDocument
    )
    val cursor = gridFS.chunks.find(selector, None, 0, Int.MaxValue)
    Cursor.enumerate(cursor) &> (Enumeratee.map { doc =>
      doc.find(_.name == "data").flatMap {
        case ReadBSONElement(_, BSONBinary(data, _)) => Some(data.array())
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
   * Basically it's just an enumerator produced by the ''enumerate'' method that is applied to an Iteratee that writes the consumed chunks into the given stream.
   */
  def readContent(os: OutputStream) :Promise[Iteratee[Array[Byte],Unit]] = {
    enumerate()(Iteratee.foreach { chunk =>
      os.write(chunk)
    })
  }
}

object ReadFileEntry {
  private val logger = LoggerFactory.getLogger("ReadFileEntry")
  def bsonReader(gFS: GridFS) = new BSONReader[ReadFileEntry] {
    def read(buffer: ChannelBuffer) = {
      val iterator = DefaultBSONHandlers.DefaultBSONReader.read(buffer)
      val map = iterator.mapped
      new ReadFileEntry {
        val length = map.get("length").flatMap(_.value match {
          case BSONInteger(i) => Some(i)
          case _ => None
        }).getOrElse(throw new RuntimeException("length is mandatory for a stored gridfs file!"))
        override val chunkSize = map.get("chunkSize").flatMap(_.value match {
          case BSONInteger(i) => Some(i)
          case _ => None
        }).getOrElse(throw new RuntimeException("chunkSize is mandatory for a stored gridfs file!"))
        val uploadDate = map.get("uploadDate").flatMap(_.value match {
          case BSONDateTime(time) => Some(time)
          case _ => None
        })
        val md5 = map.get("md5").flatMap(_.value match {
          case BSONString(m) => Some(m)
          case _ => None
        })
        val filename = map.get("filename").flatMap(_.value match {
          case BSONString(name) => Some(name)
          case _ => None
        }).getOrElse("")
        val contentType = map.get("contentType").flatMap(_.value match {
          case BSONString(ct) => Some(ct)
          case _ => None
        })
        val id = map.get("_id").getOrElse(throw new RuntimeException("_id is mandatory for a stored gridfs file!")).value
        val gridFS = gFS
      }
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
   * Returns an Iteratee[Array[Byte], Promise[PutResult]] that will consume chunks of bytes, normalize their size and write them into the ''chunks'' collection.
   *
   * @param gfs The GridFS store.
   * @param chunkSize The size of the chunks to be written. Defaults to 256k (GridFS Spec).
   */
  def iteratee(gfs: GridFS, chunkSize: Int = 262144) = {
    implicit val ec = MongoConnection.system

    val files_id = id.getOrElse(BSONObjectID.generate)

    case class Chunk(
      previous: Array[Byte] = new Array(0),
      n: Int = 0,
      md: java.security.MessageDigest = java.security.MessageDigest.getInstance("MD5"),
      length: Int = 0
    ) {
      def feed(chunk: Array[Byte]) :Promise[Chunk] = {
        val wholeChunk = ArrayUtils.concat(previous, chunk)

        val normalizedChunkNumber = wholeChunk.length / chunkSize

        logger.debug("wholeChunk size is " + wholeChunk.length + " => " + normalizedChunkNumber)

        val zipped = for(i <- 0 until normalizedChunkNumber) yield
          Arrays.copyOfRange(wholeChunk, i * chunkSize, (i + 1) * chunkSize) -> i

        val left = Arrays.copyOfRange(wholeChunk, normalizedChunkNumber * chunkSize, wholeChunk.length)

        new AkkaPromise(Future.traverse(zipped) { ci =>
          writeChunk(n + ci._2, ci._1)
        }.map { _ =>
          logger.debug("all futures for the last given chunk are redeemed.")
          Chunk(
            if(left.isEmpty) Array.empty else left,
            n + normalizedChunkNumber,
            md,//{ md.update(chunk) ; md },
            length + chunk.length
          )
        })
      }
      def finish() :Promise[PutResult] = {
        logger.debug("writing last chunk (n=" + n + ")!")
        new AkkaPromise(writeChunk(n, previous).flatMap { _ =>
          val bson = Bson(
            "_id" -> files_id,
            "filename" -> BSONString(name),
            "chunkSize" -> BSONInteger(chunkSize),
            "length" -> BSONInteger(length),
            "uploadDate" -> BSONDateTime(System.currentTimeMillis))
          if(contentType.isDefined)
            bson.add("contentType" -> BSONString(contentType.get))
          gfs.files.insert(bson).map(_ => PutResult(files_id, n, length, Some(Converters.hex2Str(md.digest))))
        })
      }
      def writeChunk(n: Int, array: Array[Byte]) = {
        val bson = Bson("files_id" -> files_id)
        bson.add("n" -> BSONInteger(n))
        bson.add("data" -> new BSONBinary(array, Subtype.GenericBinarySubtype))
        logger.debug("writing chunk " + n)
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
  private val logger = LoggerFactory.getLogger("FileToWrite")
}

/**
 * The metadata of the saved file.
 *
 * @param id The id of the saved file.
 * @param nbChunks The number of chunks that have been written into the ''chunks'' collection.
 * @param length The length of the saved file.
 * @param md5 The MD5 hash of the saved file, if computed.
 */
case class PutResult(
  id: BSONValue,
  nbChunks: Int,
  length: Int,
  md5: Option[String]
)

/**
 * A helper class to make GridFS queries.
 *
 * GridFS creates two collections, ''files'' and ''chunks'', that store respectively the files metadata ant their chunks.
 * These collections are prefixed by a customizable name (usually "fs") followed by a dot character.
 *
 * So, if the database is "media" and the GridFS prefix is "photos", the two collections are:
 *   - media.photos.files
 *   - media.photos.chunks
 *
 * @param db The database where the GridFS collections are.
 * @param prefix The prefix of this GridFS. Defaults to "fs".
 */
case class GridFS(db: DB, prefix: String = "fs") {
  /** The ''files'' collection */
  val files = db(prefix + ".files")
  /** The ''chunks'' collection */
  val chunks = db(prefix + ".chunks")

  /**
   * Finds the files matching the given selector.
   *
   * @tparam S the type of the selector document. An implicit [[org.asyncmongo.handlers.BSONWriter]][S] must be in the scope.
   *
   * @param selector The document to select the files to return
   * @param limit Limits the returned documents.
   */
  def find[S](selector: S, limit :Int = Int.MaxValue)(implicit sWriter: BSONWriter[S]) :Future[Cursor[ReadFileEntry]] = {
    implicit val rfeReader = ReadFileEntry.bsonReader(this)
    files.find(selector, None, 0, limit, 0)
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
  def save(name: String, id: Option[BSONValue], contentType: Option[String] = None) :Iteratee[Array[Byte], Promise[PutResult]] = FileToWrite(id, name, contentType).iteratee(this)
}

object GridFS {
  import akka.pattern.ask
  import akka.util.Timeout
  import akka.util.duration._

  implicit val timeout = Timeout(5 seconds)

  // tests
  def read {
    val gfs = new GridFS(DB("plugin", MongoConnection(List("localhost:27016"))))
    val baos = new ByteArrayOutputStream
    new AkkaPromise(gfs.find(Bson(), 1)).flatMap( _.iterator.next.readContent(baos) ).onRedeem(_ => {
      val result = baos.toString("utf-8")
      println("DONE \n => " + result)
      println("\tof md5 = " + Converters.hex2Str(java.security.MessageDigest.getInstance("MD5").digest(baos.toByteArray)))
    })
  }

  def write3 {
    import akka.pattern.ask
    import akka.util.Timeout
    import akka.util.duration._

    implicit val timeout = Timeout(5 seconds)

    val start = System.currentTimeMillis
    val connection = MongoConnection(List("localhost:27016"))
    connection.waitForPrimary(timeout).onSuccess {
      case _ => val gfs = new GridFS(DB("plugin", connection))

      val filetowrite = FileToWrite(None, "hepla.txt", Some("text/plain"))

      val iteratee = filetowrite.iteratee(gfs, 11)

      val file = new File("/Volumes/Data/code/mongo-async-driver/TODO.txt")
      val enumerator = Enumerator.fromFile(file, 1024 * 1024)

      val pp = Iteratee.flatten(enumerator(iteratee)).run
      pp.flatMap(i => i).onRedeem { result =>
        println("successfully inserted file with result " + result + " in " + (System.currentTimeMillis - start) + " ms")
      }

    }

  }
}