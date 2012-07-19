package org.asyncmongo.gridfs

import java.io.File
import java.io.InputStream
import scala.Array.canBuildFrom
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray
import scala.util.Random
import org.asyncmongo.api.Collection
import org.asyncmongo.api.Cursor
import org.asyncmongo.api.DB
import org.asyncmongo.api.MongoConnection
import org.asyncmongo.bson._
import org.asyncmongo.handlers._
import org.asyncmongo.handlers.DefaultBSONHandlers._
import org.asyncmongo.protocol.commands.GetLastError
import org.asyncmongo.utils.Converters
import org.jboss.netty.buffer.ChannelBuffers
import java.io.{OutputStream, ByteArrayOutputStream}
import play.api.libs.concurrent.{AkkaPromise, Promise}
import play.api.libs.iteratee._
import akka.dispatch.Future
import org.slf4j.{Logger, LoggerFactory}

case class GridFSFile(
  id: Option[BSONElement],
  length: Int,
  chunkSize: Int,
  md5: Option[String],
  filename: Option[String]) {
  def toBson = {
    val bson = Bson(
      "length" -> BSONInteger(length),
      "chunkSize" -> BSONInteger(chunkSize))
    if(md5.isDefined)
      bson.write("md5" -> BSONString(md5.get))
    if(id.isDefined)
      bson.write(id.get)
    if(filename.isDefined)
      bson.write("filename" -> BSONString(filename.get))
    bson
  }
}

case class PutResult(
  nbChunks: Int,
  md5: Option[String]
)

class GridFSIteratee(
  files_id: BSONValue,
  chunksCollection: Collection,
  chunkSize: Int = 262144 // default chunk size is 256k (GridFS spec)
) {
  import GridFSIteratee._
  implicit val ec = MongoConnection.system
  def iteratee = Iteratee.fold1(Chunk()) { (previous, chunk :Array[Byte]) =>
    logger.debug("processing new enumerated chunk from n=" + previous.n + "...\n")
    previous.feed(chunk)
  }.mapDone(_.finish)

  private case class Chunk(
    previous: Array[Byte] = new Array(0),
    n: Int = 0,
    md: java.security.MessageDigest = java.security.MessageDigest.getInstance("MD5")
  ) {
    def feed(chunk: Array[Byte]) :Promise[Chunk] = {
      val (toWrite, left) = (previous ++ chunk).grouped(chunkSize).partition(_.length == chunkSize)
      val writes = toWrite.toList.zipWithIndex
      new AkkaPromise(
        Future.traverse(writes) { ci =>
          writeChunk(n + ci._2, ci._1)
        }.map { _ =>
          logger.debug("all futures for the last given chunk are redeemed.")
          Chunk(
            if(left.isEmpty) Array.empty else left.next,
            n + writes.length,
            { md.update(chunk) ; md }
          )
        })
    }
    def finish() :Promise[PutResult] = {
      logger.debug("writing last chunk (n=" + n + ")!")
      new AkkaPromise(writeChunk(n, previous).map { _ =>
        PutResult(n, Some(Converters.hex2Str(md.digest)))
      })
    }
    def writeChunk(n: Int, array: Array[Byte]) = {
      val bson = Bson("files_id" -> files_id)
      bson.write("n" -> BSONInteger(n))
      bson.write("data" -> new BSONBinary(array, Subtype.GenericBinarySubtype))
      logger.debug("writing chunk " + n)
      chunksCollection.insert(bson, GetLastError())
    }
  }
}

object GridFSIteratee {
  private val logger = LoggerFactory.getLogger("GridFSIteratee")
}

class GridFS(db: DB, name: String = "fs") {
  import GridFS._
  val files = db(name + ".files")
  val chunks = db(name + ".chunks")

  def readContent(id: BSONValue) :Enumerator[Array[Byte]] = {
    val cursor = chunks.find(Bson("files_id" -> id), None, 0, Int.MaxValue)
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

  def readContent(id: BSONValue, os: OutputStream) :Promise[Iteratee[Array[Byte],Unit]] = {
    readContent(id)(Iteratee.foreach { chunk =>
      os.write(chunk)
    })
  }

  def write(file: File, chunkSize: Int = 16) :Promise[PutResult] = {
    val enumerator = Enumerator.fromFile(file, 100)
    val id = BSONString(file.getName + Random.nextInt)
    val bson = Bson(
        "_id" -> id,
        "chunkSize" -> BSONInteger(chunkSize),
        "filename" -> BSONString(file.getName),
        "length" -> BSONInteger(file.length.toInt))

    new AkkaPromise(files.insert(bson, GetLastError())).flatMap { _ =>
      val iteratee = GridFS.iteratee(id, chunks, chunkSize)
      val e = enumerator(iteratee)
      val pp = Iteratee.flatten(e).run
      pp.flatMap(i => i)
    }
  }
}

object GridFS {
  private val logger = LoggerFactory.getLogger("GridFS")
  def iteratee(files_id: BSONValue, chunksCollection: Collection, chunkSize: Int) :Iteratee[Array[Byte], Promise[PutResult]] =
    new GridFSIteratee(files_id, chunksCollection, chunkSize).iteratee
  def iteratee(files_id: BSONValue, chunksCollection: Collection) :Iteratee[Array[Byte], Promise[PutResult]] =
    new GridFSIteratee(files_id, chunksCollection).iteratee

  // tests
  def read {
    val gfs = new GridFS(DB("plugin", MongoConnection(List("localhost:27016"))))
    val baos = new ByteArrayOutputStream
    gfs.readContent(BSONString("TODO.txt4521292"), baos).onRedeem(_ => {
      val result = baos.toString("utf-8")
      println("DONE \n => " + result)
      println("\tof md5 = " + Converters.hex2Str(java.security.MessageDigest.getInstance("MD5").digest(baos.toByteArray)))
    })
  }

  def write {
    val gfs = new GridFS(DB("plugin", MongoConnection(List("localhost:27016"))))
    gfs.write(new File("/Volumes/Data/code/mongo-async-driver/TODO.txt")).onRedeem(result => println("successfully inserted file with result " + result))
  }
}