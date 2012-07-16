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

case class GridFSFile(
  id: Option[BSONElement],
  length: Int,
  chunkSize: Int,
  md5: Option[String],
  filename: Option[String]) {
  def toBson = {
    val bson = Bson(
      BSONInteger("length", length),
      BSONInteger("chunkSize", chunkSize))
    if(md5.isDefined)
      BSONString("md5", md5.get)
    if(id.isDefined)
      bson.write(id.get)
    if(filename.isDefined)
      bson.write(BSONString("filename", filename.get))
    bson
  }
}

class GridFS(db: DB, name: String = "fs") {
  val files = db(name + ".files")
  val chunks = db(name + ".chunks")

  def readContent(id: BSONElement) :Enumerator[Array[Byte]] = {
    val toto = chunks.find(Bson(id), None, 0, Int.MaxValue)
    val e = Cursor.enumerate(Some(toto))
    val e2 = e.through(Enumeratee.map { doc =>
      doc.find(_.name == "data").flatMap {
        case BSONBinary(_, data, _) => Some(data.array())
        case _ => None
      }.getOrElse {
        println("ahem.")
        throw new RuntimeException("not a chunk")
      }
    })
    e2
  }

  def readContent(id: BSONElement, os: OutputStream) :Promise[Iteratee[Array[Byte],Unit]] = {
    readContent(id)(Iteratee.foreach { chunk =>
      os.write(chunk)
      println("wrote a chunk into os.")
    })
  }

  def write(file: File, chunkSize: Int = 16) :Promise[PutResult] = {
    val enumerator = Enumerator.fromFile(file, 100)
    val id = BSONString("_id", file.getName + Random.nextInt)
    val bson = Bson(
        id,
        BSONInteger("chunkSize", chunkSize),
        BSONString("filename", file.getName),
        BSONInteger("length", file.length.toInt))

    new AkkaPromise(files.insert(bson, GetLastError())).flatMap { _ =>
      val iteratee = GridFS.iteratee(id.copy(name="files_id"), chunks, chunkSize)
      val e = enumerator(iteratee)
      val pp = Iteratee.flatten(e).run
      pp.flatMap(i => i)
    }
  }
}

case class PutResult(
  nbChunks: Int,
  md5: Option[String]
)

class GridFSIteratee(
  files_id: BSONElement,
  chunksCollection: Collection,
  chunkSize: Int = 262144 // default chunk size is 256k (GridFS spec)
) {
  implicit val ec = MongoConnection.system
  def iteratee = Iteratee.fold1(Chunk()) { (previous, chunk :Array[Byte]) =>
    println("\nGRIDFS: processing n=" + previous.n + "...\n")
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
          println("all futures for the last given chunk are redeemed")
          Chunk(
            if(left.isEmpty) Array.empty else left.next,
            n + writes.length,
            { md.update(chunk) ; md }
          )
        })
    }
    def finish() :Promise[PutResult] = {
      println("GRIDFS: writing last chunk!")
      new AkkaPromise(writeChunk(n, previous).map { _ =>
        PutResult(n, Some(Converters.hex2Str(md.digest)))
      })
    }
    def writeChunk(n: Int, array: Array[Byte]) = {
      val bson = Bson(files_id)
      bson.write(BSONInteger("n", n))
      bson.write(new BSONBinary("data", array, genericBinarySubtype))
      println("GRIDFS: writing chunk " + n)
      chunksCollection.insert(bson, GetLastError())
    }
  }
}

object GridFS {
  def iteratee(files_id: BSONElement, chunksCollection: Collection, chunkSize: Int) :Iteratee[Array[Byte], Promise[PutResult]] =
    new GridFSIteratee(files_id, chunksCollection, chunkSize).iteratee
  def iteratee(files_id: BSONElement, chunksCollection: Collection) :Iteratee[Array[Byte], Promise[PutResult]] =
    new GridFSIteratee(files_id, chunksCollection).iteratee

  // tests
  def read {
    val gfs = new GridFS(DB("plugin", MongoConnection(List("localhost:27016"))))
    val baos = new ByteArrayOutputStream
    gfs.readContent(BSONString("files_id", "TODO.txt-1978187670"), baos).onRedeem(_ => {
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