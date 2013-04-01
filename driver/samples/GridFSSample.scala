package samples

import java.io.{ File, FileOutputStream }

import play.api.libs.iteratee._

import reactivemongo.api.MongoDriver
import reactivemongo.api.gridfs._
import reactivemongo.api.gridfs.Implicits._
import reactivemongo.bson._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{ Failure, Success }

object GridFSSample {
  // obtain a connection on the local MongoDB instance
  val driver = new MongoDriver
  val connection = driver.connection(List("localhost"))
  // get the database 'gfstest'
  val db = connection.db("gfstest")

  // get a default GridFS store for this database
  val gfs = GridFS(db)

  // metadata for the file we are going to put in the store
  val metadata = DefaultFileToSave(
    "mongodb-osx-x86_64-2.4.0-rc3.tgz",
    contentType = Some("application/x-compressed"))

  // enumerator of the file contents
  val enumerator = Enumerator.fromFile(new File("/Users/sgo/Downloads/mongodb-osx-x86_64-2.4.0-rc3.tgz"))

  // ok, save the file and attach a callback when the operation is done
  gfs.save(enumerator, metadata).onComplete {
    case Success(file) =>
      println(s"successfully saved file of id ${file.id}")
    case Failure(e) =>
      println("exception while saving the file!")
      e.printStackTrace()
  }

  // enumerate contents and save them on disk
  val query = BSONDocument("filename" -> "mongodb-osx-x86_64-2.4.0-rc3.tgz")
  val maybeFile = gfs.find(query).headOption

  maybeFile.onComplete {
    // we found a file
    case Success(Some(file)) =>
      println(s"fetching contents of file '${file.filename}' (length ${file.length}b)...")
      val out = new FileOutputStream(file.filename)

      val writeOnFileSystemTask = gfs.readToOutputStream(file, out)

      writeOnFileSystemTask.onComplete {
        // we successfully wrote the file contents on disk
        case Success(_) =>
          println("done...")
        // there was an error, let's print it out
        case Failure(e) =>
          println("file download failed")
          e.printStackTrace()
      }
    case Success(None) =>
      println("file not found")
    case Failure(error) =>
      println("got an error!")
      error.printStackTrace()
  }

}