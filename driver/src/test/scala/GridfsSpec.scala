import org.specs2.mutable._
import play.api.libs.iteratee.Enumerator
import reactivemongo.api._
import reactivemongo.api.gridfs.{DefaultFileToSave, GridFS}
import reactivemongo.api.gridfs.Implicits._
import reactivemongo.bson._
import reactivemongo.core.commands.Count
import scala.concurrent._
import scala.util.Failure
import reactivemongo.api.gridfs
import scala.concurrent.duration._

class GridfsSpec extends Specification {
  import Common._

  sequential

  lazy val gfs = GridFS(db)

  lazy val file = DefaultFileToSave("somefile", Some("application/file"), Some(1387201380000l), id = BSONObjectID.generate)
  lazy val fileContent = Enumerator((1 to 100).view.map(_.toByte).toArray)

  "ReactiveMongo" should {

    "store a file in gridfs" in {
      Await.result(gfs.save(fileContent, file), Duration(3, SECONDS)).filename mustEqual "somefile"
    }

    "find this file in gridfs" in {
      Await.result(gfs.find(BSONDocument("filename" -> "somefile")).headOption, Duration(3, SECONDS)) match {
        case None => failure("file not found")
        case Some(f) => success
      }
    }

    "delete this file from gridfs" in {
      Await.result(gfs.remove(file.id), Duration(3, SECONDS)).n mustEqual 1
    }
  }
}
