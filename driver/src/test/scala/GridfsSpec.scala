import org.specs2.mutable._
import play.api.libs.iteratee.Enumerator
import reactivemongo.api._
import reactivemongo.api.gridfs.{ReadFile, DefaultFileToSave, GridFS}
import reactivemongo.api.gridfs.Implicits._
import reactivemongo.bson._
import scala.concurrent._
import reactivemongo.api.gridfs
import scala.concurrent.duration._

class GridfsSpec extends Specification {
  import Common._

  sequential

  lazy val gfs = GridFS(db)

  lazy val file = DefaultFileToSave("somefile", Some("application/file"))
  lazy val fileContent = Enumerator((1 to 100).view.map(_.toByte).toArray)

  "ReactiveMongo" should {

    "store a file in gridfs" in {
      val actual = Await.result(gfs.save(fileContent, file), timeout)
      actual.filename mustEqual "somefile"
    }

    "find this file in gridfs" in {
      val futureFile = gfs.find(BSONDocument("filename" -> "somefile")).collect[List]()
      val actual = Await.result(futureFile, timeout).head
      (actual.filename mustEqual file.filename) and
      (actual.uploadDate must beSome) and
      (actual.contentType mustEqual file.contentType)
    }

    "delete this file from gridfs" in {
      val actual = Await.result(gfs.remove(file.id), timeout)
      actual.n mustEqual 1
    }
  }
}
