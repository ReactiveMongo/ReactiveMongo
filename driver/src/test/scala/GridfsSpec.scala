import play.api.libs.iteratee._
import reactivemongo.api.gridfs.{ ReadFile, DefaultFileToSave, GridFS }
import reactivemongo.api.gridfs.Implicits._
import reactivemongo.bson._
import scala.concurrent._
import reactivemongo.api.gridfs

object GridfsSpec extends org.specs2.mutable.Specification {
  "GridFS" title

  import Common._

  sequential

  lazy val gfs = GridFS(db)

  lazy val file = DefaultFileToSave(Some("somefile"), Some("application/file"))
  lazy val fileContent = Enumerator((1 to 100).view.map(_.toByte).toArray)

  "ReactiveMongo" should {
    "store a file in gridfs" in {
      gfs.save(fileContent, file).map(_.filename).
        aka("filename") must beSome("somefile").await(timeoutMillis)
    }

    "find this file in gridfs" in {
      val futureFile = gfs.find(BSONDocument("filename" -> "somefile")).collect[List]()
      val actual = Await.result(futureFile, timeout).head
      (actual.filename mustEqual file.filename) and
        (actual.uploadDate must beSome) and
        (actual.contentType mustEqual file.contentType)
      import scala.collection.mutable.ArrayBuilder
      val res = Await.result(gfs.enumerate(actual) |>>> Iteratee.fold(ArrayBuilder.make[Byte]()) { (result, arr) =>
        result ++= arr
      }, timeout)
      res.result mustEqual ((1 to 100).map(_.toByte).toArray)
    }

    "delete this file from gridfs" in {
      gfs.remove(file.id).map(_.n) must beEqualTo(1).await(timeoutMillis)
    }
  }
}
