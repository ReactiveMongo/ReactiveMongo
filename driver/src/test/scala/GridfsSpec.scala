import play.api.libs.iteratee._

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.gridfs.{
  DefaultFileToSave,
  GridFS,
  Implicits,
  ReadFile
}, Implicits._
import reactivemongo.bson._

import org.specs2.concurrent.{ ExecutionEnv => EE }

class GridfsSpec extends org.specs2.mutable.Specification {
  "GridFS" title

  import Common._

  sequential

  lazy val gfs = GridFS(db)

  lazy val file = DefaultFileToSave(Some("somefile"), Some("application/file"))
  lazy val bytes = (1 to 100).view.map(_.toByte).toArray
  def fileContent = Enumerator(bytes)

  "ReactiveMongo" should {
    "store a file in gridfs" in { implicit ee: EE =>
      gfs.save(fileContent, file).map(_.filename).
        aka("filename") must beSome("somefile").await(1, timeout)
    }

    "find this file in gridfs" in { implicit ee: EE =>
      gfs.find(BSONDocument("filename" -> "somefile")).
        headOption must beSome[ReadFile[BSONSerializationPack.type, BSONValue]].
        which { actual =>
          (actual.filename must_== file.filename) and
            (actual.uploadDate must beSome) and
            (actual.contentType must_== file.contentType) and
            (actual.length must_== 100) and {
              import scala.collection.mutable.ArrayBuilder
              def res = gfs.enumerate(actual) |>>> Iteratee.fold(ArrayBuilder.make[Byte]()) { (result, arr) =>
                result ++= arr
              }

              res.map(_.result()) must beEqualTo(bytes).await(1, timeout)
            }
        }.await(1, timeout)
    }

    "delete this file from gridfs" in { implicit ee: EE =>
      gfs.remove(file.id).map(_.n) must beEqualTo(1).await(1, timeout)
    }
  }
}
