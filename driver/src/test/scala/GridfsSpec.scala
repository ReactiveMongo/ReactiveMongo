import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import play.api.libs.iteratee._

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.gridfs.{ ReadFile, DefaultFileToSave, GridFS }
import reactivemongo.api.gridfs.Implicits._
import reactivemongo.bson._
import reactivemongo.api.gridfs

import org.specs2.concurrent.{ ExecutionEnv => EE }

class GridFSSpec extends org.specs2.mutable.Specification {
  "GridFS" title

  import Common._

  sequential

  "Default connection" should {
    gridFsSpec(GridFS[BSONSerializationPack.type](db), timeout)
  }

  "Slow connection" should {
    gridFsSpec(GridFS[BSONSerializationPack.type](db), timeout)
  }

  // ---

  type GFile = ReadFile[BSONSerializationPack.type, BSONValue]

  def gridFsSpec(gfs: GridFS[BSONSerializationPack.type], timeout: FiniteDuration) = {
    val filename = s"somefile${System identityHashCode gfs}"
    lazy val file = DefaultFileToSave(Some(filename), Some("application/file"))

    lazy val fileContent = Enumerator((1 to 100).view.map(_.toByte).toArray)

    "store a file" in { implicit ee: EE =>
      gfs.save(fileContent, file).map(_.filename).
        aka("filename") must beSome(filename).await(1, timeout)
    }

    "find this file" in { implicit ee: EE =>
      def f: Future[Option[GFile]] =
        gfs.find(BSONDocument("filename" -> filename)).headOption

      f must beSome[GFile].which { actual =>
        actual.filename mustEqual file.filename and (
          actual.uploadDate must beSome) and (
            actual.contentType mustEqual file.contentType) and {
              import scala.collection.mutable.ArrayBuilder
              def res = gfs.enumerate(actual) |>>>
                Iteratee.fold(ArrayBuilder.make[Byte]()) { (result, arr) =>
                  result ++= arr
                }

              res.map(_.result()) must beEqualTo(
                (1 to 100).map(_.toByte).toArray).await(1, timeout)
            }
      }.await(1, timeout)
    }

    "delete this file from GridFS" in { implicit ee: EE =>
      gfs.remove(file.id).map(_.n) must beEqualTo(1).await(1, timeout)
    }
  }
}
