import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import play.api.libs.iteratee._

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.gridfs.{ ReadFile, DefaultFileToSave, GridFS }
import reactivemongo.api.gridfs.Implicits._
import reactivemongo.bson._
import reactivemongo.bson.utils.Converters

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
    val filename1 = s"file1-${System identityHashCode gfs}"
    lazy val file1 = DefaultFileToSave(Some(filename1), Some("application/file"))
    lazy val content1 = (1 to 100).view.map(_.toByte).toArray

    "store a file without a computed MD5" in { implicit ee: EE =>
      gfs.save(Enumerator(content1), file1).map(_.filename).
        aka("filename") must beSome(filename1).await(1, timeout)
    }

    val filename2 = s"file2-${System identityHashCode content1}"
    lazy val file2 = DefaultFileToSave(Some(filename2), Some("text/plain"))
    lazy val content2 = (100 to 200).view.map(_.toByte).toArray

    "store a file with computed MD5" in { implicit ee: EE =>
      gfs.saveWithMD5(Enumerator(content2), file2).map(_.filename).
        aka("filename") must beSome(filename2).await(1, timeout)
    }

    "find the files" in { implicit ee: EE =>
      def find(n: String): Future[Option[GFile]] =
        gfs.find(BSONDocument("filename" -> n)).headOption

      def matchFile(
        actual: GFile,
        expected: DefaultFileToSave,
        content: Array[Byte]) = actual.filename must_== expected.filename and (
        actual.uploadDate must beSome) and (actual.contentType must_== expected.contentType) and {
          import scala.collection.mutable.ArrayBuilder
          def res = gfs.enumerate(actual) |>>>
            Iteratee.fold(ArrayBuilder.make[Byte]()) { _ ++= _ }

          val buf = new java.io.ByteArrayOutputStream()

          res.map(_.result()) must beEqualTo(content).await(1, timeout) and {
            gfs.readToOutputStream(actual, buf).
              map(_ => buf.toByteArray) must beEqualTo(content).await(1, timeout)
          }
        }

      find(filename1) aka "file #1" must beSome[GFile].
        which(matchFile(_, file1, content1)).await(1, timeout) and {
          find(filename2) aka "file #2" must beSome[GFile].which { actual =>
            def expectedMd5 = Converters.hex2Str(Converters.md5(content2))

            matchFile(actual, file2, content2) and {
              actual.md5 must beSome[String].which {
                _ aka "MD5" must_== expectedMd5
              }
            }
          }.await(1, timeout)
        }
    }

    "delete the files from GridFS" in { implicit ee: EE =>
      (for {
        a <- gfs.remove(file1.id).map(_.n)
        b <- gfs.remove(file2.id).map(_.n)
      } yield a + b) must beEqualTo(2).await(1, timeout)
    }
  }
}
