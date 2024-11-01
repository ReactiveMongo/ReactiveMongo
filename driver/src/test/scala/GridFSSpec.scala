import java.io.ByteArrayInputStream

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.{ Cursor, DB, WrappedCursor }
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.gridfs.FileToSave
import reactivemongo.api.tests.{ newBuilder, pack }

import org.specs2.concurrent.ExecutionEnv

final class GridFSSpec(
    implicit
    ee: ExecutionEnv)
    extends org.specs2.mutable.Specification
    with org.specs2.specification.AfterAll {

  "GridFS".title

  sequential
  stopOnFail

  // ---

  import tests.Common
  import Common.{ timeout, slowTimeout }

  lazy val (db, slowDb) = Common.databases(
    s"reactivemongo-gridfs-${System.identityHashCode(this)}",
    Common.connection,
    Common.slowConnection,
    retries = 1
  )

  def afterAll() = { db.drop(); () }

  // ---

  "Default connection" should {
    val prefix = s"fs${System.identityHashCode(db)}"

    gridFsSpec(db, prefix, timeout)
  }

  "Slow connection" should {
    val prefix = s"fs${System.identityHashCode(slowDb)}"

    gridFsSpec(slowDb, prefix, slowTimeout)
  }

  // ---

  def gridFsSpec(
      db: DB,
      prefix: String,
      timeout: FiniteDuration
    )(implicit
      ev: scala.reflect.ClassTag[pack.Value]
    ) = {

    val gfs = db.gridfs(prefix)
    type GFile = gfs.ReadFile[pack.Value]
    val builder = newBuilder(pack)
    import builder.{ document, elementProducer => elem, string => str }
    implicit def dw: pack.Writer[pack.Document] = pack.IdentityWriter

    val filename1 = s"file1-${System.identityHashCode(gfs)}"
    lazy val file1 = gfs.fileToSave(Some(filename1), Some("application/file"))

    lazy val content1 = (1 to 100).view.map(_.toByte).toArray

    "not exists before" in {
      gfs.exists must beFalse.awaitFor(timeout)
    }

    "ensure the indexes are ok" in {
      gfs.ensureIndex() must beTrue.await(2, timeout) and {
        gfs.exists must beTrue.awaitFor(timeout)
      } and {
        gfs.ensureIndex() must beFalse.awaitFor(timeout)
      }
    }

    "store a file without a computed MD5" in {
      def in = new ByteArrayInputStream(content1)

      gfs
        .writeFromInputStream(file1, in)
        .andThen { case _ => in.close() }
        .map(_.filename) must beSome(filename1).await(1, timeout)
    }

    val filename2 = s"file2-${System.identityHashCode(gfs)}"
    lazy val file2 = gfs.fileToSave(
      filename = Some(filename2),
      contentType = Some("text/plain"),
      uploadDate = None,
      metadata = document(Seq(elem("foo", str("bar")))),
      id = str(filename2)
    )

    lazy val content2 = (100 to 200).view.map(_.toByte).toArray

    def countFile2Chunks(): Future[Long] =
      db.collection(s"${prefix}.chunks")
        .count(Some(BSONDocument("files_id" -> file2.id)))

    "store a file with computed MD5" in {
      def in = new ByteArrayInputStream(content2)

      gfs
        .writeFromInputStream(
          file2,
          in,
          chunkSize = content2.size / 2 // enforce at least 2 chunks
        )
        .andThen { case _ => in.close() }
        .map(_.filename) must beSome(filename2).await(1, timeout) and {
        countFile2Chunks() must beTypedEqualTo(3L).await(1, timeout)
      }
    }

    lazy val customField = s"foo-${System.identityHashCode(content2)}"

    "update a file metadata" in {
      val update = document(
        Seq(elem(f"$$set", document(Seq(elem("_custom", str(customField))))))
      )

      gfs.update(file2.id, update).map(_.n) must beTypedEqualTo(1).awaitFor(
        timeout
      )
    }

    "find the files" in {
      def find(n: String): Future[Option[GFile]] =
        gfs
          .find[pack.Document, pack.Value](
            document(Seq(elem("filename", str(n))))
          )
          .headOption

      def matchFile(
          actual: GFile,
          expected: FileToSave[_, _],
          content: Array[Byte]
        ) = actual.filename must_=== expected.filename and {
        actual.uploadDate must beSome
      } and (actual.contentType must_=== expected.contentType) and {
        val buf = new java.io.ByteArrayOutputStream()

        gfs
          .readToOutputStream(actual, buf)
          .map(_ => buf.toByteArray) must beTypedEqualTo(content)
          .await(1, timeout) and {
          gfs
            .chunks(actual)
            .fold(Array.empty[Byte]) { _ ++ _ }
            .aka("chunks") must beTypedEqualTo(content).awaitFor(timeout)
        }
      }

      {
        import reactivemongo.api.CursorProducer

        type CP[T] = CursorProducer[T] { type ProducedCursor = FooCursor[T] }

        implicit def fooProducer[T]: CP[T] = new CursorProducer[T] {
          type ProducedCursor = FooCursor[T]

          def produce(base: Cursor.WithOps[T]): ProducedCursor =
            new DefaultFooCursor(base)
        }

        val cursor = gfs.find(document(Seq(elem("filename", str(filename1)))))

        cursor.foo must_=== "Bar" and {
          cursor.headOption must beSome[GFile]
            .which(matchFile(_, file1, content1))
            .await(1, timeout / 3L * 2L)
        }
      } and {
        find(filename2) aka "file #2" must beSome[GFile].which { actual =>
          def expectedMd5 = reactivemongo.api.tests.md5Hex(content2)

          matchFile(actual, file2, content2) and {
            actual.md5 must beSome[String].which {
              _ aka "MD5" must_=== expectedMd5
            }
          }
        }.await(1, timeout + (timeout / 3L))
      } and {
        gfs
          .find[pack.Document, pack.Value](
            document(Seq(elem("_custom", str(customField))))
          )
          .headOption
          .map(_.map(_.id)) must beSome(file2.id)
          .await(1, timeout + (timeout / 3L))
      }
    }

    "delete the files from GridFS" in {
      def spec(id: gfs.pack.Value) =
        gfs.remove(id).map(_.n) must beTypedEqualTo(1).awaitFor(timeout)

      spec(file1.id) and spec(file2.id) and {
        countFile2Chunks() must beTypedEqualTo(0L).await(1, timeout)
      }
    }
  }

  // ---

  private sealed trait FooCursor[T] extends Cursor[T] { def foo: String }

  private sealed trait FooExtCursor[T] extends FooCursor[T]

  private class DefaultFooCursor[T](val wrappee: Cursor[T])
      extends FooExtCursor[T]
      with WrappedCursor[T] {
    val foo = "Bar"
  }
}
