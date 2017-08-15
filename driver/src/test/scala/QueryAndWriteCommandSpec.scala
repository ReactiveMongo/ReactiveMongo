import scala.concurrent.duration.FiniteDuration

import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.api.commands.Command
import reactivemongo.api.commands.bson._
import reactivemongo.api.collections.bson.BSONCollection

import BSONCommonWriteCommandsImplicits._
import BSONInsertCommand._
import BSONInsertCommandImplicits._
import BSONCountCommand._
import BSONCountCommandImplicits._
import BSONIsMasterCommand._
import BSONIsMasterCommandImplicits._

import org.specs2.concurrent.{ ExecutionEnv => EE }

class QueryAndWriteCommandSpec extends org.specs2.mutable.Specification {
  import Common._

  sequential

  val colName = s"queryandwritecommandspec${System identityHashCode this}"
  lazy val collection = db(colName)
  lazy val slowColl = slowDb(colName)

  "ReactiveMongo" should {
    "insert 1 doc and retrieve it" >> {
      val doc = BSONDocument("_id" -> BSONNull, "name" -> "jack", "plop" -> -1)
      def test1(c: BSONCollection, timeout: FiniteDuration)(implicit ee: EE) = {
        Command.run(BSONSerializationPack)(collection, Insert(doc, doc)).
          map(_.ok) aka "inserted" must beTrue.await(1, timeout) and {
            collection.find(doc).cursor[BSONDocument]().
              collect[List]().map(_.size) must beEqualTo(1).await(1, timeout)
          } and {
            Command.run(BSONSerializationPack).
              unboxed(collection, Count(BSONDocument())) must beEqualTo(1).
              await(1, timeout)
          } and {
            Command.run(BSONSerializationPack)(db, IsMaster).map(_ => {}).
              aka("isMaster") must beEqualTo({}).await(1, timeout)
          }
      }

      "with the default connection" in { implicit ee: EE =>
        test1(collection, timeout)
      }

      "with the slow connection" in { implicit ee: EE =>
        test1(slowColl, slowTimeout)
      }
    }

    val nDocs = 1000000
    s"insert with bulks (including 3 duplicate errors)" >> {
      import reactivemongo.api.indexes._
      import reactivemongo.api.indexes.IndexType.Ascending

      def bulkSpec(c: BSONCollection, n: Int, e: Int, timeout: FiniteDuration)(implicit ee: EE) = {
        @inline def docs = (0 until n).toStream.map { i =>
          if (i == 0 || i == 1529 || i == 3026 || i == 19862) {
            BSONDocument("bulk" -> true, "i" -> i, "plop" -> -3)
          } else BSONDocument("bulk" -> true, "i" -> i, "plop" -> i)
        }

        c.indexesManager.ensure(
          Index(List("plop" -> Ascending), unique = true)).map(_ => {}).
          aka("index") must beEqualTo({}).await(1, timeout) and {
            c.bulkInsert(docs, false).map(_ => {}) must beEqualTo({}).
              await(1, timeout * (n / 2L)) and {
                c.count(Some(BSONDocument("bulk" -> true))).
                  aka("count") must beEqualTo(e).await(1, timeout)
                // all docs minus errors
              }
          }
      }

      def newName(n: Int) = s"queryandwritecommandsspec$n"

      s"$nDocs documents with the default connection" in { implicit ee: EE =>
        bulkSpec(db(newName(nDocs)), nDocs, nDocs - 3, timeout)
      }

      s"${nDocs / 1000} with the slow connection" in { implicit ee: EE =>
        bulkSpec(
          slowDb(newName(nDocs / 1000)),
          nDocs / 1000, nDocs / 1000, slowTimeout)
      }
    }

    "insert from an empty bulk of docs" in { implicit ee: EE =>
      val docs = Stream.empty[BSONDocument]

      collection.bulkInsert(docs, ordered = true).map(_.n) must beEqualTo(0).
        await(1, timeout)
    }
  }
}
