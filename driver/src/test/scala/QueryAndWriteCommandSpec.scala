import scala.concurrent.Future
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

import org.specs2.concurrent.ExecutionEnv

class QueryAndWriteCommandSpec(
  implicit
  ee: ExecutionEnv) extends org.specs2.mutable.Specification {

  import Common._

  sequential

  "ReactiveMongo" should {
    "insert 1 document and retrieve it" >> {
      val colName1 = s"queryandwritecommandspec${System identityHashCode this}"
      lazy val coll1 = db(colName1)
      lazy val slowColl1 = slowDb(colName1)

      val doc = BSONDocument("_id" -> BSONNull, "name" -> "jack", "plop" -> -1)
      val runner = Command.run(BSONSerializationPack, db.failoverStrategy)

      def test1(c: BSONCollection, timeout: FiniteDuration) = {
        runner(
          coll1, Insert(doc, doc), ReadPreference.primary).
          map(_.ok) must beTrue.await(1, timeout) and {
            coll1.find(doc).cursor[BSONDocument]().
              collect[List](-1, Cursor.FailOnError[List[BSONDocument]]()).
              map(_.size) must beEqualTo(1).await(1, timeout)

          } and {
            runner.unboxed(coll1, Count(BSONDocument()),
              ReadPreference.primary) must beEqualTo(1).await(1, timeout)
          } and {
            runner(db, IsMaster, ReadPreference.primary).
              map(_ => {}) must beEqualTo({}).await(1, timeout)
          }
      }

      "with the default connection" in {
        test1(coll1, timeout)
      }

      "with the slow connection" in {
        test1(slowColl1, slowTimeout)
      }
    }

    "use bulks" >> {
      s"to insert (including 3 duplicate errors)" >> {
        val nDocs = 1000000
        def colName(n: Int) = s"bulked${System identityHashCode this}_${n}"

        import reactivemongo.api.indexes._
        import reactivemongo.api.indexes.IndexType.Ascending

        def bulkSpec(
          c: BSONCollection,
          n: Int,
          e: Int,
          timeout: FiniteDuration) = {
          @inline def docs = (0 until n).toStream.map { i =>
            if (i == 0 || i == 1529 || i == 3026 || i == 19862) {
              BSONDocument("bulk" -> true, "i" -> i, "plop" -> -3)
            } else BSONDocument("bulk" -> true, "i" -> i, "plop" -> i)
          }

          c.create().flatMap(_ => c.indexesManager.ensure(
            Index(List("plop" -> Ascending), unique = true)).map(_ => {})).
            aka("index") must beEqualTo({}).await(1, timeout) and {
              c.bulkInsert(docs, false).map(_ => {}) must beEqualTo({}).
                await(1, timeout * (n / 2L)) and {
                  c.count(Some(BSONDocument("bulk" -> true))).
                    aka("count") must beEqualTo(e).await(1, timeout)
                  // all docs minus errors
                }
            }
        }

        s"$nDocs documents with the default connection" in {
          bulkSpec(db(colName(nDocs)), nDocs, nDocs - 3, timeout)
        }

        s"${nDocs / 1000} with the slow connection" in {
          bulkSpec(
            slowDb(colName(nDocs / 1000)),
            nDocs / 1000, nDocs / 1000, slowTimeout)
        }
      }

      {
        lazy val coll = db(s"bulked${System identityHashCode this}_2")

        "to insert" in {
          val docs = Stream.empty[BSONDocument]

          coll.insert[BSONDocument](ordered = true).
            many(docs).map(_.n) must beEqualTo(0).await(1, timeout)
        }

        "to update" in {
          val builder = coll.update(ordered = false)

          Future.sequence(Seq(
            builder.element(
              q = BSONDocument("upsert" -> 1),
              u = BSONDocument("i" -> -1, "foo" -> "bar"),
              upsert = true,
              multi = false),
            builder.element(
              q = BSONDocument("i" -> BSONDocument(f"$$lte" -> 3)),
              u = BSONDocument(f"$$set" -> BSONDocument("foo" -> "bar")),
              upsert = false,
              multi = true))).flatMap(builder.many(_)).map { r =>
            (r.n, r.nModified, r.upserted.size)
          } must beEqualTo((2, 0, 1)).await(0, timeout)
        }
      }
    }
  }
}
