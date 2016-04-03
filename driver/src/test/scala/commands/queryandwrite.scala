import scala.concurrent._
import scala.util.{ Try, Failure }

import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.api.commands.Command
import reactivemongo.api.commands.bson._
import BSONCommonWriteCommandsImplicits._
import BSONInsertCommand._
import BSONInsertCommandImplicits._
import BSONCountCommand._
import BSONCountCommandImplicits._
import BSONIsMasterCommand._
import BSONIsMasterCommandImplicits._

import org.specs2.concurrent.{ ExecutionEnv => EE }

object QueryAndWriteCommands extends org.specs2.mutable.Specification {
  import Common._

  sequential

  lazy val collection = db("queryandwritecommandsspec")

  "ReactiveMongo" should {
    "insert 1 doc and retrieve it" in { implicit ee: EE =>
      val doc = BSONDocument("_id" -> BSONNull, "name" -> "jack", "plop" -> -1)

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

    "insert 1 doc with collection.insert and retrieve it" in { implicit ee: EE =>
      val doc = BSONDocument("name" -> "joe", "plop" -> -2)

      collection.insert(doc).map(_.ok) must beTrue.await(1, timeout) and {
        collection.find(doc).cursor[BSONDocument]().collect[List]().map(_.size).
          aka("result count") must beEqualTo(1).await(1, timeout)
      }
    }
  }

  import reactivemongo.api.collections.bson._

  "ReactiveMongo with new collections" should {
    "insert 1 doc and retrieve it" in { implicit ee: EE =>
      val coll = db.collection("queryandwritecommandsspec")
      val doc = BSONDocument("name" -> "Stephane")

      coll.insert(doc).map(_.ok) must beTrue.await(1, timeout) and (
        coll.find(doc).cursor[BSONDocument]().collect[List]().map(_.size).
        aka("result size") must beEqualTo(1).await(1, timeout))
    }

    val nDocs = 1000000
    s"insert $nDocs in bulks (including 3 duplicate errors)" in {
      implicit ee: EE =>
        import reactivemongo.api.indexes._
        import reactivemongo.api.indexes.IndexType.Ascending

        val coll = db.collection("queryandwritecommandsspec4")
        val created = coll.indexesManager.ensure(
          Index(List("plop" -> Ascending), unique = true))
        Await.result(created, timeout)

        val docs = (0 until nDocs).toStream.map { i =>
          if (i == 0 || i == 1529 || i == 3026 || i == 19862) {
            BSONDocument("bulk" -> true, "i" -> i, "plop" -> -3)
          } else BSONDocument("bulk" -> true, "i" -> i, "plop" -> i)
        }

        coll.bulkInsert(docs, false).map(_ => {}) must beEqualTo({}).
          await(1, timeout * (nDocs / 2)) and {
            coll.count(Some(BSONDocument("bulk" -> true))).
              aka("count") must beEqualTo(nDocs - 3).await(1, timeout)
            // all docs minus errors
          }
    }
  }
}
