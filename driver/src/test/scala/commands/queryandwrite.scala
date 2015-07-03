//import play.api.libs.iteratee.Enumerator
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
//import InsertCommandImplicits._
//import BSONGetLastErrorImplicits.LastErrorReader

object QueryAndWriteCommands extends org.specs2.mutable.Specification {
  import Common._

  sequential

  lazy val collection = db("queryandwritecommandsspec")

  "ReactiveMongo" should {
    "insert 1 doc and retrieve it" in {
      val doc = BSONDocument("_id" -> BSONNull, "name" -> "jack", "plop" -> -1)
      val lastError = Await.result(Command.run(BSONSerializationPack)(collection, Insert(doc, doc)), timeout)
      println(lastError)
      lastError.ok mustEqual true
      /*val lastError2 = Await.result(Command.run(BSONSerializationPack)(collection,Insert(true)(doc)), timeout)
      println(lastError2)
      lastError2.ok mustEqual true*/
      val found = Await.result(collection.find(doc).cursor[BSONDocument].collect[List](), timeout)
      found.size mustEqual 1
      val count = Await.result(Command.run(BSONSerializationPack).unboxed(collection, Count(BSONDocument())), timeout)
      count mustEqual 1

      val ismaster = Await.result(Command.run(BSONSerializationPack)(db, IsMaster), timeout)
      println(ismaster)
      ok
    } tag ("mongo2_6")
    "insert 1 doc with collection.insert and retrieve it" in {
      val doc = BSONDocument("name" -> "joe", "plop" -> -2)
      val lastError = Await.result(collection.insert(doc), timeout)
      println(lastError)
      lastError.ok mustEqual true
      /*val lastError2 = Await.result(Command.run(BSONSerializationPack)(collection,Insert(true)(doc)), timeout)
      println(lastError2)
      lastError2.ok mustEqual true*/
      val found = Await.result(collection.find(doc).cursor[BSONDocument].collect[List](), timeout)
      found.size mustEqual 1
    }
  }

  import reactivemongo.api.collections.bson._
  import scala.concurrent.duration._

  val nDocs = 1000000
  "ReactiveMongo with new colls" should {
    "insert 1 doc and retrieve it" in {
      val coll = BSONCollection(db, "queryandwritecommandsspec", collection.failoverStrategy)
      val doc = BSONDocument("name" -> "Stephane")
      val lastError = Await.result(coll.insert(doc), timeout)
      println(lastError)
      lastError.ok mustEqual true
      val list = Await.result(coll.find(doc).cursor[BSONDocument].collect[List](), timeout)
      println(list)
      list.size mustEqual 1
    }

    s"Insert $nDocs in bulks (including 3 duplicate errors)" in {
      import reactivemongo.api.indexes._
      import reactivemongo.api.indexes.IndexType.Ascending
      val created = collection.indexesManager.ensure(Index(List("plop" -> Ascending), unique = true))
      Await.result(created, timeout)
      val start = System.currentTimeMillis
      val coll = BSONCollection(db, "queryandwritecommandsspec", collection.failoverStrategy)
      val docs = (0 until nDocs).toStream.map { i =>
        if (i == 0 || i == 1529 || i == 3026 || i == 19862) {
          BSONDocument("bulk" -> true, "i" -> i, "plop" -> -3)
        }
        else BSONDocument("bulk" -> true, "i" -> i, "plop" -> i)
      }
      val res = Try(Await.result(coll.bulkInsert(docs, false), DurationInt(100).seconds))
      println(res)
      if (res.isFailure) {
        throw res.failed.get
      }
      //println(s"writeErrors: ${res.get.map(_.writeErrors)}")
      //println(s"writeConcernError: ${res.get.map(_.writeConcernError)}")
      println(s"took ${System.currentTimeMillis - start} ms")
      val count = Await.result(Command.run(BSONSerializationPack).unboxed(collection, Count(BSONDocument("bulk" -> true))), timeout)
      count mustEqual (nDocs - 3) // all docs minus errors
    } tag ("mongo2_6")
  }

}
