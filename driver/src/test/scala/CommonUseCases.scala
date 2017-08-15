import scala.concurrent._
import scala.concurrent.duration.FiniteDuration

import org.specs2.mutable._

import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.bson.BSONCountCommand._
import reactivemongo.api.commands.bson.BSONCountCommandImplicits._

import org.specs2.concurrent.{ ExecutionEnv => EE }

class CommonUseCases extends Specification {
  import Common._

  sequential

  val colName = s"commonusecases${System identityHashCode this}"
  lazy val collection = db(colName)
  lazy val slowColl = slowDb(colName)

  "ReactiveMongo" should {
    "create a collection" in { implicit ee: EE =>
      collection.create() must beEqualTo({}).await(1, timeout)
    }

    "insert some docs from a seq of docs" in { implicit ee: EE =>
      val docs = (18 to 60).toStream.map(i => BSONDocument(
        "age" -> BSONInteger(i), "name" -> BSONString("Jack" + i)))

      (for {
        result <- collection.bulkInsert(docs, ordered = true)
        count <- collection.runValueCommand(Count(BSONDocument(
          "age" -> BSONDocument("$gte" -> 18, "$lte" -> 60))))
      } yield count) must beEqualTo(43).await(1, timeout)
    }

    "find them" in { implicit ee: EE =>
      // batchSize (>1) allows us to test cursors ;)
      val it = collection.find(BSONDocument()).
        options(QueryOpts().batchSize(2)).cursor[BSONDocument]()

      it.collect[List]().map(_.map(_.getAs[BSONInteger]("age").get.value).
        mkString("")) must beEqualTo((18 to 60).mkString("")).
        await(1, timeout * 2)
    }

    "find by regexp" in { implicit ee: EE =>
      collection.find(BSONDocument("name" -> BSONRegex("ack2", ""))).
        cursor[BSONDocument]().collect[List]().map(_.size).
        aka("size") must beEqualTo(10).await(1, timeout)
    }

    "find by regexp with flag" in { implicit ee: EE =>
      val query =
        BSONDocument(
          "$or" -> BSONArray(
            BSONDocument("name" -> BSONRegex("^jack2", "i")),
            BSONDocument("name" -> BSONRegex("^jack3", "i"))))

      collection.find(query).cursor[BSONDocument]().collect[List]().
        map(_.size) aka "size" must beEqualTo(20).await(1, timeout)
    }

    "find them with a projection" >> {
      def findSpec(c: BSONCollection, timeout: FiniteDuration)(implicit ee: EE) = {
        val pjn = BSONDocument(
          "name" -> BSONInteger(1),
          "age" -> BSONInteger(1),
          "something" -> BSONInteger(1))

        def it = c.find(BSONDocument(), pjn).
          options(QueryOpts().batchSize(2)).cursor[BSONDocument]()

        it.collect[List]().map(_.map(
          _.getAs[BSONInteger]("age").get.value).mkString("")) must beEqualTo((18 to 60).mkString("")).
          await(1, timeout * 2)
      }

      "with the default connection" in { implicit ee: EE =>
        findSpec(collection, timeout)
      }

      "with the slow connection" in { implicit ee: EE =>
        findSpec(slowColl, slowTimeout)
      }
    }

    "insert a document containing a merged array of objects, fetch and check it" in { implicit ee: EE =>
      val array = BSONArray(
        BSONDocument(
          "entry" -> BSONInteger(1),
          "type" -> BSONString("telephone"),
          "professional" -> BSONBoolean(true),
          "value" -> BSONString("+331234567890")))
      val array2 = BSONArray(
        BSONDocument(
          "entry" -> BSONInteger(2),
          "type" -> BSONString("mail"),
          "professional" -> BSONBoolean(true),
          "value" -> BSONString("joe@plop.com")))
      val doc = BSONDocument(
        "name" -> BSONString("Joe"),
        "contacts" -> (array ++ array2))

      Await.result(collection.insert(doc), timeout).ok mustEqual true

      val fetched = Await.result(collection.find(BSONDocument("name" -> BSONString("Joe"))).one[BSONDocument], timeout)
      fetched.isDefined mustEqual true
      val contactsString = fetched.get.getAs[BSONArray]("contacts").get.values.map {
        case contact: BSONDocument =>
          contact.getAs[BSONString]("type").get.value + ":" +
            contact.getAs[BSONString]("value").get.value
      }.mkString(",")

      contactsString mustEqual "telephone:+331234567890,mail:joe@plop.com"
    }

    "insert a weird doc" in { implicit ee: EE =>
      val doc = BSONDocument("coucou" -> BSONString("coucou"), "plop" -> BSONInteger(1), "plop" -> BSONInteger(2))

      collection.insert(doc).map(_.ok) must beTrue.await(1, timeout)
    }

    "find this weird doc" in { implicit ee: EE =>
      collection.find(BSONDocument("coucou" -> BSONString("coucou"))).
        one[BSONDocument] must beSome.await(1, timeout)
    }

    "fail with this error" in { implicit ee: EE =>
      val query = BSONDocument("$and" ->
        BSONDocument("name" -> BSONString("toto")))

      Await.result(collection.find(query).one[BSONDocument], timeout).
        aka("findOne") must throwA[Exception]
    }
  }
}
