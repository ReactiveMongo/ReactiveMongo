import scala.concurrent._
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.bson.BSONCountCommand._
import reactivemongo.api.commands.bson.BSONCountCommandImplicits._

import org.specs2.concurrent.ExecutionEnv

import _root_.tests.Common

final class CommonUseCases(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification
  with org.specs2.specification.AfterAll {

  "Common use cases" title

  sequential

  // ---

  import Common.{ timeout, slowTimeout }

  lazy val (db, slowDb) = Common.databases(s"reactivemongo-usecases-${System identityHashCode this}", Common.connection, Common.slowConnection)

  val colName = s"commonusecases${System identityHashCode this}"
  lazy val collection = db(colName)
  lazy val slowColl = slowDb(colName)

  def afterAll = { db.drop(); () }

  // ---

  "ReactiveMongo" should {
    "create a collection" in {
      collection.create() must beTypedEqualTo({}).await(1, timeout)
    }

    "insert some docs from a seq of docs" in {
      val docs = (18 to 60).toStream.map(i => BSONDocument(
        "age" -> BSONInteger(i), "name" -> BSONString("Jack" + i)))

      (for {
        result <- collection.insert[BSONDocument](ordered = true).many(docs)
        count <- collection.runValueCommand(
          Count(BSONDocument(
            "age" -> BSONDocument("$gte" -> 18, "$lte" -> 60))),
          ReadPreference.Primary)
      } yield count) must beEqualTo(43).await(1, timeout)
    }

    "find them" in {
      // batchSize (>1) allows us to test cursors ;)
      val it = collection.find(BSONDocument()).
        options(QueryOpts().batchSize(2)).cursor[BSONDocument]()

      //import reactivemongo.core.protocol.{ Response, Reply }
      //import reactivemongo.api.tests.{ makeRequest => req, nextResponse }

      it.collect[List](Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]()).
        map(_.map(_.getAs[BSONInteger]("age").get.value).
          mkString("")) must beEqualTo((18 to 60).mkString("")).
        await(1, timeout * 2)

    }

    "find by regexp" in {
      collection.find(BSONDocument("name" -> BSONRegex("ack2", ""))).
        cursor[BSONDocument]().
        collect[List](Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]()).
        map(_.size) must beEqualTo(10).await(1, timeout)
    }

    "find by regexp with flag" in {
      val query =
        BSONDocument(
          "$or" -> BSONArray(
            BSONDocument("name" -> BSONRegex("^jack2", "i")),
            BSONDocument("name" -> BSONRegex("^jack3", "i"))))

      collection.find(query).cursor[BSONDocument]().
        collect[List](Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]()).
        map(_.size) aka "size" must beEqualTo(20).await(1, timeout)
    }

    "find them with a projection" >> {
      val pjn = BSONDocument("name" -> 1, "age" -> 1, "something" -> 1)

      def findSpec(c: BSONCollection, t: FiniteDuration) = {
        def it = c.find(BSONDocument.empty, pjn).
          options(QueryOpts().batchSize(2)).cursor[BSONDocument]()

        //import reactivemongo.core.protocol.{ Response, Reply }
        //import reactivemongo.api.tests.{ makeRequest => req, nextResponse }

        it.collect[List](
          Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]()).map {
            _.map(_.getAs[BSONInteger]("age").get.value).mkString("")
          } must beEqualTo((18 to 60).mkString("")).await(0, t)
      }

      "with the default connection" in {
        findSpec(collection, timeout)
      }

      "with the slow connection" in eventually(2, timeout) {
        val t = Common.ifX509(slowTimeout * 5)(slowTimeout * 2)

        findSpec(slowColl, t)
      }
    }

    "insert a document containing a merged array of objects, fetch and check it" in {
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

      Await.result(collection.insert(doc), timeout).ok must beTrue

      val fetched = Await.result(collection.find(BSONDocument("name" -> BSONString("Joe"))).one[BSONDocument], timeout)

      fetched.isDefined mustEqual true
      val contactsString = fetched.get.getAs[BSONArray]("contacts").
        get.values.collect {
          case contact: BSONDocument =>
            contact.getAs[BSONString]("type").get.value + ":" +
              contact.getAs[BSONString]("value").get.value
        }.mkString(",")

      contactsString mustEqual "telephone:+331234567890,mail:joe@plop.com"
    }

    "insert a weird doc" in {
      val doc = BSONDocument("coucou" -> BSONString("coucou"), "plop" -> BSONInteger(1), "plop" -> BSONInteger(2))

      collection.insert(doc).map(_.ok) must beTrue.await(1, timeout)
    }

    "find this weird doc" in {
      collection.find(BSONDocument("coucou" -> BSONString("coucou"))).
        one[BSONDocument] must beSome.await(1, timeout)
    }

    "fail with this error" in {
      val query = BSONDocument("$and" ->
        BSONDocument("name" -> BSONString("toto")))

      Await.result(collection.find(query).one[BSONDocument], timeout).
        aka("findOne") must throwA[Exception]
    }
  }
}
