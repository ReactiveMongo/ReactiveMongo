import org.specs2.mutable._
import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.api.commands.bson.BSONCountCommand._
import reactivemongo.api.commands.bson.BSONCountCommandImplicits._
import scala.concurrent._
import scala.util.Failure

class CommonUseCases extends Specification {
  import Common._

  sequential

  lazy val collection = db("somecollection_commonusecases")

  "ReactiveMongo" should {
    "create a collection" in {
      collection.create() must beEqualTo({}).await(timeoutMillis)
    }

    // TODO !!!
    /*
    "insert some docs from an enumerator of docs" in {
      val enum = Enumerator((18 to 60).map(i => BSONDocument("age" -> BSONInteger(i), "name" -> BSONString("Jack" + i))): _*)
      //Await.result(collection.bulkInsert(enum, 100), timeout) mustEqual 43
      Await.result(collection.bulkInsert(enum, 100), timeout) mustEqual 43
    }
    "insert from an empty enumerator of docs" in {
      val enum = Enumerator[BSONDocument]()
      Await.result(collection.bulkInsert(enum, 100), timeout) mustEqual 0
    }*/

    "insert some docs from a seq of docs" in {
      val docs = (18 to 60).toStream.map(i => BSONDocument(
        "age" -> BSONInteger(i), "name" -> BSONString("Jack" + i)))

      (for {
        result <- collection.bulkInsert(docs, ordered = true)
        count <- collection.runValueCommand(Count(BSONDocument(
          "age" -> BSONDocument("$gte" -> 18, "$lte" -> 60))))
      } yield count) must beEqualTo(43).await(timeoutMillis)
    }

    "insert from an empty enumerator of docs" in {
      val docs = Stream.empty[BSONDocument]

      collection.bulkInsert(docs, ordered = true).map(_.n) must beEqualTo(0).
        await(timeoutMillis)
    }

    "find them" in {
      // batchSize (>1) allows us to test cursors ;)
      val it = collection.find(BSONDocument()).
        options(QueryOpts().batchSize(2)).cursor[BSONDocument]()

      it.collect[List]().map(_.map(_.getAs[BSONInteger]("age").get.value).
        mkString("")) must beEqualTo((18 to 60).mkString("")).
        await((timeoutMillis * 1.5D).toInt)
    }

    "find by regexp" in {
      collection.find(BSONDocument("name" -> BSONRegex("ack2", ""))).
        cursor[BSONDocument]().collect[List]().map(_.size).
        aka("size") must beEqualTo(10).await(timeoutMillis)
    }

    "find by regexp with flag" in {
      val query =
        BSONDocument(
          "$or" -> BSONArray(
            BSONDocument("name" -> BSONRegex("^jack2", "i")),
            BSONDocument("name" -> BSONRegex("^jack3", "i"))))

      collection.find(query).cursor[BSONDocument]().collect[List]().
        map(_.size) aka "size" must beEqualTo(20).await(timeoutMillis)
    }

    "find them with a projection" in {
      val pjn = BSONDocument("name" -> BSONInteger(1), "age" -> BSONInteger(1), "something" -> BSONInteger(1))
      def it = collection.find(BSONDocument(), pjn).options(QueryOpts().batchSize(2)).cursor[BSONDocument]()

      it.collect[List]().map(_.map(
        _.getAs[BSONInteger]("age").get.value).mkString("")).
        aka("all") must beEqualTo((18 to 60).mkString("")).
        await((timeoutMillis * 1.5).toInt)
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

    "insert a weird doc" in {
      val doc = BSONDocument("coucou" -> BSONString("coucou"), "plop" -> BSONInteger(1), "plop" -> BSONInteger(2))

      collection.insert(doc).map(_.ok) must beTrue.await(timeoutMillis)
    }

    "find this weird doc" in {
      collection.find(BSONDocument("coucou" -> BSONString("coucou"))).
        one[BSONDocument] must beSome.await(timeoutMillis)
    }

    "fail with this error" in {
      val query = BSONDocument("$and" ->
        BSONDocument("name" -> BSONString("toto")))

      Await.result(collection.find(query).one[BSONDocument], timeout).
        aka("findOne") must throwA[Exception]
    }
  }
}
