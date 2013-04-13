import org.specs2.mutable._
import play.api.libs.iteratee.Enumerator
import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.core.commands.Count
import scala.concurrent._
import scala.util.Failure

class CommonUseCases extends Specification {
  import Common._

  sequential

  lazy val collection = db("somecollection_commonusecases")

  "ReactiveMongo" should {
    "create a collection" in {
      Await.result(collection.create(), timeout) mustEqual true
    }
    "insert some docs from an enumerator of docs" in {
      val enum = Enumerator((18 to 60).map(i => BSONDocument("age" -> BSONInteger(i), "name" -> BSONString("Jack" + i))): _*)
      Await.result(collection.bulkInsert(enum, 100), timeout) mustEqual 43
    }
    "find them" in {
      // batchSize (>1) allows us to test cursors ;)
      val it = collection.find(BSONDocument()).options(QueryOpts().batchSize(2)).cursor
      Await.result(it.toList, timeout).map(_.getAs[BSONInteger]("age").get.value).mkString("") mustEqual (18 to 60).mkString("")
    }
    "find by regexp" in {
      Await.result(collection.find(BSONDocument("name" -> BSONRegex("ack2", ""))).cursor.toList, timeout).size mustEqual 10
    }
    "find them with a projection" in {
      val pjn = BSONDocument("name" -> BSONInteger(1), "age" -> BSONInteger(1), "something" -> BSONInteger(1))
      val it = collection.find(BSONDocument(), pjn).options(QueryOpts().batchSize(2)).cursor
      Await.result(it.toList, timeout).map(_.getAs[BSONInteger]("age").get.value).mkString("") mustEqual (18 to 60).mkString("")
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
      val fetched = Await.result(collection.find(BSONDocument("name" -> BSONString("Joe"))).one, timeout)
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
      val result = Await.result(collection.insert(doc), timeout)
      println("\n" + result + "\n")
      result.ok mustEqual true
    }
    "find this weird doc" in {
      val doc = Await.result(collection.find(BSONDocument("coucou" -> BSONString("coucou"))).one, timeout)
      println("\n" + doc.map(BSONDocument.pretty(_)) + "\n")
      doc.isDefined mustEqual true
    }
    "fail with this error" in {
      val query = BSONDocument("$and" -> BSONDocument("name" -> BSONString("toto")))
      val future = collection.find(query).one
      Await.ready(future, timeout)
      (future.value.get match { case Failure(e) => e.printStackTrace(); true; case _ => false }) mustEqual true
    }
  }
}