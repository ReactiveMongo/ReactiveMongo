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
      val enum = Enumerator((18 to 60).map(i => BSONDocument("age" -> BSONInteger(i), "name" -> BSONString("Jack" + i))) :_*)
      Await.result(collection.insert(enum, 100), timeout) mustEqual 43
    }
    "find them" in {
      // batchSize (>1) allows us to test cursors ;)
      val it = collection.find(BSONDocument(), QueryOpts().batchSize(2))
      Await.result(it.toList, timeout).map(_.getAs[BSONInteger]("age").get.value).mkString("") mustEqual (18 to 60).mkString("")
    }
    "find by regexp" in {
      Await.result(collection.find(BSONDocument("name" -> BSONRegex("ack2", ""))).toList, timeout).size mustEqual 10
    }
    "insert a document containing a merged array of objects, fetch and check it" in {
      val array = BSONArray(
        BSONDocument(
          "entry" -> BSONInteger(1),
          "type" -> BSONString("telephone"),
          "professional" -> BSONBoolean(true),
          "value" -> BSONString("+331234567890")
        )
      )
      val array2 = BSONArray(
        BSONDocument(
          "entry" -> BSONInteger(2),
          "type" -> BSONString("mail"),
          "professional" -> BSONBoolean(true),
          "value" -> BSONString("joe@plop.com")
        )
      )
      val doc = BSONDocument(
        "name" -> BSONString("Joe"),
        "contacts" -> (array ++ array2)
      )
      Await.result(collection.insert(doc), timeout).ok mustEqual true
      val fetched = Await.result(collection.find(BSONDocument("name" -> BSONString("Joe"))).headOption, timeout)
      fetched.isDefined mustEqual true
      val contactsString = fetched.get.getAs[BSONArray]("contacts").get.iterator.map { _.value match {
        case contact: BSONDocument =>
          contact.toTraversable.getAs[BSONString]("type").get.value + ":" +
          contact.toTraversable.getAs[BSONString]("value").get.value
      }}.mkString(",")
      contactsString mustEqual "telephone:+331234567890,mail:joe@plop.com"
    }
    "insert a weird doc" in {
      val doc = BSONDocument("coucou" -> BSONString("coucou"), "plop" -> BSONInteger(1), "plop" -> BSONInteger(2))
      val result = Await.result(collection.insert(doc), timeout)
      println("\n" + result + "\n")
      result.ok mustEqual true
    }
    "find this weird doc" in {
      val doc = Await.result(collection.find(BSONDocument("coucou" -> BSONString("coucou"))).headOption, timeout)
      println("\n" + doc.map(BSONDocument.pretty(_)) + "\n")
      doc.isDefined mustEqual true
    }
    "fail with this error" in {
      val query = BSONDocument("$and" -> BSONDocument("name" -> BSONString("toto")))
      val future = collection.find(query).headOption
      Await.ready(future, timeout)
      (future.value.get match { case f:Failure[_] => true; case _ => false}) mustEqual true
    }
  }
}