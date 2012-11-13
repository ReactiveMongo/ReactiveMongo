import org.specs2.mutable._
import play.api.libs.iteratee.Enumerator
import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.core.commands.Count
import scala.concurrent._

class CommonUseCases extends Specification {
  import Common._

  sequential

  lazy val collection = db("somecollection_commonusecases")

  "ReactiveMongo" should {
    "create a collection" in {
      Await.result(collection.create(), timeout) mustEqual true
    }
    "insert some docs from an enumerator of docs" in {
      val enum = Enumerator((18 to 60).map(i => BSONDocument("age" -> BSONInteger(i), "name" -> BSONString("Jack"))) :_*)
      Await.result(collection.insert(enum, 100), timeout) mustEqual 43
    }
    "find them" in {
      val it = collection.find(BSONDocument())
      Await.result(it.toList, timeout).map(_.getAs[BSONInteger]("age").get.value).mkString("") mustEqual (18 to 60).mkString("")
    }
  }
}