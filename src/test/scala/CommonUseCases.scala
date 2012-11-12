import org.specs2.mutable._
import play.api.libs.iteratee.Enumerator
import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.core.commands.Count
import scala.concurrent._

object CommonUseCases extends Specification {
  import Common._

  lazy val collection = db("somecollection_commonusecases")

  "ReactiveMongo" should {
    "insert then find" in {
      create
      insert
      find
    }
  }

  def create = {
    Await.result(collection.create(), timeout) mustEqual true
  }
  
  def insert = {
    val enum = Enumerator((18 to 60).map(i => BSONDocument("age" -> BSONInteger(i), "name" -> BSONString("Jack"))) :_*)
    Await.result(collection.insert(enum, 100), timeout) mustEqual 43
  }

  def find = {
    val it = collection.find(BSONDocument())
    Await.result(it.toList, timeout).map(_.getAs[BSONInteger]("age").get.value).mkString("") mustEqual (18 to 60).mkString("")
  }
}