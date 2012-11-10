import org.specs2.mutable._
import reactivemongo.bson._
import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.bson.handlers.DefaultBSONHandlers._
import scala.concurrent._
import scala.concurrent.util.duration._
import reactivemongo.core.commands.Count

class CollectionSpec extends Specification with org.specs2.matcher.ThrownExpectations {
  import ExecutionContext.Implicits.global

  val timeout = intToDurationInt(5).seconds

  val connection = MongoConnection(List("localhost:27017"))
  val db = connection("specs2-test-reactivemongo")

  val dropping = db.drop()
  Await.ready(dropping, timeout)

  "ReactiveMongo" should {
    "administrate a collection" in {
      create
      convert
      checkCapped
      insert
      empty
      drop
    }
  }

  def create = {
    val collection = db("somecollection")
    Await.result(collection.create(), timeout) mustEqual true
  }

  def convert = {
    val collection = db("somecollection")
    Await.result(collection.convertToCapped(2 * 1024 * 1024, None), timeout) mustEqual true
  }

  def checkCapped = {
    val collection = db("somecollection")
    // convertToCapped is async. Let's wait a little while before checking if it's done
    Await.result(reactivemongo.utils.ExtendedFutures.DelayedFuture(4000, MongoConnection.system), timeout)
    println("\n\n\t***** CHECKING \n\n")
    val stats = Await.result(collection.stats, timeout)
    println(stats)
    stats.capped mustEqual true
  }

  def insert = {
    val collection = db("somecollection")
    Await.result(collection.insert(BSONDocument("name" -> BSONString("Jack"))), timeout).ok mustEqual true
    Await.result(db.command(Count("somecollection")), timeout) mustEqual 1
  }

  def empty = {
    val collection = db("somecollection")
    Await.result(collection.emptyCapped(), timeout) mustEqual true
    Await.result(db.command(Count("somecollection")), timeout) mustEqual 0
  }

  def drop = {
    val collection = db("somecollection")
    Await.result(collection.drop(), timeout) mustEqual true
  }
}