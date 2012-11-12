import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.core.commands.Count
import scala.concurrent._

import org.specs2.mutable._
object CollectionSpec extends Specification {
  import Common._

  lazy val collection = db("somecollection_collectionspec")

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
    Await.result(collection.create(), timeout) mustEqual true
  }

  def convert = {
    Await.result(collection.convertToCapped(2 * 1024 * 1024, None), timeout) mustEqual true
  }

  def checkCapped = {
    // convertToCapped is async. Let's wait a little while before checking if it's done
    Await.result(reactivemongo.utils.ExtendedFutures.DelayedFuture(4000, MongoConnection.system), timeout)
    println("\n\n\t***** CHECKING \n\n")
    val stats = Await.result(collection.stats, timeout)
    println(stats)
    stats.capped mustEqual true
  }

  def insert = {
    Await.result(collection.insert(BSONDocument("name" -> BSONString("Jack"))), timeout).ok mustEqual true
    Await.result(db.command(Count(collection.name)), timeout) mustEqual 1
  }

  def empty = {
    Await.result(collection.emptyCapped(), timeout) mustEqual true
    Await.result(db.command(Count(collection.name)), timeout) mustEqual 0
  }

  def drop = {
    Await.result(collection.drop(), timeout) mustEqual true
  }
}