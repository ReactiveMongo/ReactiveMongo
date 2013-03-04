import reactivemongo.api._
import reactivemongo.bson._
import DefaultBSONHandlers._
import reactivemongo.core.commands.Count
import scala.concurrent._

import org.specs2.mutable._
class CollectionSpec extends Specification {
  import Common._

  sequential

  lazy val collection = db("somecollection_collectionspec")

  "ReactiveMongo" should {
    "create a collection" in {
      Await.result(collection.create(), timeout) mustEqual true
    }
    "convert to capped" in {
      Await.result(collection.convertToCapped(2 * 1024 * 1024, None), timeout) mustEqual true
    }
    "check if it's capped" in {
      // convertToCapped is async. Let's wait a little while before checking if it's done
      Await.result(reactivemongo.utils.ExtendedFutures.DelayedFuture(4000, connection.actorSystem), timeout)
      println("\n\n\t***** CHECKING \n\n")
      val stats = Await.result(collection.stats, timeout)
      println(stats)
      stats.capped mustEqual true
    }
    "insert some docs then count" in {
      Await.result(collection.insert(BSONDocument("name" -> BSONString("Jack"))), timeout).ok mustEqual true
      Await.result(db.command(Count(collection.name)), timeout) mustEqual 1
    }
    "empty the capped collection" in {
      Await.result(collection.emptyCapped(), timeout) mustEqual true
      Await.result(db.command(Count(collection.name)), timeout) mustEqual 0
    }
    "drop it" in {
      Await.result(collection.drop(), timeout) mustEqual true
    }
  }
}