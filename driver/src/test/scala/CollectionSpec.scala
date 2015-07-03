import reactivemongo.bson.{ BSONDocument, BSONString }
import scala.concurrent.Await
import scala.util.{ Try, Success, Failure }
import org.specs2.mutable.{ Specification, Tags }

object CollectionSpec extends Specification with Tags {
  import Common._

  sequential

  lazy val collection = db("somecollection_collectionspec")

  "ReactiveMongo" should {
    "create a collection" in {
      collection.create() must beEqualTo(()).await(timeoutMillis)
    }

    "convert to capped" in {
      collection.convertToCapped(2 * 1024 * 1024, None) must beEqualTo(()).
        await(timeoutMillis)
    }

    "check if it's capped" in {
      // convertToCapped is async. Let's wait a little while before checking if it's done
      Await.result(reactivemongo.utils.ExtendedFutures.DelayedFuture(4000, connection.actorSystem), timeout)
      println("\n\n\t***** CHECKING \n\n")
      val stats = Await.result(collection.stats, timeout)
      println(stats)
      stats.capped mustEqual true
    }

    "insert some docs then test lastError result and finally count" in {
      val lastError = Await.result(collection.insert(BSONDocument("name" -> BSONString("Jack"))), timeout)
      lastError.ok mustEqual true
      //lastError.updated mustEqual 0
      // this fails with mongodb < 2.6 (n in insertions is always 0 in mongodb < 2.6)
      // lastError.n shouldEqual 1
      //lastError.updatedExisting mustEqual false
      //lastError.get("ok") mustEqual Some(BSONDouble(1))
      //lastError.getTry("ok") mustEqual Success(BSONDouble(1))
      //lastError.getAs[BSONDouble]("ok") mustEqual Some(BSONDouble(1))

      collection.count() must beEqualTo(1).await(timeoutMillis) and (
        collection.count(skip = 1) must beEqualTo(0).await(timeoutMillis)) and (
          collection.count(selector = Some(BSONDocument("name" -> "Jack"))).
          aka("matching count") must beEqualTo(1).await(timeoutMillis)) and (
            collection.count(selector = Some(BSONDocument("name" -> "Foo"))).
            aka("not matching count") must beEqualTo(0).await(timeoutMillis))
    }

    // Empty capped need to be enabled with enableTestCommands
    // see: http://docs.mongodb.org/manual/reference/command/emptycapped/#dbcmd.emptycapped
    /*"empty the capped collection" in {
      Await.result(collection.emptyCapped(), timeout) mustEqual true
      Await.result(db.command(Count(collection.name)), timeout) mustEqual 0
    } tag ("testCommands")*/

    "drop it" in {
      collection.drop() must beEqualTo(()).await(timeoutMillis)
    }
  }
}
