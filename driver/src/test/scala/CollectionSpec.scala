import reactivemongo.bson.{ BSONDocument, BSONString }
import reactivemongo.api.commands.CollStatsResult
import scala.concurrent.Await
import scala.util.{ Try, Success, Failure }
import org.specs2.mutable.{ Specification, Tags }

object CollectionSpec extends Specification with Tags {
  import Common._

  sequential

  lazy val collection = db("somecollection_collectionspec")

  val cappedMaxSize = 2 * 1024 * 1024

  "ReactiveMongo" should {
    "create a collection" in {
      collection.create() must beEqualTo(()).await(timeoutMillis)
    }

    "convert to capped" in {
      collection.convertToCapped(cappedMaxSize, None) must beEqualTo(()).
        await(timeoutMillis)
    }

    "check if it's capped (MongoDB <= 2.6)" in {
      // convertToCapped is async. Let's wait a little while before checking if it's done
      Await.result(reactivemongo.util.ExtendedFutures.DelayedFuture(4000, connection.actorSystem), timeout)
      collection.stats must beLike[CollStatsResult] {
        case stats => stats.capped must beTrue and (stats.maxSize must beNone)
      }.await(timeoutMillis)
    } tag ("mongo2", "mongo26")

    "check if it's capped (MongoDB >= 3.0)" in {
      // convertToCapped is async. Let's wait a little while before checking if it's done
      Await.result(reactivemongo.util.ExtendedFutures.DelayedFuture(4000, connection.actorSystem), timeout)
      collection.stats must beLike[CollStatsResult] {
        case stats => stats.capped must beTrue and (stats.maxSize must beSome(cappedMaxSize))
      }.await(timeoutMillis)
    } tag ("mongo3", "not_mongo26")

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
      collection.drop(false) must beTrue.await(timeoutMillis)
    }
  }
}
