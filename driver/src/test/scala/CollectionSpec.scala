import scala.concurrent.duration.FiniteDuration

import reactivemongo.bson.{ BSONDocument, BSONString }
import reactivemongo.api.MongoConnection
import reactivemongo.api.commands.CollStatsResult
import reactivemongo.api.collections.bson.BSONCollection

import org.specs2.concurrent.ExecutionEnv

class CollectionSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification {

  import Common._

  sequential

  lazy val collection = db(s"collspec${System identityHashCode db}")
  lazy val slowColl = slowDb(s"collspec${System identityHashCode slowDb}")

  val cappedMaxSize: Long = 2 * 1024 * 1024

  "ReactiveMongo" should {
    "create a collection" in {
      collection.create() must beEqualTo({}).await(1, timeout) and (
        slowColl.create() must beEqualTo({}).await(1, slowTimeout))
    }

    "convert to capped" >> {
      def cappedSpec(c: BSONCollection, timeout: FiniteDuration) = c.convertToCapped(cappedMaxSize, None) must beEqualTo({}).await(1, timeout)

      "with the default collection" in {
        cappedSpec(collection, timeout)
      }

      "with the default collection" in {
        cappedSpec(slowColl, slowTimeout)
      }
    }

    "check if it's capped (MongoDB <= 2.6)" in {
      collection.stats must beLike[CollStatsResult] {
        case stats => stats.capped must beTrue and (stats.maxSize must beNone)
      }.await(1, timeout)
    } tag "mongo2"

    "check if it's capped (MongoDB >= 3.0)" >> {
      def statSpec(con: MongoConnection, c: BSONCollection, timeout: FiniteDuration) = {
        c.stats must beLike[CollStatsResult] {
          case stats => stats.capped must beTrue and (
            stats.maxSize must beSome(cappedMaxSize))
        }.await(1, timeout)
      }

      "with the default connection" in {
        statSpec(connection, collection, timeout)
      } tag "not_mongo26"

      "with the slow connection" in {
        statSpec(slowConnection, slowColl, slowTimeout)
      } tag "not_mongo26"
    }

    "insert some docs then test lastError result and finally count" in {

      collection.insert(BSONDocument("name" -> BSONString("Jack"))).
        map(_.ok) must beTrue.await(1, timeout) and (
          collection.count() must beEqualTo(1).await(1, timeout)) and (
            collection.count(skip = 1) must beEqualTo(0).await(1, timeout)) and (
              collection.count(selector = Some(BSONDocument("name" -> "Jack"))).
              aka("matching count") must beEqualTo(1).await(1, timeout)) and (
                collection.count(selector = Some(BSONDocument("name" -> "Foo"))).
                aka("not matching count") must beEqualTo(0).await(1, timeout))
    }

    // Empty capped need to be enabled with enableTestCommands
    // see: http://docs.mongodb.org/manual/reference/command/emptycapped/#dbcmd.emptycapped
    /*"empty the capped collection" in {
      Await.result(collection.emptyCapped(), timeout) mustEqual true
      Await.result(db.command(Count(collection.name)), timeout) mustEqual 0
    } tag ("testCommands")*/

    "drop it" in {
      collection.drop(false) must beTrue.await(1, timeout)
    }
  }
}
