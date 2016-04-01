import scala.concurrent.duration.FiniteDuration

import reactivemongo.bson.{ BSONString, BSONDocument }
import reactivemongo.api.MongoConnection

import org.specs2.concurrent.{ ExecutionEnv => EE }

class DatabaseCollectionNameReadSpec extends org.specs2.mutable.Specification {
  sequential

  import Common._

  "ReactiveMongo DB" should {
    val dbName = s"dbnmeread${System identityHashCode this}"

    "query names of collection from database" >> {
      def dbSpec(con: MongoConnection, timeout: FiniteDuration)(implicit ee: EE) = {
        val db2 = con.database(dbName)
        def i1 = db2.map(_("collection_one")).flatMap(
          _.insert(BSONDocument("one" -> BSONString("one")))).map(_.ok)

        def i2 = db2.map(_("collection_two")).flatMap(
          _.insert(BSONDocument("one" -> BSONString("two")))).map(_.ok)

        i1 aka "insert #1" must beTrue.await(1, timeout) and {
          i2 aka "insert #2" must beTrue.await(1, timeout)
        } and {
          db2.flatMap(_.collectionNames).
            map(_.toSet.filterNot(_ startsWith "system.")).
            aka("names") must beEqualTo(Set(
              "collection_one", "collection_two")).await(2, timeout)
        }
      }

      "with the default connection" in { implicit ee: EE =>
        dbSpec(connection, timeout)
      }

      "with the slow connection" in { implicit ee: EE =>
        dbSpec(slowConnection, slowTimeout)
      }
    }

    {
      def dropSpec(con: MongoConnection, timeout: FiniteDuration)(implicit ee: EE) = connection.database(dbName).flatMap(_.drop()) aka "drop" must beEqualTo({}).await(2, timeout)

      "be dropped with the default connection" in { implicit ee: EE =>
        dropSpec(connection, timeout)
      }

      "be dropped with the slow connection" in { implicit ee: EE =>
        dropSpec(slowConnection, slowTimeout)
      }
    }
  }
}
