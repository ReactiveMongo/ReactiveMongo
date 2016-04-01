import concurrent.Await
import org.specs2.mutable.Specification
import reactivemongo.bson.{ BSONString, BSONDocument }

import org.specs2.concurrent.{ ExecutionEnv => EE }

object DatabaseCollectionNameReadSpec extends Specification {
  sequential

  import Common._

  "ReactiveMongo DB" should {
    val dbName = "specs2-test-reactivemongo-DatabaseCollectionNameReadSpec"

    "query names of collection from database" in { implicit ee: EE =>
      val db2 = connection.database(dbName)
      def i1 = db2.map(_("collection_one")).flatMap(
        _.insert(BSONDocument("one" -> BSONString("one")))).map(_.ok)

      def i2 = db2.map(_("collection_two")).flatMap(
        _.insert(BSONDocument("one" -> BSONString("two")))).map(_.ok)

      i1 aka "insert #1" must beTrue.await(1, timeout) and {
        i2 aka "insert #2" must beTrue.await(1, timeout)
      } and {
        db2.flatMap(_.collectionNames).
          map(_.toSet.filterNot(_ startsWith "system.")).
          aka("names") must beEqualTo(Set("collection_one", "collection_two")).
          await(2, timeout)
      }
    }

    "be dropped" in { implicit ee: EE =>
      connection.database(dbName).flatMap(_.drop()) must beEqualTo({}).
        await(2, timeout)
    }
  }
}
