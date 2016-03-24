import concurrent.Await
import org.specs2.mutable.Specification
//import scala.concurrent.duration._
import reactivemongo.bson.{ BSONString, BSONDocument }

import org.specs2.concurrent.{ ExecutionEnv => EE }

object DatabaseCollectionNameReadSpec extends Specification {
  sequential

  import Common._

  "ReactiveMongo db" should {
    val dbName = "specs2-test-reactivemongo-DatabaseCollectionNameReadSpec"

    "query names of collection from database" in { implicit ee: EE =>
      val db2 = db.sibling(dbName)
      val collectionNames = for {
        _ <- {
          val c1 = db2("collection_one")
          c1.insert(BSONDocument("one" -> BSONString("one")))
        }
        _ <- {
          val c2 = db2("collection_two")
          c2.insert(BSONDocument("one" -> BSONString("two")))
        }
        ns <- db2.collectionNames.map(_.toSet)
      } yield ns

      collectionNames.map(_.filterNot(_ startsWith "system.")) must beEqualTo(
        Set("collection_one", "collection_two")).await(2, timeout)
    }

    "remove db..." in { implicit ee: EE =>
      db.sibling(dbName).drop must beEqualTo({}).await(2, timeout)
    }
  }
}
