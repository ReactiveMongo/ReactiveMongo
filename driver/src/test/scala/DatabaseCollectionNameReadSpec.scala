import concurrent.Await
import org.specs2.mutable.Specification
import concurrent.duration._
import reactivemongo.bson.{ BSONString, BSONDocument }

class DatabaseCollectionNameReadSpec extends Specification {
  sequential

  import Common._

  "ReactiveMongo db" should {
    val db2 = db.sibling("specs2-test-reactivemongo-DatabaseCollectionNameReadSpec")

    "query names of collection from database" in {
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
        Set("collection_one", "collection_two")).await(10000)
    }

    "remove db..." in {
      Await.result(db2.drop, DurationInt(10) second) mustEqual (())
    }
  }
}
