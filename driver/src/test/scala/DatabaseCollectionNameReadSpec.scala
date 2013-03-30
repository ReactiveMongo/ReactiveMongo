import concurrent.Await
import org.specs2.mutable.Specification
import concurrent.duration._
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.bson.BSONString
import reactivemongo.bson.{BSONString, BSONDocument}


class DatabaseCollectionNameReadSpec extends Specification {
  sequential

  import Common._

  "ReactiveMongo db " should {

    "query names of collection from database" in {

      val c1: BSONCollection = db("collection_one")

      Await.result(c1.insert(BSONDocument("one"->BSONString("one"))), DurationInt(10) second)

      val c2: BSONCollection = db("collection_two")

      Await.result(c2.insert(BSONDocument("one"->BSONString("two"))), DurationInt(10) second)

      Await.result(db.collectionNames.toList(), DurationInt(10) second)
      .mustEqual (Seq("system.indexes","collection_one", "collection_two"))

    }


  }

}
