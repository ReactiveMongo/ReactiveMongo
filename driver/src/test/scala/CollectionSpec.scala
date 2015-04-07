import reactivemongo.bson.{ BSONDocument, BSONString }
import scala.concurrent.Await
import scala.util.{ Try, Success, Failure }
import org.specs2.mutable.{ Specification, Tags }
import reactivemongo.api.commands.bson.BSONCountCommand._
import reactivemongo.api.commands.bson.BSONCountCommandImplicits._

object CollectionSpec extends Specification with Tags {
  import Common._

  sequential

  lazy val collection = db("somecollection_collectionspec")

  "ReactiveMongo" should {
    "create a collection" in {
      Await.result(collection.create(), timeout) mustEqual(())
    }

    "convert to capped" in {
      Await.result(collection.convertToCapped(2 * 1024 * 1024, None), timeout) mustEqual (())
    }

    "check if it's capped" in {
      // convertToCapped is async. Let's wait a little while before checking if it's done
      Thread.sleep(4000)
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

      Await.result(collection.runValueCommand(Count(BSONDocument())), timeout) mustEqual 1
    }

    // Empty capped need to be enabled with enableTestCommands
    // see: http://docs.mongodb.org/manual/reference/command/emptycapped/#dbcmd.emptycapped
    /*"empty the capped collection" in {
      Await.result(collection.emptyCapped(), timeout) mustEqual true
      Await.result(db.command(Count(collection.name)), timeout) mustEqual 0
    } tag ("testCommands")*/

    "drop it" in {
      Await.result(collection.drop(), timeout) mustEqual (())
    }
  }
}
