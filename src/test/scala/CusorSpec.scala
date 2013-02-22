import org.specs2.mutable._
import reactivemongo.bson._
import DefaultBSONHandlers._
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import play.api.libs.iteratee.Iteratee

class CursorSpec  extends Specification {
  sequential

  import Common._

  val coll = db("cursorspec")

  "ReactiveMongo" should {
    "insert 16,517 records" in {
      val futs = for(i <- 0 until 16517)
        yield coll.insert(BSONDocument("i" -> BSONInteger(i), "record" -> BSONString("record" + i)))
      val fut = Future.sequence(futs)
      Await.result(fut, DurationInt(20).seconds)
      println("inserted 16,517 records")
      success
    }
    "get all the 16,517 documents" in {
      var i = 0
      val future = coll.find(BSONDocument()).cursor.enumerate |>>> (Iteratee.foreach({ e =>
        i += 1
      }))
      Await.result(future, DurationInt(21).seconds) mustEqual ()
      i mustEqual 16517
    }
  }
}