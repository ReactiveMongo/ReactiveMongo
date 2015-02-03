import org.specs2.mutable.Specification
import reactivemongo.api.MongoDriver

import scala.concurrent.duration.FiniteDuration

/** A Test Suite For MongoDriver */
class MongoDriverSpec extends Specification {

  sequential

  val hosts = Seq("localhost")

  "MongoDriver" should {
    "start and close cleanly with no connections" in {
      val md = MongoDriver()
      md.close(FiniteDuration(100,"milliseconds"))
      success
    }

    "start and close with one connection open" in {
      val md = MongoDriver()
      val connection = md.connection(hosts)
      md.close(FiniteDuration(2,"seconds"))
      success
    }

    "start and close with multiple connections open" in {
      val md = MongoDriver()
      val connection1 = md.connection(hosts,name=Some("Connection1"))
      val connection2 = md.connection(hosts)
      val connection3 = md.connection(hosts)
      md.close(FiniteDuration(2,"seconds"))
      success
    }
  }

}
