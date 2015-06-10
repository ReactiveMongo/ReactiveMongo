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

      md.numConnections must_== 0 and (
        md.close(FiniteDuration(500,"milliseconds")) must not(
          throwA[Throwable]))

    }

    "start and close with one connection open" in {
      val md = MongoDriver()
      val connection = md.connection(hosts)
      md.close(FiniteDuration(5,"seconds"))
      success
    }

    "start and close with multiple connections open" in {
      val md = MongoDriver()
      val connection1 = md.connection(hosts,name=Some("Connection1"))
      val connection2 = md.connection(hosts)
      val connection3 = md.connection(hosts)
      md.close(FiniteDuration(5,"seconds"))
      success
    }
  }

}
