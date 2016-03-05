package reactivemongo.api

import scala.util.Try
import scala.concurrent.{ Await, Future, Promise }

object FailoverSpec extends org.specs2.mutable.Specification {
  "Failover" title

  import tests.utility._

  "Asynchronous failover" should {
    val strategy = FailoverStrategy().copy(retries = 1)

    "be successful" in {
      Failover2(connection, strategy)(() => Future.successful({})).
        future must beEqualTo({}).await(timeoutMillis)
    }

    "fail" in {
      Failover2(connection, strategy)(() =>
        Future.failed(new Exception("Foo"))).
        future must throwA[Exception]("Foo").await(timeoutMillis)
    }

    "handle producer error" in {
      Failover2(connection, strategy) { () =>
        throw new scala.RuntimeException("Producer error")
      }.future must throwA[Exception]("Producer error").await(timeoutMillis)
    }
  }
}
