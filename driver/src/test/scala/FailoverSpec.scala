package reactivemongo.api

import scala.util.Try
import scala.concurrent.{ Await, Future, Promise }

import org.specs2.concurrent.{ ExecutionEnv => EE }

object FailoverSpec extends org.specs2.mutable.Specification {
  "Failover" title

  import tests.utility._

  "Asynchronous failover" should {
    val strategy = FailoverStrategy().copy(retries = 1)

    "be successful" in { implicit ee: EE =>
      Failover2(connection, strategy)(() => Future.successful({})).
        future must beEqualTo({}).await(1, timeout)
    }

    "fail" in { implicit ee: EE =>
      Failover2(connection, strategy)(() =>
        Future.failed(new Exception("Foo"))).
        future must throwA[Exception]("Foo").await(1, timeout)
    }

    "handle producer error" in { implicit ee: EE =>
      Failover2(connection, strategy) { () =>
        throw new scala.RuntimeException("Producer error")
      }.future must throwA[Exception]("Producer error").await(1, timeout)
    }
  }
}
