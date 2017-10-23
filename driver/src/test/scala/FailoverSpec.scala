import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import org.specs2.concurrent.ExecutionEnv

import reactivemongo.api.{ FailoverStrategy, MongoConnection }
import reactivemongo.api.tests._

class FailoverSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification {

  "Failover" title

  import Common._

  {
    val strategy = FailoverStrategy.default.copy(retries = 1)
    def spec(con: MongoConnection, timeout: FiniteDuration) = {
      "be successful" in {
        _failover2(con, strategy)(() => Future.successful({})).
          future must beTypedEqualTo({}).await(1, timeout)
      }

      "fail" in {
        _failover2(con, strategy)(() =>
          Future.failed(new Exception("Foo"))).
          future must throwA[Exception]("Foo").await(1, timeout)
      }

      "handle producer error" in {
        _failover2(con, strategy) { () =>
          throw new scala.RuntimeException("Producer error")
        }.future must throwA[Exception]("Producer error").await(1, timeout)
      }
    }

    "Asynchronous failover on the default connection" should {
      spec(connection, timeout)
    }

    "Asynchronous failover on the slow connection" should {
      spec(slowConnection, slowTimeout)
    }
  }
}
