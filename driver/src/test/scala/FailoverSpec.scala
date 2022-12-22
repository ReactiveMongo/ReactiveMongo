import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.{ FailoverStrategy, MongoConnection }
import reactivemongo.api.tests._

import org.specs2.concurrent.ExecutionEnv

final class FailoverSpec(implicit ee: ExecutionEnv)
    extends org.specs2.mutable.Specification {

  "Failover".title

  import tests.Common
  import Common._

  "Asynchronous failover" should {
    "on the default connection" >> {
      spec(connection, timeout)
    }

    "on the slow connection" >> {
      spec(slowConnection, slowTimeout)
    }
  }

  // ---

  private val strategy = FailoverStrategy.default.copy(retries = 1)

  private def spec(con: MongoConnection, timeout: FiniteDuration) = {
    "be successful" in {
      _failover2(con, strategy)(() =>
        Future.successful({})
      ).future must beTypedEqualTo({}).await(1, timeout)
    }

    "fail" >> {
      "with an failed result" in {
        _failover2(con, strategy)(() =>
          Future.failed(new Exception("Foo"))
        ).future must throwA[Exception]("Foo").await(1, timeout)
      }

      "with an erroneous response" in {
        @volatile var c = 0
        val s = FailoverStrategy(
          retries = 10,
          delayFactor = { i =>
            c = i
            1D
          }
        )

        _failover2(con, s)(() =>
          Future.successful(
            fakeResponseError(
              reactivemongo.api.bson.BSONDocument(
                "ok" -> 0D,
                "errmsg" -> "not master",
                "code" -> 10107,
                "codeName" -> "NotMaster"
              )
            )
          )
        ).future must throwA[Exception]("not master").awaitFor(timeout) and {
          c must_=== 10
        }
      }
    }

    "handle producer error" in {
      _failover2(con, strategy) { () =>
        throw new scala.RuntimeException("Producer error")
      }.future must throwA[Exception]("Producer error").await(1, timeout)
    }
  }

}
