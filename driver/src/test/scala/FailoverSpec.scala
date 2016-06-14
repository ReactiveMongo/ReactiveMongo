import scala.util.Try
import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration._

import org.specs2.concurrent.{ ExecutionEnv => EE }

import reactivemongo.api.FailoverStrategy
import reactivemongo.api.tests._

object FailoverSpec extends org.specs2.mutable.Specification {
  "Failover" title

  import Common._

  "Asynchronous failover" should {
    val strategy = FailoverStrategy.default.copy(retries = 1)

    "be successful" in { implicit ee: EE =>
      _failover2(connection, strategy)(() => Future.successful({})).
        future must beTypedEqualTo({}).await(1, timeout)
    }

    "fail" in { implicit ee: EE =>
      _failover2(connection, strategy)(() =>
        Future.failed(new Exception("Foo"))).
        future must throwA[Exception]("Foo").await(1, timeout)
    }

    "handle producer error" in { implicit ee: EE =>
      _failover2(connection, strategy) { () =>
        throw new scala.RuntimeException("Producer error")
      }.future must throwA[Exception]("Producer error").await(1, timeout)
    }
  }

  "Connection" should {
    "use the failover strategy defined in the options" in { implicit ee: EE =>
      lazy val con = driver.connection(List(primaryHost),
        DefaultOptions.copy(failoverStrategy = FailoverStrategy.remote))

      con.database(commonDb).map(_.failoverStrategy).
        aka("strategy") must beTypedEqualTo(FailoverStrategy.remote).
        await(1, timeout) and {
          con.askClose()(timeout) must not(throwA[Exception]).await(1, timeout)
        }
    }

    "fail within expected timeout interval" in { implicit ee: EE =>
      lazy val con = driver.connection(List("foo:123"),
        DefaultOptions.copy(failoverStrategy = FailoverStrategy.remote))

      val before = System.currentTimeMillis()

      con.database(commonDb).map(_ => -1L).recover {
        case _ => System.currentTimeMillis()
      }.aka("duration") must beLike[Long] {
        case duration =>
          (duration must be_>=(1465655000000L)) and (
            duration must be_<(1466000000000L))
      }.await(1, 1466000000000L.milliseconds) and {
        con.askClose()(timeout) must not(throwA[Exception]).await(1, timeout)
      }
    }
  }
}
