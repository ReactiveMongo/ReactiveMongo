import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.concurrent.duration._

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }

import reactivemongo.bson.BSONDocument

import reactivemongo.core.actors.{
  PrimaryAvailable,
  RegisterMonitor,
  SetAvailable
}
import reactivemongo.core.actors.Exceptions.{
  InternalState,
  PrimaryUnavailableException
}

import reactivemongo.core.nodeset.{ Authenticate, ProtocolMetadata }
import reactivemongo.core.commands.{
  FailedAuthentication,
  SuccessfulAuthentication
}

import reactivemongo.api.{ DefaultDB, FailoverStrategy }
import reactivemongo.api.commands.DBUserRole

import org.specs2.concurrent.ExecutionEnv

class DriverSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification {

  "Driver" title

  sequential

  import Common._

  val hosts = Seq(primaryHost)

  "Connection pool" should {
    "cleanly start and close with no connections #1" in {
      val md = newDriver()

      md.numConnections must_== 0 and {
        md.close(timeout) must not(throwA[Throwable])
      } and {
        md.close(timeout) aka "extra close" must_=== ({})
      }
    }

    "cleanly start and close with no connections #2" in {
      val md = newDriver()

      md.numConnections must_== 0 and (
        md.close(timeout) must not(throwA[Throwable]))
    }

    "start and close with one connection open (using raw URI)" in {
      newDriver().close(timeout) must not(throwA[Exception])
    }

    "start and close with multiple connections open" in {
      newDriver().close(timeout) must not(throwA[Exception])
    }

    "use the failover strategy defined in the options" in {
      lazy val con = driver.connection(
        List(primaryHost),
        DefaultOptions.copy(failoverStrategy = FailoverStrategy.remote))

      con.database(commonDb).map(_.failoverStrategy).
        aka("strategy") must beTypedEqualTo(FailoverStrategy.remote).
        await(1, timeout) and {
          con.askClose()(timeout) must not(throwA[Exception]).await(1, timeout)
        }
    }

    "notify a monitor after the NodeSet is started" in {
      val con = driver.connection(List(primaryHost), DefaultOptions)
      val setAvailable = Promise[ProtocolMetadata]()
      val priAvailable = Promise[ProtocolMetadata]()

      val setMon = testMonitor(setAvailable) {
        case msg: SetAvailable =>
          SetAvailable.unapply(msg)
        case _ => None
      }

      val priMon = testMonitor(priAvailable) {
        case msg: PrimaryAvailable =>
          PrimaryAvailable.unapply(msg)

        case _ => None
      }

      con.database(commonDb).flatMap { _ =>
        // Database is resolved (so NodeSet and Primary is reachable) ...

        // ... register monitors after
        setMon ! con.mongosystem
        priMon ! con.mongosystem

        Future.sequence(Seq(setAvailable.future, priAvailable.future))
      }.map(_.size) must beEqualTo(2).await(0, timeout)
    }

    "fail within expected timeout interval" in {
      lazy val con = driver.connection(
        List("foo:123"),
        DefaultOptions.copy(failoverStrategy = FailoverStrategy.remote))

      val before = System.currentTimeMillis()
      val unresolved = con.database(commonDb)
      val after = System.currentTimeMillis()

      (after - before) aka "invocation" must beBetween(0L, 75L) and {
        unresolved.map(_ => Option.empty[Throwable] -> -1L).recover {
          case reason => Option(reason) -> (System.currentTimeMillis() - after)
        }.aka("duration") must beLike[(Option[Throwable], Long)] {
          case (Some(reason), duration) =>
            reason.getStackTrace.tail.headOption.
              aka("most recent") must beSome[StackTraceElement].like {
                case mostRecent =>
                  mostRecent.getClassName aka "class" must beEqualTo(
                    "reactivemongo.api.MongoConnection") and (
                      mostRecent.getMethodName aka "method" must_== "database")

              } and {
                Option(reason.getCause) must beSome[Throwable].like {
                  case _: InternalState => ok
                }
              } and {
                (duration must be_>=(17000L)) and (duration must be_<(28500L))
              }
        }.await(1, 22.seconds) and {
          con.askClose()(timeout) must not(throwA[Exception]).await(1, timeout)
        }
      }
    }
  }

  "CR Authentication" should {
    lazy val drv = newDriver()
    lazy val connection = drv.connection(
      List(primaryHost),
      options = DefaultOptions.copy(
        authMode = reactivemongo.api.CrAuthentication, // enforce
        nbChannelsPerNode = 1))

    val dbName = s"specs2-test-cr${System identityHashCode drv}"
    lazy val db_ = connection.database(dbName, failoverStrategy)

    val id = System.identityHashCode(drv)

    section("mongo2")

    "be the default mode" in {
      db_.flatMap(_.drop()).map(_ => {}) must beEqualTo({}).await(1, timeout)
    }

    "create a user" in {
      db_.flatMap(_.createUser(s"test-$id", Some(s"password-$id"),
        roles = List(DBUserRole("readWrite", dbName)))).
        aka("creation") must beEqualTo({}).await(0, timeout)
    }

    "not be successful with wrong credentials" in {
      connection.authenticate(dbName, "foo", "bar").
        aka("authentication") must throwA[FailedAuthentication].
        await(1, timeout)

    }

    "be successful with right credentials" in {
      connection.authenticate(dbName, s"test-$id", s"password-$id").
        aka("authentication") must beLike[SuccessfulAuthentication](
          { case _ => ok }).await(1, timeout) and {
            db_.flatMap {
              _("testcol").insert(BSONDocument("foo" -> "bar")).map(_ => {})
            } must beEqualTo({}).await(1, timeout)
          }
    }

    "driver shutdown" in { // mainly to ensure the test driver is closed
      drv.close(timeout) must not(throwA[Exception])
    }

    "fail on DB without authentication" in {
      val auth = Authenticate(Common.commonDb, "test", "password")
      val conOpts = DefaultOptions.copy(nbChannelsPerNode = 1)

      def con = Common.driver.connection(
        List(primaryHost),
        options = conOpts,
        authentications = Seq(auth))

      Await.result(con.database(
        Common.commonDb, failoverStrategy), timeout).
        aka("database resolution") must throwA[PrimaryUnavailableException]
    }
    section("mongo2")
  }

  "Authentication SCRAM-SHA1" should {
    import Common.{ DefaultOptions, timeout }

    lazy val drv = Common.newAsyncDriver()
    val conOpts = DefaultOptions.copy(nbChannelsPerNode = 1)
    lazy val connection = drv.connect(List(primaryHost), options = conOpts)
    val slowOpts = SlowOptions.copy(nbChannelsPerNode = 1)
    lazy val slowConnection = {
      val started = Common.slowProxy.isStarted
      drv.connect(List(slowPrimary), slowOpts).filter(_ => started)
    }

    val id = System.identityHashCode(drv)
    val dbName = s"specs2-test-scramsha1${id}"
    def db_(implicit ee: ExecutionContext) =
      connection.flatMap(_.database(dbName, failoverStrategy))

    section("not_mongo26")

    "work only if configured" in {
      db_.flatMap(_.drop()).map(_ => {}) must beEqualTo({}).
        await(0, timeout * 2)
    }

    "create a user" in {
      db_.flatMap(_.createUser(s"test-$id", Some(s"password-$id"),
        roles = List(DBUserRole("readWrite", dbName)))).
        aka("creation") must beEqualTo({}).await(0, timeout)
    }

    "not be successful with wrong credentials" >> {
      "with the default connection" in {
        connection.flatMap(_.authenticate(dbName, "foo", "bar")).
          aka("authentication") must throwA[FailedAuthentication].
          await(1, timeout)

      }

      "with the slow connection" in {
        slowConnection.flatMap(_.authenticate(dbName, "foo", "bar")).
          aka("authentication") must throwA[FailedAuthentication].
          await(1, slowTimeout)
      }
    }

    "be successful on existing connection with right credentials" >> {
      "with the default connection" in {
        connection.flatMap(
          _.authenticate(dbName, s"test-$id", s"password-$id")).
          aka("authentication") must beLike[SuccessfulAuthentication](
            { case _ => ok }).await(1, timeout) and {
              db_.flatMap {
                _("testcol").insert(BSONDocument("foo" -> "bar"))
              }.map(_ => {}) must beEqualTo({}).await(1, timeout * 2)
            }

      }

      "with the slow connection" in {
        slowConnection.flatMap(
          _.authenticate(dbName, s"test-$id", s"password-$id")).
          aka("authentication") must beLike[SuccessfulAuthentication](
            { case _ => ok }).await(1, slowTimeout)

      }
    }

    "be successful with right credentials" >> {
      val auth = Authenticate(dbName, s"test-$id", s"password-$id")

      "with the default connection" in {
        val con = Await.result(
          drv.connect(
            List(primaryHost), options = conOpts, authentications = Seq(auth)),
          timeout)

        con.database(dbName, Common.failoverStrategy).
          aka("authed DB") must beLike[DefaultDB] {
            case rdb => rdb.collection("testcol").insert(
              BSONDocument("foo" -> "bar")).map(_ => {}).
              aka("insertion") must beEqualTo({}).await(1, timeout)

          }.await(1, timeout) and {
            con.askClose()(timeout).
              aka("close") must not(throwA[Exception]).await(1, timeout)
          }
      }

      "with the slow connection" in {
        val con = Await.result(
          drv.connect(
            List(slowPrimary), options = slowOpts, authentications = Seq(auth)),
          slowTimeout)

        con.database(dbName, slowFailover).
          aka("authed DB") must beLike[DefaultDB] { case _ => ok }.
          await(1, slowTimeout) and {
            con.askClose()(slowTimeout) must not(throwA[Exception]).
              await(1, slowTimeout)
          }
      }
    }

    "driver shutdown" in {
      // mainly to ensure the test driver is closed
      drv.close(timeout) must not(throwA[Exception]).await(1, timeout)
    }

    "fail on DB without authentication" >> {
      val auth = Authenticate(Common.commonDb, "test", "password")

      "with the default connection" in {
        def con = Common.driver.connection(
          List(primaryHost), options = conOpts, authentications = Seq(auth))

        con.database(Common.commonDb, failoverStrategy).
          aka("DB resolution") must throwA[PrimaryUnavailableException].like {
            case reason => reason.getStackTrace.tail.headOption.
              aka("most recent") must beSome[StackTraceElement].like {
                case mostRecent =>
                  mostRecent.getClassName aka "class" must beEqualTo(
                    "reactivemongo.api.MongoConnection") and (
                      mostRecent.getMethodName aka "method" must_== "database")
              } and {
                Option(reason.getCause).
                  aka("cause") must beSome[Throwable].like {
                    case _: InternalState => ok
                  }
              }
          }.await(1, timeout)

      }

      "with the slow connection" in {
        def con = Common.driver.connection(
          List(slowPrimary), options = slowOpts, authentications = Seq(auth))

        Common.slowProxy.isStarted must beTrue and {
          con.database(Common.commonDb, slowFailover).
            aka("database resolution") must throwA[PrimaryUnavailableException].
            await(0, slowTimeout + 1.second)
        }
      }
    }

    section("not_mongo26")
  }

  // ---

  def testMonitor[T](result: Promise[T], actorSys: ActorSystem = driver.system)(test: Any => Option[T]): ActorRef = actorSys.actorOf(Props(new Actor {
    private object Msg {
      def unapply(that: Any): Option[T] = test(that)
    }

    val receive: Receive = {
      case sys: akka.actor.ActorRef => {
        // Manually register this as connection/system monitor
        sys ! RegisterMonitor
      }

      case Msg(v) => {
        result.success(v)
        context.stop(self)
      }
    }
  }))
}
