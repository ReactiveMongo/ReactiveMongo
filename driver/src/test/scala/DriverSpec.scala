import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.concurrent.duration._

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }

import reactivemongo.api.{ MongoConnectionOptions, X509Authentication }
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

import reactivemongo.core.nodeset.ProtocolMetadata
import reactivemongo.core.commands.{
  FailedAuthentication,
  SuccessfulAuthentication
}

import reactivemongo.api.{ DefaultDB, FailoverStrategy }
import reactivemongo.api.commands.DBUserRole
import reactivemongo.util.timestamp

import org.specs2.concurrent.ExecutionEnv

class DriverSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification {

  "Driver" title

  sequential

  import tests.Common
  import Common._

  val hosts = Seq(primaryHost)

  "Connection pool" should {
    "cleanly start and close with no connections #1" in {
      val md = newDriver()

      reactivemongo.api.tests.numConnections(md) must_== 0 and {
        md.close(timeout) must not(throwA[Throwable])
      } and {
        md.close(timeout) aka "extra close" must_=== ({})
      }
    }

    "cleanly start and close with no connections #2" in {
      val md = newDriver()

      reactivemongo.api.tests.numConnections(md) must_== 0 and (
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

    "fail within expected timeout interval" in eventually(2, timeout) {
      lazy val con = driver.connection(
        List("foo:123"),
        DefaultOptions.copy(failoverStrategy = FailoverStrategy.remote))

      val before = timestamp()
      val unresolved = con.database(commonDb)
      val after = timestamp()

      (after - before) aka "invocation" must beBetween(0L, 75L) and {
        unresolved.map(_ => Option.empty[Throwable] -> -1L).recover {
          case reason => Option(reason) -> (timestamp() - after)
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
        }.await(0, 22.seconds) and {
          con.askClose()(timeout) must not(throwA[Exception]).await(0, timeout)
        }
      }
    }
  }

  "CR Authentication" should {
    section("cr_auth")

    lazy val drv = newDriver()
    lazy val connection = drv.connection(
      List(primaryHost),
      options = DefaultOptions.copy(
        authenticationMechanism = reactivemongo.api.CrAuthentication, // enforce
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
      connection.authenticate(dbName, "foo", "bar", failoverStrategy).
        aka("authentication") must throwA[FailedAuthentication].
        await(0, timeout) and {
          // With credential in initial connection options
          val con = drv.connection(
            List(primaryHost),
            options = DefaultOptions.copy(
              authenticationMechanism = reactivemongo.api.CrAuthentication,
              credentials = Map(dbName -> MongoConnectionOptions.
                Credential("foo", Some("bar"))),
              nbChannelsPerNode = 1))

          con.database(dbName, failoverStrategy).
            map(_ => {}) must throwA[PrimaryUnavailableException].
            await(0, timeout)
        }
    }

    "be successful with right credentials" in {
      connection.authenticate(
        dbName, s"test-$id", s"password-$id", failoverStrategy).
        aka("authentication") must beLike[SuccessfulAuthentication](
          { case _ => ok }).await(1, timeout) and {
            db_.flatMap {
              _("testcol").insert(BSONDocument("foo" -> "bar")).map(_ => {})
            } must beEqualTo({}).await(0, timeout)
          } and {
            // With credential in initial connection options
            val con = drv.connection(
              List(primaryHost),
              options = DefaultOptions.copy(
                authenticationMechanism = reactivemongo.api.CrAuthentication,
                credentials = Map(dbName -> MongoConnectionOptions.
                  Credential(s"test-$id", Some(s"password-$id"))),
                nbChannelsPerNode = 1))

            con.database(dbName, failoverStrategy).
              map(_ => {}) must beTypedEqualTo({}).await(0, timeout)

          }
    }

    "driver shutdown" in { // mainly to ensure the test driver is closed
      drv.close(timeout) must not(throwA[Exception])
    }

    "fail on DB without authentication" in {
      def con = driver.connection(
        List(primaryHost),
        options = DefaultOptions.copy(
          nbChannelsPerNode = 1,
          credentials = Map(commonDb -> MongoConnectionOptions.
            Credential("test", Some("password")))))

      Await.result(con.database(
        commonDb, failoverStrategy), timeout).
        aka("database resolution") must throwA[PrimaryUnavailableException]
    }
    section("mongo2")

    section("cr_auth")
  }

  "Authentication SCRAM-SHA1" should {
    section("scram_auth")

    lazy val drv = newAsyncDriver()
    val conOpts = DefaultOptions.copy(nbChannelsPerNode = 1)
    lazy val connection = drv.connect(List(primaryHost), options = conOpts)
    val slowOpts = SlowOptions.copy(nbChannelsPerNode = 1)
    lazy val slowConnection = {
      val started = slowProxy.isStarted
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
        connection.flatMap(_.authenticate(
          dbName, "foo", "bar", failoverStrategy)).
          aka("authentication") must throwA[FailedAuthentication].
          await(0, timeout)

      }

      "with the slow connection" in {
        slowConnection.flatMap(_.authenticate(
          dbName, "foo", "bar", slowFailover)).
          aka("authentication") must throwA[FailedAuthentication].
          await(1, slowTimeout)
      }
    }

    "be successful on existing connection with right credentials" >> {
      "with the default connection" in {
        connection.flatMap(
          _.authenticate(dbName, s"test-$id", s"password-$id",
            failoverStrategy)) must beLike[SuccessfulAuthentication](
            { case _ => ok }).await(1, timeout) and {
              db_.flatMap {
                _("testcol").insert(BSONDocument("foo" -> "bar"))
              }.map(_ => {}) must beEqualTo({}).await(1, timeout * 2)
            }

      }

      "with the slow connection" in {
        eventually(2, timeout) {
          slowConnection.flatMap(
            _.authenticate(dbName, s"test-$id", s"password-$id", slowFailover)).
            aka("authentication") must beLike[SuccessfulAuthentication](
              { case _ => ok }).await(0, slowTimeout)
        }
      }
    }

    "be successful with right credentials" >> {
      val rightCreds = Map(
        dbName -> MongoConnectionOptions.Credential(
          s"test-$id", Some(s"password-$id")))

      "with the default connection" in {
        val con = Await.result(
          drv.connect(
            List(primaryHost),
            options = conOpts.copy(credentials = rightCreds)),
          timeout)

        con.database(dbName, failoverStrategy).
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
            List(slowPrimary),
            options = slowOpts.copy(credentials = rightCreds)),
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

    "fail on DB with invalid credential" >> {
      val invalidCreds = Map(commonDb ->
        MongoConnectionOptions.Credential("test", Some("password")))

      "with the default connection" in {
        def con = driver.connection(
          List(primaryHost),
          options = conOpts.copy(credentials = invalidCreds))

        con.database(commonDb, failoverStrategy).
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
        def con = driver.connection(
          List(slowPrimary),
          options = slowOpts.copy(credentials = invalidCreds))

        slowProxy.isStarted must beTrue and {
          eventually(2, timeout) {
            //println("DriverSpec_1")

            con.database(commonDb, slowFailover).
              aka("resolution") must throwA[PrimaryUnavailableException].
              await(0, slowTimeout + timeout)
          }
        }

      }
    }
    section("not_mongo26")

    section("scram_auth")
  }

  "X509 Authentication" should {
    section("x509")

    val drv = newAsyncDriver()
    val conOpts = DefaultOptions.copy(
      nbChannelsPerNode = 1,
      authenticationMechanism = X509Authentication,
      sslEnabled = true)

    val dbName = "specs2-test-x509-auth"

    "be successful with right credentials" in {
      val con = Await.result(
        drv.connect(List(primaryHost), options = conOpts), timeout)

      conOpts.credentials must not(beEmpty) and {
        con.database(dbName, failoverStrategy).
          aka("authed DB") must beLike[DefaultDB] {
            case rdb => rdb.collection("testcol").insert(
              BSONDocument("foo" -> "bar")).map(_ => {}).
              aka("insertion") must beEqualTo({}).await(1, timeout)

          }.await(1, timeout) and {
            con.askClose()(timeout).
              aka("close") must not(throwA[Exception]).await(1, timeout)
          }
      }
    }

    "driver shutdown" in {
      // mainly to ensure the test driver is closed
      drv.close(timeout) must not(throwA[Exception]).await(1, timeout)
    }

    "be successful with an invalid credential over and a valid one" in {
      val unauthorizedUser = "user_that_is_not_cert_subject"

      // Default options with a valid credential + unauthorizedUser
      val unauthOpts = conOpts.copy(credentials = conOpts.credentials + (
        commonDb -> MongoConnectionOptions.Credential(
          unauthorizedUser, Some("password"))))

      def con = driver.connection(List(primaryHost), options = unauthOpts)

      con.database(commonDb, failoverStrategy).flatMap(_.collectionNames).
        map(_ => {}) must beTypedEqualTo({}).await(0, timeout)

    }

    "fail without a valid credential" in {
      val unauthorizedUser = "user_that_is_not_cert_subject"
      val unauthOpts = conOpts.copy(credentials = Map(
        commonDb -> MongoConnectionOptions.Credential(
          unauthorizedUser, Some("password"))))

      def con = driver.connection(List(primaryHost), options = unauthOpts)

      con.database(commonDb, failoverStrategy).
        aka("DB resolution") must throwA[PrimaryUnavailableException].like {
          case reason => reason.getStackTrace.tail.headOption.
            aka("most recent") must beSome[StackTraceElement].like {
              case mostRecent =>
                mostRecent.getClassName aka "class" must beTypedEqualTo(
                  "reactivemongo.api.MongoConnection") and (
                    mostRecent.getMethodName aka "method" must_=== "database")
            } and {
              Option(reason.getCause).
                aka("cause") must beSome[Throwable].like {
                  case _: InternalState => ok
                }
            }
        }.await(1, timeout)
    }

    section("x509")
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
