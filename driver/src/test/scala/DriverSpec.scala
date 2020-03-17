import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration._

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }

import reactivemongo.api.{
  AuthenticationMode,
  DefaultDB,
  FailoverStrategy,
  MongoConnectionOptions,
  ScramSha1Authentication,
  ScramSha256Authentication,
  X509Authentication
}

import reactivemongo.api.bson.BSONDocument

import reactivemongo.core.actors.Exceptions.{
  InternalState,
  PrimaryUnavailableException
}

import reactivemongo.core.protocol.ProtocolMetadata
import reactivemongo.core.commands.{
  FailedAuthentication,
  SuccessfulAuthentication
}

import reactivemongo.api.commands.{ DBUserRole, WriteConcern }

import org.specs2.concurrent.ExecutionEnv

final class DriverSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification {

  "Driver" title

  sequential

  import tests.Common
  import Common._

  val hosts = Seq(primaryHost)

  "Connection pool" should {
    "cleanly start and close with no connections #1" in {
      val md = newAsyncDriver()

      reactivemongo.api.tests.numConnections(md) must_=== 0 and {
        md.close(timeout) must not(throwA[Throwable]).awaitFor(timeout)
      } and {
        md.close(timeout) aka "extra close" must beTypedEqualTo({}).
          awaitFor(timeout)
      }
    }

    "cleanly start and close with no connections #2" in {
      val md = newAsyncDriver()

      reactivemongo.api.tests.numConnections(md) must_=== 0 and (
        md.close(timeout) must not(throwA[Throwable]))
    }

    "start and close with one connection open (using raw URI)" in {
      newAsyncDriver().close(timeout) must not(throwA[Exception])
    }

    "start and close with multiple connections open" in {
      newAsyncDriver().close(timeout) must not(throwA[Exception])
    }

    "use the failover strategy defined in the options" in {
      lazy val con = driver.connect(
        List(primaryHost),
        DefaultOptions.copy(failoverStrategy = FailoverStrategy.remote))

      con.flatMap(_.database(commonDb)).map(_.failoverStrategy).
        aka("strategy") must beTypedEqualTo(FailoverStrategy.remote).
        await(1, timeout) and {
          con.flatMap(_.askClose()(timeout)) must not(
            throwA[Exception]).await(1, timeout)
        }
    }

    "notify a monitor after the NodeSet is started" in {
      val con = driver.connect(List(primaryHost), DefaultOptions)
      val setAvailable = Promise[ProtocolMetadata]()
      val priAvailable = Promise[ProtocolMetadata]()

      val setMon = testMonitor(setAvailable) {
        case reactivemongo.api.tests.PrimaryAvailable(metadata) =>
          Some(metadata)

        case _ => None
      }

      val priMon = testMonitor(priAvailable) {
        case reactivemongo.api.tests.PrimaryAvailable(metadata) =>
          Some(metadata)

        case _ => None
      }

      (for {
        c <- con
        _ <- c.database(commonDb)
        r <- {
          // Database is resolved (so NodeSet and Primary is reachable) ...

          // ... register monitors after
          setMon ! c.mongosystem
          priMon ! c.mongosystem

          Future.sequence(Seq(setAvailable.future, priAvailable.future))
        }
      } yield r.size) must beTypedEqualTo(2).awaitFor(timeout)
    }

    "fail within expected timeout interval" in eventually(2, timeout) {
      lazy val con = driver.connect(
        List("foo:123"),
        DefaultOptions.copy(failoverStrategy = FailoverStrategy.remote))

      val before = System.currentTimeMillis()
      val unresolved = con.flatMap(_.database(commonDb))
      val after = System.currentTimeMillis()

      (after - before) aka "invocation" must beBetween(0L, 75L) and {
        unresolved.map(_ => Option.empty[Throwable] -> -1L).recover {
          case reason => Option(reason) -> (System.currentTimeMillis() - after)
        }.aka("duration") must beLike[(Option[Throwable], Long)] {
          case (Some(reason), duration) =>
            reason.getStackTrace.tail.headOption.
              aka("most recent") must beSome[StackTraceElement].like {
                case mostRecent =>
                  mostRecent.getClassName aka "class" must beTypedEqualTo(
                    "reactivemongo.api.MongoConnection") and (
                      mostRecent.getMethodName aka "method" must_=== "database")

              } and {
                Option(reason.getCause) must beSome[Throwable].like {
                  case _: InternalState => ok
                }
              } and {
                (duration must be_>=(17000L)) and (duration must be_<(28500L))
              }
        }.await(0, 22.seconds) and {
          con.flatMap(_.askClose()(timeout)) must not(
            throwA[Exception]).await(0, timeout)
        }
      }
    }
  }

  section("scram_auth")
  section("not_mongo26")

  def scramSpec(mechanism: AuthenticationMode) = {
    s"Authentication $mechanism" should {
      lazy val drv = newAsyncDriver()
      val conOpts = DefaultOptions.copy(
        nbChannelsPerNode = 1,
        authenticationMechanism = mechanism)

      lazy val connection = drv.connect(List(primaryHost), options = conOpts)
      val slowOpts = SlowOptions.copy(
        nbChannelsPerNode = 1,
        authenticationMechanism = mechanism)
      lazy val slowConnection = {
        val started = slowProxy.isStarted
        drv.connect(List(slowPrimary), slowOpts).filter(_ => started)
      }

      val id = System.identityHashCode(drv)
      val dbName = s"specs2-test-${mechanism}-${id}"
      val userName = s"${mechanism}-${id}"
      def db_ = connection.flatMap(_.database(dbName, failoverStrategy))

      "create a user" in {
        (for {
          d <- db_
          _ <- d.drop()
          _ <- d.createUser(
            user = userName,
            pwd = Some(s"password-$id"),
            customData = None,
            roles = List(DBUserRole("readWrite", dbName)),
            digestPassword = true,
            writeConcern = WriteConcern.Default,
            restrictions = List.empty,
            mechanisms = List(mechanism))
        } yield ()) must beTypedEqualTo({}).await(0, timeout * 2)
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
            _.authenticate(
              dbName, userName, s"password-$id", failoverStrategy)).
            aka("auth request") must beAnInstanceOf[SuccessfulAuthentication].
            await(1, timeout) and {
              db_.flatMap {
                _("testcol").insert.one(BSONDocument("foo" -> "bar"))
              }.map(_ => {}) must beTypedEqualTo({}).await(1, timeout * 2)
            }
        }

        "with the slow connection" in {
          eventually(2, timeout) {
            slowConnection.flatMap(
              _.authenticate(
                dbName,
                userName, s"password-$id", slowFailover)) must beAnInstanceOf[SuccessfulAuthentication].
              awaitFor(slowTimeout)
          }
        }
      }

      "be successful on new connection with right credentials" >> {
        val rightCreds = Map(
          dbName -> MongoConnectionOptions.Credential(
            userName, Some(s"password-$id")))

        "with the default connection" in {
          val con = Await.result(
            drv.connect(
              List(primaryHost),
              options = conOpts.copy(credentials = rightCreds)),
            timeout)

          con.database(dbName, failoverStrategy).
            aka("authed DB") must beLike[DefaultDB] {
              case rdb => rdb.collection("testcol").insert.one(
                BSONDocument("foo" -> "bar")).map(_ => {}).
                aka("insertion") must beTypedEqualTo({}).await(1, timeout)

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
            aka("authed DB") must beAnInstanceOf[DefaultDB].
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
          def con = driver.connect(
            List(primaryHost),
            options = conOpts.copy(credentials = invalidCreds))

          con.flatMap(_.database(commonDb, failoverStrategy)).
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

        "with the slow connection" in {
          def con = driver.connect(
            List(slowPrimary),
            options = slowOpts.copy(credentials = invalidCreds))

          slowProxy.isStarted must beTrue and {
            eventually(2, timeout) {
              //println("DriverSpec_1")

              con.flatMap(_.database(commonDb, slowFailover)).
                aka("resolution") must throwA[PrimaryUnavailableException].
                await(0, slowTimeout + timeout)
            }
          }
        }
      }
    }
  }

  scramSpec(ScramSha1Authentication)

  section("ge_mongo4")
  scramSpec(ScramSha256Authentication)
  section("ge_mongo4")

  section("not_mongo26")
  section("scram_auth")

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
            case rdb => rdb.collection("testcol").insert.one(
              BSONDocument("foo" -> "bar")).map(_ => {}).
              aka("insertion") must beTypedEqualTo({}).await(1, timeout)

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

      def con = driver.connect(List(primaryHost), options = unauthOpts)

      (for {
        c <- con
        db <- c.database(commonDb, failoverStrategy)
        _ <- db.collectionNames
      } yield {}) must beTypedEqualTo({}).await(0, timeout)
    }

    "fail without a valid credential" in {
      val unauthorizedUser = "user_that_is_not_cert_subject"
      val unauthOpts = conOpts.copy(credentials = Map(
        commonDb -> MongoConnectionOptions.Credential(
          unauthorizedUser, Some("password"))))

      def con = driver.connect(List(primaryHost), options = unauthOpts)

      con.flatMap(_.database(commonDb, failoverStrategy)).
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
        sys ! reactivemongo.api.tests.RegisterMonitor
      }

      case Msg(v) => {
        result.success(v)
        context.stop(self)
      }
    }
  }))
}
