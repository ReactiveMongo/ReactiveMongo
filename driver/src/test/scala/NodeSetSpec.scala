import scala.concurrent.{ Await, Future }

import akka.actor.ActorRef
import akka.testkit.TestActorRef

import org.specs2.matcher.MatchResult
import org.specs2.concurrent.ExecutionEnv

import reactivemongo.api.{
  MongoConnection,
  MongoConnectionOptions,
  MongoDriver,
  ReadPreference
}

import reactivemongo.core.nodeset.{ Authenticate, ProtocolMetadata }

import reactivemongo.core.actors.{
  PrimaryAvailable,
  PrimaryUnavailable,
  SetAvailable,
  SetUnavailable,
  StandardDBSystem
}
import reactivemongo.core.actors.Exceptions.PrimaryUnavailableException

class NodeSetSpec(implicit val ee: ExecutionEnv)
  extends org.specs2.mutable.Specification
  with ConnectAllSpec with UnresponsiveSecondarySpec {

  "Node set" title

  sequential // for ConnectAllSpec

  import reactivemongo.api.tests._

  @inline def failoverStrategy = Common.failoverStrategy
  @inline def timeout = Common.timeout
  lazy val md = Common.newDriver()
  lazy val actorSystem = md.system

  protected val nodes = Seq(
    "nodesetspec.node1:27017", "nodesetspec.node2:27017")

  section("unit")
  "Node set" should {
    "not be available" >> {
      "if the entire node set is not available" in {
        scala.concurrent.Await.result(withConAndSys(md) {
          (con, _) => isAvailable(con, timeout)
        }, timeout) must throwA[java.util.concurrent.TimeoutException]
      }

      "if the primary is not available if default preference" in {
        withCon() { (name, con, mon) =>
          mon ! SetAvailable(ProtocolMetadata.Default)

          waitIsAvailable(con, failoverStrategy).map(_ => true).recover {
            case reason: PrimaryUnavailableException if (
              reason.getMessage.indexOf(name) != -1) => false
          } must beFalse.await(1, timeout)
        }
      }
    }

    "be available" >> {
      "with the primary if default preference" in {
        withCon() { (_, con, mon) =>
          def test = (for {
            _ <- {
              mon ! SetAvailable(ProtocolMetadata.Default)
              mon ! PrimaryAvailable(ProtocolMetadata.Default)

              waitIsAvailable(con, failoverStrategy)
            }

            after <- isAvailable(con, timeout)
          } yield after)

          test must beTrue.await(1, timeout)
        }
      }

      "without the primary if slave ok" >> {
        org.specs2.specification.core.Fragments.foreach[ReadPreference](
          Seq(ReadPreference.primaryPreferred, ReadPreference.secondary)) { readPref =>
            s"using $readPref" in {
              val opts = MongoConnectionOptions(readPreference = readPref)

              withCon(opts) { (_, con, mon) =>
                def test = (for {
                  _ <- {
                    mon ! SetAvailable(ProtocolMetadata.Default)
                    waitIsAvailable(con, failoverStrategy)
                  }
                  after <- isAvailable(con, timeout)
                } yield after)

                test must beTrue.await(1, timeout)
              }
            }
          }
      }
    }

    "be unavailable" >> {
      "with the primary unavailable if default preference" in {
        withCon() { (name, con, mon) =>
          mon ! SetAvailable(ProtocolMetadata.Default)
          mon ! PrimaryAvailable(ProtocolMetadata.Default)

          def test = (for {
            _ <- waitIsAvailable(con, failoverStrategy)
            before <- isAvailable(con, timeout)
            _ = mon ! PrimaryUnavailable
            after <- waitIsAvailable(
              con, failoverStrategy).map(_ => true).recover {
              case _ => false
            }
          } yield before -> after)

          test must beEqualTo(true -> false).await(1, timeout)
        }
      }

      "without the primary if slave ok" in {
        val opts = MongoConnectionOptions(
          readPreference = ReadPreference.primaryPreferred)

        withCon(opts) { (name, con, mon) =>
          mon ! SetAvailable(ProtocolMetadata.Default)

          def test = (for {
            _ <- waitIsAvailable(con, failoverStrategy)
            before <- isAvailable(con, timeout)
            _ = mon ! SetUnavailable
            after <- waitIsAvailable(
              con, failoverStrategy).map(_ => true).recover {
              case _ => false
            }
          } yield before -> after)

          test must beEqualTo(true -> false).await(1, timeout)
        }
      }
    }

    connectAllSpec

    unresponsiveSecondarySpec

    "be closed" in {
      md.close(timeout) must not(throwA[Exception])
    }
  }

  "Connection listener" should {
    import external.reactivemongo.ConnectionListener

    "not find class StaticListenerBinder" in {
      Class.forName("reactivemongo.core.StaticListenerBinder").
        aka("class loader") must throwA[ClassNotFoundException]
    }

    "be resolved as the default one" in {
      ConnectionListener().map(_.getClass.getName) must beNone
    }
  }
  section("unit")

  // ---

  def withConAndSys[T](drv: MongoDriver, options: MongoConnectionOptions = MongoConnectionOptions(nbChannelsPerNode = 1), _nodes: Seq[String] = nodes)(f: (MongoConnection, TestActorRef[StandardDBSystem]) => Future[T]): Future[T] = {
    // See MongoDriver#connection
    val supervisorName = s"withConAndSys-sup-${System identityHashCode ee}"
    val poolName = s"withConAndSys-con-${System identityHashCode f}"

    @inline implicit def sys = drv.system

    val auths = Seq(Authenticate(Common.commonDb, "test", "password"))
    lazy val mongosystem = TestActorRef[StandardDBSystem](
      standardDBSystem(supervisorName, poolName, nodes, auths, options),
      poolName)

    def connection = addConnection(
      drv, poolName, _nodes, options, mongosystem).mapTo[MongoConnection]

    connection.flatMap { con =>
      f(con, mongosystem).flatMap { res =>
        con.askClose()(timeout).recover { case _ => {} }.map(_ => res)
      }
    }
  }

  private def withCon[T](opts: MongoConnectionOptions = MongoConnectionOptions())(test: (String, MongoConnection, ActorRef) => MatchResult[T]): org.specs2.execute.Result = {
    val name = s"withCon-${System identityHashCode opts}"
    val auths = Seq(Authenticate(Common.commonDb, "test", "password"))
    val con = md.connection(
      nodes, authentications = auths, options = opts, name = Some(name))

    val res = actorSystem.actorSelection(s"/user/Monitor-$name").
      resolveOne(timeout).map(test(name, con, _)).andThen {
        case _ => try {
          Await.result(con.askClose()(timeout), timeout); ()
        } catch {
          case cause: Exception => cause.printStackTrace()
        }
      }

    res.await(0, Common.slowTimeout /* tolerate nested timeout */ )
  }
}
