import scala.concurrent.Future

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

  sequential

  import reactivemongo.api.tests._

  @inline def failoverStrategy = Common.failoverStrategy
  @inline def timeout = Common.timeout
  lazy val md = MongoDriver()
  lazy val actorSystem = md.system
  val nodes = Seq("node1:27017", "node2:27017")

  section("unit")
  "Node set" should {
    "not be available" >> {
      "if the entire node set is not available" in {
        withConAndSys(md) { (con, _) => isAvailable(con, timeout) }.
          aka("is available") must beFalse.await(1, timeout)
      }

      "if the primary is not available if default preference" in {

        withCon() { (con, name) =>
          withConMon(name) { conMon =>
            conMon ! SetAvailable(ProtocolMetadata.Default)

            waitIsAvailable(con, failoverStrategy).map(_ => true).recover {
              case reason: PrimaryUnavailableException if (
                reason.getMessage.indexOf(name) != -1) => false
            } must beFalse.await(1, timeout)
          }
        }
      }
    }

    "be available" >> {
      "with the primary if default preference" in {
        withCon() { (con, name) =>
          withConMon(name) { conMon =>
            def test = (for {
              before <- isAvailable(con, timeout)
              _ = {
                conMon ! SetAvailable(ProtocolMetadata.Default)
                conMon ! PrimaryAvailable(ProtocolMetadata.Default)
              }
              _ <- waitIsAvailable(con, failoverStrategy)
              after <- isAvailable(con, timeout)
            } yield before -> after).andThen { case _ => con.close() }

            test must beEqualTo(false -> true).await(1, timeout)
          }
        }
      }

      "without the primary if slave ok" >> {
        org.specs2.specification.core.Fragments.foreach[ReadPreference](
          Seq(ReadPreference.primaryPreferred, ReadPreference.secondary)) { readPref =>
            s"using $readPref" in {
              val opts = MongoConnectionOptions(readPreference = readPref)

              withCon(opts) { (con, name) =>
                withConMon(name) { conMon =>
                  def test = (for {
                    before <- isAvailable(con, timeout)
                    _ = conMon ! SetAvailable(ProtocolMetadata.Default)
                    _ <- waitIsAvailable(con, failoverStrategy)
                    after <- isAvailable(con, timeout)
                  } yield before -> after).andThen { case _ => con.close() }

                  test must beEqualTo(false -> true).await(1, timeout)
                }
              }
            }
          }
      }
    }

    "be unavailable" >> {
      "with the primary unavailable if default preference" in {
        withCon() { (con, name) =>
          withConMon(name) { conMon =>
            conMon ! SetAvailable(ProtocolMetadata.Default)
            conMon ! PrimaryAvailable(ProtocolMetadata.Default)

            def test = (for {
              _ <- waitIsAvailable(con, failoverStrategy)
              before <- isAvailable(con, timeout)
              _ = conMon ! PrimaryUnavailable
              after <- waitIsAvailable(
                con, failoverStrategy).map(_ => true).recover {
                case _ => false
              }
            } yield before -> after).andThen { case _ => con.close() }

            test must beEqualTo(true -> false).await(1, timeout)
          }
        }
      }

      "without the primary if slave ok" in {
        val opts = MongoConnectionOptions(
          readPreference = ReadPreference.primaryPreferred)

        withCon(opts) { (con, name) =>
          withConMon(name) { conMon =>
            conMon ! SetAvailable(ProtocolMetadata.Default)

            def test = (for {
              _ <- waitIsAvailable(con, failoverStrategy)
              before <- isAvailable(con, timeout)
              _ = conMon ! SetUnavailable
              after <- waitIsAvailable(
                con, failoverStrategy).map(_ => true).recover {
                case _ => false
              }
            } yield before -> after).andThen { case _ => con.close() }

            test must beEqualTo(true -> false).await(1, timeout)
          }
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

  def withConAndSys[T](drv: MongoDriver, options: MongoConnectionOptions = MongoConnectionOptions(nbChannelsPerNode = 1))(f: (MongoConnection, TestActorRef[StandardDBSystem]) => Future[T]): Future[T] = {
    // See MongoDriver#connection
    val supervisorName = s"withConAndSys-sup-${System identityHashCode ee}"
    val poolName = s"withConAndSys-con-${System identityHashCode f}"

    @inline implicit def sys = drv.system

    val auths = Seq(Authenticate(Common.commonDb, "test", "password"))
    lazy val mongosystem = TestActorRef[StandardDBSystem](
      standardDBSystem(supervisorName, poolName, nodes, auths, options),
      poolName)

    def connection = addConnection(
      drv, poolName, nodes, options, mongosystem).mapTo[MongoConnection]

    connection.flatMap { con =>
      f(con, mongosystem).andThen {
        case _ => con.close()
      }
    }
  }

  def withCon[T](opts: MongoConnectionOptions = MongoConnectionOptions())(f: (MongoConnection, String) => T): T = {
    val name = s"withCon-${System identityHashCode opts}"
    val auths = Seq(Authenticate(Common.commonDb, "test", "password"))
    val con = md.connection(
      nodes, authentications = auths, options = opts, name = Some(name))

    f(con, name)
  }

  def withConMon[T](name: String)(f: ActorRef => MatchResult[T]): MatchResult[Future[ActorRef]] =
    actorSystem.actorSelection(s"/user/Monitor-$name").
      resolveOne(timeout) aka "actor ref" must beLike[ActorRef] {
        case ref => f(ref)
      }.await(1, timeout)
}
