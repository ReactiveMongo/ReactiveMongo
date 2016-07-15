import scala.concurrent.Future

import akka.actor.ActorRef
import akka.pattern.ask

import org.specs2.matcher.MatchResult
import org.specs2.concurrent.{ ExecutionEnv => EE }

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
  SetUnavailable
}
import reactivemongo.core.actors.Exceptions.NodeSetNotReachable
import reactivemongo.api.tests._

class NodeSetSpec extends org.specs2.mutable.Specification {
  "Node set" title

  sequential

  import Common.{ failoverStrategy, timeout }

  "Node set" should {
    lazy val md = MongoDriver()
    lazy val actorSystem = md.system

    def withConMon[T](name: String)(f: ActorRef => MatchResult[T])(implicit ee: EE): MatchResult[Future[ActorRef]] =
      actorSystem.actorSelection(s"/user/Monitor-$name").
        resolveOne(timeout) aka "actor ref" must beLike[ActorRef] {
          case ref => f(ref)
        }.await(1, timeout)

    def withCon[T](opts: MongoConnectionOptions = MongoConnectionOptions())(f: (MongoConnection, String) => T): T = {
      val name = s"con-${System identityHashCode opts}"
      val con = md.connection(Seq("node1:27017", "node2:27017"),
        authentications = Seq(Authenticate(
          Common.commonDb, "test", "password")),
        options = opts,
        name = Some(name))

      f(con, name)
    }

    "not be available" >> {
      "if the entire node set is not available" in { implicit ee: EE =>
        withCon() { (con, name) =>
          isAvailable(con) must beFalse.await(1, timeout) and {
            con.askClose()(timeout).map(_ => {}) must beEqualTo({}).
              await(1, timeout)
          }
        }
      }

      "if the primary is not available if default preference" in {
        implicit ee: EE =>
          withCon() { (con, name) =>
            withConMon(name) { conMon =>
              conMon ! SetAvailable(ProtocolMetadata.Default)

              waitIsAvailable(con, failoverStrategy).map(_ => true).recover {
                case reason: NodeSetNotReachable if (
                  reason.getMessage.indexOf(name) != -1) => false
              } must beFalse.await(1, timeout)
            }
          }
      }
    }

    "be available" >> {
      "with the primary if default preference" in { implicit ee: EE =>
        withCon() { (con, name) =>
          withConMon(name) { conMon =>
            def test = (for {
              before <- isAvailable(con)
              _ = {
                conMon ! SetAvailable(ProtocolMetadata.Default)
                conMon ! PrimaryAvailable(ProtocolMetadata.Default)
              }
              _ <- waitIsAvailable(con, failoverStrategy)
              after <- isAvailable(con)
            } yield before -> after).andThen { case _ => con.close() }

            test must beEqualTo(false -> true).await(1, timeout)
          }
        }
      }

      "without the primary if slave ok" in { implicit ee: EE =>
        val opts = MongoConnectionOptions(
          readPreference = ReadPreference.primaryPreferred)

        withCon(opts) { (con, name) =>
          withConMon(name) { conMon =>
            def test = (for {
              before <- isAvailable(con)
              _ = conMon ! SetAvailable(ProtocolMetadata.Default)
              _ <- waitIsAvailable(con, failoverStrategy)
              after <- isAvailable(con)
            } yield before -> after).andThen { case _ => con.close() }

            test must beEqualTo(false -> true).await(1, timeout)
          }
        }
      }
    }

    "be unavailable" >> {
      "with the primary unavailable if default preference" in {
        implicit ee: EE =>
          withCon() { (con, name) =>
            withConMon(name) { conMon =>
              conMon ! SetAvailable(ProtocolMetadata.Default)
              conMon ! PrimaryAvailable(ProtocolMetadata.Default)

              def test = (for {
                _ <- waitIsAvailable(con, failoverStrategy)
                before <- isAvailable(con)
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

      "without the primary if slave ok" in { implicit ee: EE =>
        val opts = MongoConnectionOptions(
          readPreference = ReadPreference.primaryPreferred)

        withCon(opts) { (con, name) =>
          withConMon(name) { conMon =>
            conMon ! SetAvailable(ProtocolMetadata.Default)

            def test = (for {
              _ <- waitIsAvailable(con, failoverStrategy)
              before <- isAvailable(con)
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

    "be closed" in {
      md.close(timeout) must not(throwA[Exception])
    }
  }

  "Connection listener" should {
    import reactivemongo.core.ConnectionListener

    "not find class StaticListenerBinder" in {
      Class.forName("reactivemongo.core.StaticListenerBinder").
        aka("class loader") must throwA[ClassNotFoundException]
    }

    "be resolved as the default one" in {
      ConnectionListener().map(_.getClass.getName) must beNone
    }
  }
}
