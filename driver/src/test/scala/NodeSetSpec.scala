import scala.concurrent.Future

import akka.actor.ActorRef
import akka.testkit.TestActorRef

import shaded.netty.channel.ChannelFuture

import org.specs2.matcher.MatchResult
import org.specs2.concurrent.{ ExecutionEnv => EE }

import reactivemongo.api.{
  MongoConnection,
  MongoConnectionOptions,
  MongoDriver,
  ReadPreference
}

import reactivemongo.core.nodeset.{
  Authenticate,
  Connection,
  ConnectionStatus,
  Node,
  NodeSet,
  NodeStatus,
  ProtocolMetadata
}

import reactivemongo.core.actors.{
  PrimaryAvailable,
  PrimaryUnavailable,
  SetAvailable,
  SetUnavailable,
  StandardDBSystem
}
import reactivemongo.core.actors.Exceptions.PrimaryUnavailableException

class NodeSetSpec extends org.specs2.mutable.Specification
    with UnresponsiveSecondarySpec {

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
      "if the entire node set is not available" in { implicit ee: EE =>
        withConAndSys(md) { (con, _) => isAvailable(con) }.
          aka("is available") must beFalse.await(1, timeout)
      }

      "if the primary is not available if default preference" in {
        implicit ee: EE =>
          withCon() { (con, name) =>
            withConMon(name) { conMon =>
              conMon ! SetAvailable(ProtocolMetadata.Default)

              waitIsAvailable(con, failoverStrategy).map(_ => true).recover {
                case reason: PrimaryUnavailableException if (
                  reason.getMessage.indexOf(name) != -1
                ) => false
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

      "without the primary if slave ok" >> {
        org.specs2.specification.core.Fragments.foreach[ReadPreference](
          Seq(ReadPreference.primaryPreferred, ReadPreference.secondary)
        ) { readPref =>
            s"using $readPref" in { implicit ee: EE =>
              val opts = MongoConnectionOptions(readPreference = readPref)

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
                  con, failoverStrategy
                ).map(_ => true).recover {
                  case _ => false
                }
              } yield before -> after).andThen { case _ => con.close() }

              test must beEqualTo(true -> false).await(1, timeout)
            }
          }
      }

      "without the primary if slave ok" in { implicit ee: EE =>
        val opts = MongoConnectionOptions(
          readPreference = ReadPreference.primaryPreferred
        )

        withCon(opts) { (con, name) =>
          withConMon(name) { conMon =>
            conMon ! SetAvailable(ProtocolMetadata.Default)

            def test = (for {
              _ <- waitIsAvailable(con, failoverStrategy)
              before <- isAvailable(con)
              _ = conMon ! SetUnavailable
              after <- waitIsAvailable(
                con, failoverStrategy
              ).map(_ => true).recover {
                case _ => false
              }
            } yield before -> after).andThen { case _ => con.close() }

            test must beEqualTo(true -> false).await(1, timeout)
          }
        }
      }
    }

    def connectAllSpec(specTitle: String, checkCon: List[String] => MatchResult[Any])(conAll: StandardDBSystem => NodeSet => ((Node, ChannelFuture) => (Node, ChannelFuture)) => NodeSet) = specTitle in { implicit ee: EE =>
      val coned = List.newBuilder[String]

      withConAndSys(md) { (con, ref) =>
        def node(chanId: Int, host: String, chanConnected: Boolean, status: ConnectionStatus): Node =
          NettyEmbedder.withChannel(chanId, chanConnected, _ => ()) { chan =>
            val con = Connection(
              chan, status,
              authenticated = Set.empty,
              authenticating = None
            )

            Node(
              host,
              NodeStatus.Unknown,
              Vector(con),
              Set.empty,
              None,
              ProtocolMetadata.Default
            )
          }

        val node1 = node(1, "node:27017",
          true, ConnectionStatus.Connected)

        val node2 = node(2, "localhost:27018",
          false, ConnectionStatus.Disconnected)

        val node3 = node(3, "localhost:27019",
          false, ConnectionStatus.Connecting)

        val node4 = node(4, "localhost:27020",
          false, ConnectionStatus.Disconnected)

        val ns = NodeSet(Some("foo"), None,
          Vector(node1, node2, node3, node4), Set.empty)

        def concurCon = Future(conAll(ref.underlyingActor)(ns) { (n, c) =>
          coned.synchronized {{
            coned += n.name
          }

          n -> c
        })

        Future.sequence((0 to 10).map(_ => concurCon)).
          map(_.flatMap(_.nodes).flatMap { node =>
            node.connections.map { node.name -> _.status }
          }.toSet)
      } must beEqualTo(Set[(String, ConnectionStatus)](
        "node:27017" -> ConnectionStatus.Connected, // already connect
        "localhost:27018" -> ConnectionStatus.Connecting,
        "localhost:27018" -> ConnectionStatus.Connected,
        "localhost:27019" -> ConnectionStatus.Connecting,
        "localhost:27020" -> ConnectionStatus.Connecting,
        "localhost:27020" -> ConnectionStatus.Connected
      )).await(0, timeout) and checkCon(coned.result())
    }

    (connectAllSpec("connect all the nodes without synchronization", {
      _.toSet must_== Set("localhost:27018", "localhost:27020")
    }) { sys =>
      // !! override the nodeset synchronization that takes place normally
      { ns =>
        connectAll(sys, ns, _)
      }
    })

    (connectAllSpec("connect all the nodes with synchronization", {
      _.sorted must_== List("localhost:27018", "localhost:27020")
    }) { sys =>
      { ns =>
        { f =>
          ns.synchronized {
            connectAll(sys, ns, f)
          }
        }
      }
    })

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

  def withConAndSys[T](drv: MongoDriver, options: MongoConnectionOptions = MongoConnectionOptions(nbChannelsPerNode = 1))(f: (MongoConnection, TestActorRef[StandardDBSystem]) => Future[T])(implicit ee: EE): Future[T] = {
    // See MongoDriver#connection
    val supervisorName = s"Supervisor-${System identityHashCode ee}"
    val poolName = s"Connection-${System identityHashCode f}"

    @inline implicit def sys = drv.system

    lazy val mongosystem = TestActorRef[StandardDBSystem](
      standardDBSystem(
        supervisorName, poolName,
        nodes,
        Seq(Authenticate(Common.commonDb, "test", "password")), options
      ), poolName
    )

    def connection = addConnection(
      drv, poolName, nodes, options, mongosystem
    ).mapTo[MongoConnection]

    connection.flatMap { con =>
      f(con, mongosystem).andThen {
        case _ =>
          con.close()
      }
    }
  }

  def withCon[T](opts: MongoConnectionOptions = MongoConnectionOptions())(f: (MongoConnection, String) => T): T = {
    val name = s"con-${System identityHashCode opts}"
    val con = md.connection(
      nodes,
      authentications = Seq(Authenticate(
        Common.commonDb, "test", "password"
      )),
      options = opts,
      name = Some(name)
    )

    f(con, name)
  }

  def withConMon[T](name: String)(f: ActorRef => MatchResult[T])(implicit ee: EE): MatchResult[Future[ActorRef]] =
    actorSystem.actorSelection(s"/user/Monitor-$name").
      resolveOne(timeout) aka "actor ref" must beLike[ActorRef] {
        case ref => f(ref)
      }.await(1, timeout)

}
