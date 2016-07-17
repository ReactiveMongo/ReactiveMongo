import scala.concurrent.{ Await, Future }

import akka.actor.{ ActorRef, Props }
import akka.pattern.ask

import org.specs2.matcher.MatchResult
import org.specs2.concurrent.{ ExecutionEnv => EE }

import reactivemongo.api.{
  FailoverStrategy,
  MongoConnection,
  MongoConnectionOptions,
  MongoDriver,
  ReadPreference
}

import reactivemongo.core.nodeset.{
  Authenticate,
  Connection,
  Node,
  ProtocolMetadata
}
import reactivemongo.core.actors.{
  MongoDBSystem,
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

  "Monitor" should {
    "manage the DB system" in { implicit ee: EE =>
      val expectFactor = 4L
      val opts = Common.DefaultOptions.copy(
        nbChannelsPerNode = 3,
        monitorRefreshMS = 3600000 // disable refreshAll/connectAll during test
        )

      withConAndSys(options = opts) { (con, dbsystem) =>
        waitIsAvailable(con, Common.failoverStrategy).map { _ =>
          Thread.sleep(750)

          //TODO:val history1 = history(dbsystem)
          val nodeset1 = nodeSet(dbsystem)
          val primary1 = nodeset1.primary
          val authCon1 = primary1.toVector.flatMap {
            _.authenticatedConnections.subject
          }
          var chanId1 = -1

          // #1
          /*TODO:
history1 aka "history #1" must not(beEmpty) and {
            primary1 aka "primary #1" must beSome[Node]
          } and 
           */
          {
            authCon1.size aka "auth'ed connections #1" must beEqualTo(
              opts.nbChannelsPerNode)
          } and { // #2
            nodeset1.pick(ReadPreference.Primary).
              aka("channel #1") must beSome[(Node, Connection)].like {
                case (node, chan) =>
                  val primary2 = nodeSet(dbsystem).primary
                  val authCon2 = primary2.toVector.flatMap {
                    _.authenticatedConnections.subject
                  }

                  node.name aka "node #1" must_== Common.primaryHost and {
                    // After one node is picked up
                    primary2.map(_.name) aka "primary #2" must beSome(
                      primary1.get.name)
                  } and {
                    // After one connection is picked up...
                    chanId1 = chan.channel.getId

                    authCon2.size aka "auth'ed connections #2" must beEqualTo(
                      opts.nbChannelsPerNode)
                  }
              }
          } and { // #3
            chanId1 aka "channel ID #1" must not(beEqualTo(-1)) and {
              dbsystem.receive(channelClosed(chanId1)) must_== {}
            } and {
              val nodeSet3 = nodeSet(dbsystem)
              val primary3 = nodeSet3.primary
              val authCon3 = primary3.toVector.flatMap {
                _.authenticatedConnections.subject
              }

              primary3.map(_.name) aka "primary #3 (after ChannelClosed)" must (
                beSome(primary1.get.name)) and {
                  authCon3.size aka "auth'ed connections #3" must beEqualTo(
                    opts.nbChannelsPerNode - 1) // after channel is closed
                } and {
                  nodeSet3.pick(ReadPreference.Primary).
                    aka("channel #2") must beSome[(Node, Connection)].like {
                      case (_, chan) =>
                        val chanId2 = chan.channel.getId

                        chanId2 must not(beEqualTo(-1)) and (
                          chanId2 must not(beEqualTo(chanId1)))
                    }
                }
            }
          }
        }
      }.await(0, timeout * expectFactor)
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

  // ---

  def withConAndSys[T](host: String = Common.primaryHost, options: MongoConnectionOptions = Common.DefaultOptions, drv: MongoDriver = Common.driver, authentications: Seq[Authenticate] = Seq.empty[Authenticate])(f: (MongoConnection, MongoDBSystem) => Future[T])(implicit ee: EE): Future[T] = {
    import reactivemongo.core.actors.StandardDBSystem

    // See MongoDriver#connection
    val supervisorName = s"Supervisor-${System identityHashCode this}"
    val poolName = s"Connection-${System identityHashCode supervisorName}"
    val nodes = Seq(host)
    lazy val dbsystem = standardDBSystem(
      supervisorName, poolName, nodes, authentications, options)

    def mongosystem = drv.system.actorOf(Props(dbsystem), poolName)
    def connection = addConnection(drv,
      poolName, nodes, options, mongosystem).mapTo[MongoConnection]

    connection.flatMap { con =>
      f(con, dbsystem).andThen { case _ => con.close() }
    }
  }
}
