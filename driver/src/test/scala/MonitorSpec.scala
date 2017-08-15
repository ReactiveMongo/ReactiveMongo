import akka.actor.Actor
import akka.testkit.TestActorRef

import scala.concurrent.Future

import org.specs2.concurrent.{ ExecutionEnv => EE }

import reactivemongo.core.actors.StandardDBSystem
import reactivemongo.core.nodeset.{ Authenticate, Connection, Node }
import reactivemongo.core.protocol.Response

import reactivemongo.api.{
  MongoConnection,
  MongoConnectionOptions,
  MongoDriver,
  ReadPreference
}

class MonitorSpec extends org.specs2.mutable.Specification {
  "Monitor" title

  import reactivemongo.api.tests._
  import Common.{ timeout, tryUntil }

  "Monitor" should {
    "manage a single node DB system" in { implicit ee: EE =>
      val expectFactor = 3L
      val opts = Common.DefaultOptions.copy(
        nbChannelsPerNode = 3,
        monitorRefreshMS = 3600000 // disable refreshAll/connectAll during test
      )

      withConAndSys(options = opts) { (con, sysRef) =>
        @inline def dbsystem = sysRef.underlyingActor

        waitIsAvailable(con, Common.failoverStrategy).map { _ =>
          Thread.sleep(250)

          val history1 = history(dbsystem)
          val nodeset1 = nodeSet(dbsystem)
          val primary1 = nodeset1.primary
          val authCon1 = primary1.toVector.flatMap {
            _.authenticatedConnections.subject
          }
          var chanId1 = -1

          // #1
          history1 aka "history #1" must not(beEmpty) and {
            primary1 aka "primary #1" must beSome[Node]
          } and {
            authCon1.size aka "authed connections #1" must beLike[Int] {
              case number => number must beGreaterThan(1) and (
                number must beLessThanOrEqualTo(opts.nbChannelsPerNode))
            }
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

                    authCon2.size aka "authed connections #2" must beLike[Int] {
                      case number => number must beGreaterThan(1) and (
                        number must beLessThanOrEqualTo(opts.nbChannelsPerNode))
                    }
                  }
              }
          } and { // #3
            chanId1 aka "channel ID #1" must not(beEqualTo(-1)) and {
              dbsystem.receive(channelClosed(chanId1)) must_== {}
            } and {
              val nodeSet3 = nodeSet(dbsystem)
              val primary3 = nodeSet3.primary

              primary3.map(_.name) aka "primary #3 (after ChannelClosed)" must (
                beSome(primary1.get.name)) and {
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

    "manage unhandled Actor exception and Akka Restart" in { implicit ee: EE =>
      val expectFactor = 5L
      val opts = Common.DefaultOptions.copy(
        nbChannelsPerNode = 3,
        monitorRefreshMS = 3600000 // disable refreshAll/connectAll during test
      )

      withConAndSys(options = opts) { (con, sysRef) =>
        @inline def dbsystem = sysRef.underlyingActor

        waitIsAvailable(con, Common.failoverStrategy).map { _ =>
          Thread.sleep(250)

          val nodeset1 = nodeSet(dbsystem)
          val primary1 = nodeset1.primary
          val authCon1 = primary1.toVector.flatMap {
            _.authenticatedConnections.subject
          }

          // #1
          primary1 aka "primary #1" must beSome[Node] and {
            authCon1 aka "connections #1" must not(beEmpty)
          } and {
            nodeset1.pick(ReadPreference.Primary).
              aka("channel #1") must beSome[(Node, Connection)]
          } and { // #2
            val respWithNulls = Response(null, null, null, null)

            dbsystem.receive(respWithNulls).
              aka("invalid response") must throwA[NullPointerException] and {
                sysRef.tell(respWithNulls, Actor.noSender) must_== {}
              }
          } and { // #3
            // Akka Restart on unhandled exception (see issue 558)
            tryUntil[Traversable[(Long, String)]](
              List(125, 250, 500, 1000, 2125))(
                history(dbsystem), _.exists(_._2.startsWith("Restart("))) aka "history #3" must beTrue

          } and { // #4 (see issue 558)
            tryUntil[Option[Node]](List(125, 250, 500, 1000, 2125))(
              nodeSet(dbsystem).primary, _.isDefined) aka "primary #4" must beTrue
          } and { // #5
            val nodeSet5 = nodeSet(dbsystem)
            val primary5 = nodeSet5.primary

            primary5.map(_.name) aka "primary #5 (after Akka Restart)" must (
              beSome(primary1.get.name)) and {
                nodeSet5.pick(ReadPreference.Primary).
                  aka("channel #5") must beSome[(Node, Connection)]
              }
          }
        }
      }.await(0, timeout * expectFactor)
    }
  }

  // ---

  def withConAndSys[T](nodes: Seq[String] = Seq(Common.primaryHost), options: MongoConnectionOptions = Common.DefaultOptions, drv: MongoDriver = Common.driver, authentications: Seq[Authenticate] = Seq.empty[Authenticate])(f: (MongoConnection, TestActorRef[StandardDBSystem]) => Future[T])(implicit ee: EE): Future[T] = {
    // See MongoDriver#connection
    val supervisorName = s"Supervisor-${System identityHashCode ee}"
    val poolName = s"Connection-${System identityHashCode f}"

    implicit def sys: akka.actor.ActorSystem = drv.system
    lazy val mongosystem = TestActorRef[StandardDBSystem](
      standardDBSystem(
        supervisorName, poolName, nodes, authentications, options), poolName)

    def connection = addConnection(
      drv,
      poolName, nodes, options, mongosystem).mapTo[MongoConnection]

    connection.flatMap { con =>
      f(con, mongosystem).andThen { case _ => con.close() }
    }
  }
}
