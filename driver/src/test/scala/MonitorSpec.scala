package reactivemongo

import scala.util.{ Failure, Success }

import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration._

import akka.actor.Actor
import akka.testkit.TestActorRef

import reactivemongo.io.netty.channel.{
  ChannelFuture,
  ChannelFutureListener,
  ChannelId,
  DefaultChannelId
}

import org.specs2.execute.Result
import org.specs2.concurrent.ExecutionEnv

import reactivemongo.core.actors.StandardDBSystem
import reactivemongo.core.nodeset.{ Authenticate, Connection, Node, NodeSet }
import reactivemongo.core.protocol.{ Response, ResponseInfo }

import reactivemongo.api.{
  MongoConnection,
  MongoConnectionOptions,
  MongoDriver,
  ReadPreference
}

import _root_.tests.Common

final class MonitorSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification {

  "Monitor" title

  import reactivemongo.api.tests._
  import Common.{ timeout, tryUntil }

  "Monitor" should {
    "manage a single node DB system" in {
      val expectFactor = 3L
      val opts = Common.DefaultOptions.copy(
        nbChannelsPerNode = 3,
        heartbeatFrequencyMS = 3600000 // disable refreshAll/connectAll during test
      )

      eventually(2, timeout) {
        withConAndSys(options = opts) { (con, sysRef) =>
          @inline def dbsystem = sysRef.underlyingActor

          waitIsAvailable(con, Common.failoverStrategy).map { _ =>
            //Thread.sleep(250)

            val history1 = history(dbsystem)
            var nodeset1: NodeSet = null
            var primary1 = Option.empty[Node]
            var authCon1 = Vector.empty[Connection]
            var chanId1: ChannelId = null

            def authedCons = primary1.toVector.flatMap {
              _.authenticatedConnections.subject
            }

            // #1
            history1 aka "history #1" must not(beEmpty) and {
              eventually(1, timeout) {
                nodeset1 = nodeSet(dbsystem)
                primary1 = nodeset1.primary

                primary1 aka "primary #1" must beSome[Node]
              }
            } and eventually(1, 3.seconds) {
              authCon1 = authedCons

              authCon1.size aka "authed connections #1" must_=== 1
              // ... as connectAll is disabled by heartbeatFrequencyMS,
              // so only the first user connection could be there
            } and { // #2
              nodeset1.pick(ReadPreference.Primary, _ => true).
                aka("channel #1") must beSome[(Node, Connection)].like {
                  case (node, con) =>
                    val primary2 = nodeSet(dbsystem).primary
                    val authCon2 = primary2.toVector.flatMap {
                      _.authenticatedConnections.subject
                    }

                    node.name aka "node #1" must_=== Common.primaryHost and {
                      // After one node is picked up
                      primary2.map(_.name) aka "primary #2" must beSome(
                        primary1.get.name)
                    } and {
                      // After one connection is picked up...
                      chanId1 = con.channel.id

                      authCon2.size aka "authed #2" must_=== 1
                    }
                }
            } and { // #3
              chanId1 aka "channel ID #1" must not(beNull) and {
                dbsystem.receive(channelClosed(chanId1)) must_=== {}
              } and {
                // after ChannelClosed, no user/data connection,
                // but only signaling

                val nodeSet3 = nodeSet(dbsystem)

                nodeSet3.primary must beSome[Node].which { primary3 =>
                  primary1.map(_.name) must beSome(primary3.name) and {
                    primary3.signaling must beSome[Connection]
                  } and {
                    nodeSet3.pick(ReadPreference.Primary, _ => true).
                      aka("channel #3") must beNone
                  }
                }
              }
            }
          }
        }.await(0, timeout * expectFactor)
      }
    }

    "manage unhandled Actor exception and Akka Restart" in {
      val expectFactor = 5L
      val opts = Common.DefaultOptions.copy(
        nbChannelsPerNode = 3,
        heartbeatFrequencyMS = 3600000 // disable refreshAll/connectAll during test
      )

      // Disable logging (as simulating errors)
      val log = org.apache.logging.log4j.LogManager.
        getLogger("akka.actor.OneForOneStrategy").
        asInstanceOf[org.apache.logging.log4j.core.Logger]

      val level = log.getLevel
      log.setLevel(org.apache.logging.log4j.Level.OFF)
      //

      withConAndSys(options = opts) { (con, sysRef) =>
        @inline def dbsystem = sysRef.underlyingActor

        waitIsAvailable(con, Common.failoverStrategy).map { _ =>
          Thread.sleep(250)

          val nodeset1 = nodeSet(dbsystem)
          val primary1 = nodeset1.primary
          var authCon1 = Vector.empty[Connection]

          // #1
          primary1 aka "primary #1" must beSome[Node] and {
            eventually(1, 3.seconds) {
              authCon1 = primary1.toVector.flatMap {
                _.authenticatedConnections.subject
              }

              authCon1 aka "connections #1" must not(beEmpty)
            }
          } and {
            nodeset1.pick(ReadPreference.Primary, _ => true).
              aka("channel #1") must beSome[(Node, Connection)]
          } and { // #2
            val respWithNulls = Response(null, null, null,
              ResponseInfo(DefaultChannelId.newInstance()))

            dbsystem.receive(respWithNulls).
              aka("invalid response") must throwA[NullPointerException] and {
                sysRef.tell(respWithNulls, Actor.noSender) must_=== {}
              }
          } and eventually(1, 3.seconds) {
            // #3 Akka Restart on unhandled exception (see issue 558)

            tryUntil[Traversable[(Long, String)]](
              List(125, 250, 500, 1000, 2125, 4096))(
                history(dbsystem), _.exists(_._2 startsWith "Restart")).
                aka("history #3") must beTrue

          } and eventually(1, 3.seconds) { // #4 (see issue 558)
            tryUntil[Option[Node]](List(125, 250, 500, 1000, 2125))(
              nodeSet(dbsystem).primary, _.isDefined).
              aka("primary #4") must beTrue

          } and { // #5
            val nodeSet5 = nodeSet(dbsystem)
            val primary5 = nodeSet5.primary

            primary5.map(_.name) aka "primary #5 (after Akka Restart)" must (
              beSome(primary1.get.name)) and eventually(1, timeout) {
                nodeSet5.pick(ReadPreference.Primary, _ => true).
                  aka("channel #5") must beSome[(Node, Connection)]
              }
          }
        }
      }.andThen {
        case _ => log.setLevel(level)
      }.await(0, timeout * expectFactor)
    }

    "manage channel disconnection while probing isMaster" in {
      val expectFactor = 4L
      val opts = Common.DefaultOptions.copy(
        nbChannelsPerNode = 2,
        heartbeatFrequencyMS = 3600000 // disable refreshAll/connectAll during test
      )
      val unavailTimeout = timeout + 1.second

      withConAndSys(options = opts) { (con, sysRef) =>
        @inline def dbsystem = sysRef.underlyingActor

        //println(s"MonitorSpec_1: ${System.currentTimeMillis()}")

        Future.successful(eventually(2, timeout) {
          isAvailable(con, timeout) must beTrue.await(0, timeout)
        } and {
          @volatile var connections1 = Vector.empty[Connection]

          //println("MonitorSpec_2")

          eventually(2, timeout) {
            nodeSet(dbsystem).nodes aka "nodes #1" must beLike[Vector[Node]] {
              // #1 - Fully available with expected connection count (1)

              case nodes1 => nodes1.size must_=== 1 and {
                isAvailable(con, 1.seconds) must beTrue.await(0, timeout)
              } and {
                nodes1.flatMap(_.connections) must beLike[Vector[Connection]] {
                  case cons =>
                    // 1 op channel + 1 signaling
                    cons.size aka "connections #1" must_=== 2 and {
                      connections1 = cons
                      ok
                    }
                }
              }
            }
          } and {
            // #2 - Pass messages to the system to indicate
            // all the connections are closed,
            // even if the underlying channels are not

            connections1.foreach { con1 =>
              dbsystem.receive(channelClosed(con1.channel.id)) // ensure
            } must_=== ({})
          } and {
            // #3 - After connections are closed:
            // no longer available, no connected connection
            nodeSet(dbsystem).nodes.flatMap(_.connected) must beEmpty and {
              isAvailable(con, 1.seconds) must beFalse.await(1, timeout)
            }
          } and {
            //println("MonitorSpec_3")

            // #4 - Pass message to the system so the first connection
            // is considered connected, so it's used to probe isMaster again;
            // The channel of this connection is deregistered,
            // so the incoming buffer is not read
            // (and so no isMaster response).

            val before4 = System.nanoTime()

            nodeSet(dbsystem).nodes.flatMap(_.connections).foreach { con1 =>
              con1.channel.deregister()
              /* ... so isMaster sent on ChannelConnected
               thereafter cannot succeed */

              dbsystem.receive(channelConnected(con1.channel.id))
            }

            val ns = nodeSet(dbsystem)

            ns.nodes.flatMap(_.connected).size must_=== 1 and {
              ns.nodes.headOption.map(
                _.pingInfo.lastIsMasterTime) must beSome[Long].which {
                  // a new isMaster ping must have been sent
                  // as the first connection is again available
                  _ must beGreaterThan(before4)
                }
            } and {
              // The nodeset is still not available,
              // as the channel of the first connection used for isMaster
              // is deregistered/paused for now
              eventually(2, timeout) {
                isAvailable(con, timeout) must beFalse.await(0, unavailTimeout)
              }
            }
          } and {
            //println("MonitorSpec_5")

            // #5 - Completely close the channel (only deregistered until now)
            // of the first connection, which is waiting for isMaster response

            connections1.foreach { con1 =>
              // Simulate channel disconnection for the pool,
              // without actually closing the Netty channel,
              // so `updateNodeSetOnDisconnect` is applied
              // and result in send a new isMaster request
              dbsystem.receive(channelClosed(con1.channel.id))

              // Pending ping must be discard as the corresponding connection
              // is indicated as closed (see MongoDBSystem#cch2)
              con1.channel.close()
            }

            nodeSet(dbsystem).nodes.headOption.
              map(_.pingInfo.lastIsMasterId) must beSome(-1) and {
                nodeSet(dbsystem).nodes.flatMap(_.connected) must beEmpty
              } and {
                isAvailable(con, timeout) must beFalse.await(1, unavailTimeout)
              }
          } and {
            //println("MonitorSpec_6")

            val signaling = Promise[Unit]()

            updateNodeSet(dbsystem, "MonitorSpec#6") {
              _.updateAll { n =>
                // Direct call createSignalingConnection has previous channel
                // was unregistered

                n.copy(connections = Vector.empty).createSignalingConnection(
                  dbsystem.channelFactory, sysRef) match {
                    case Success(upd) => {
                      signaling.success({})
                      upd
                    }

                    case Failure(err) => {
                      signaling.failure(err)
                      n
                    }
                  }
              }
            }

            // #6 - Pass message to the system to indicate the second
            // (only remaining) connection is back online.
            val connections6 = nodeSet(dbsystem).nodes.flatMap(_.connections)

            signaling.future must beTypedEqualTo({}).await and {
              connections6.size must_=== 1
            } and {
              /*
              connections6.foreach { c =>
                dbsystem.receive(channelConnected(c.channel.id))
              }
               */

              isAvailable(con, 1.seconds) must beTrue.await(1, timeout)
            } and {
              nodeSet(dbsystem).nodes.
                flatMap(_.connections) must beLike[Vector[Connection]] {
                  case cons => cons.size must be_>=(1) and {
                    cons.find(_.signaling) must beSome[Connection]
                  }
                }
            }
          }
        })
      }.awaitFor(timeout * expectFactor)
    }

    "manage reconnection according heartbeat frequency" >> {
      val expectFactor = 4L

      def withClosedChannels[T](ms: Int)(f: (MongoConnection, TestActorRef[StandardDBSystem]) => Result): Result = {
        val opts = Common.DefaultOptions.copy(
          nbChannelsPerNode = 2,
          heartbeatFrequencyMS = ms)

        withConAndSys(options = opts) { (con, sysRef) =>
          @inline def dbsystem = sysRef.underlyingActor

          Future.successful(eventually(1, timeout) {
            isAvailable(con, timeout) must beTrue.await(0, timeout)
          } and {
            @volatile var closed = 0
            @volatile var count = 0

            nodeSet(dbsystem).nodes.flatMap {
              _.connections.map { c =>
                count = count + 1

                c.channel.close().addListener(new ChannelFutureListener {
                  def operationComplete(op: ChannelFuture): Unit = {
                    if (op.isSuccess) {
                      closed = closed + 1
                    }
                  }
                })
              }
            }

            count must be_<=(opts.nbChannelsPerNode + 1 /*signaling*/ ) and {
              eventually(2, timeout) {
                closed must_=== count
              }
            }
          } and {
            eventually(1, timeout) {
              f(con, sysRef)
            }
          })
        }.awaitFor(timeout * expectFactor)
      }

      "so re-connect quickly with a short heartbeat (500ms)" in {
        withClosedChannels(500) {
          (con, _) => isAvailable(con, timeout) must beTrue.await(0, timeout)
        }
      }

      "so doesn't re-connect with a long heartbeat (1h)" in {
        withClosedChannels(3600000) {
          (con, _) => isAvailable(con, timeout) must beFalse.await(0, timeout)
        }
      }
    }
  }

  // ---

  private def withConAndSys[T](
    options: MongoConnectionOptions,
    nodes: Seq[String] = Seq(Common.primaryHost),
    drv: MongoDriver = Common.driver,
    authentications: Seq[Authenticate] = Seq.empty[Authenticate])(f: (MongoConnection, TestActorRef[StandardDBSystem]) => Future[T]): Future[T] = {
    // See MongoDriver#connection
    val supervisorName = s"monitorspec-sup-${System identityHashCode ee}"
    val poolName = s"monitorspec-con-${System identityHashCode f}"

    implicit def sys: akka.actor.ActorSystem = drv.system
    lazy val mongosystem = TestActorRef[StandardDBSystem](
      standardDBSystem(
        supervisorName, poolName, nodes, authentications, options), poolName)

    def connection = addConnection(
      drv, poolName, nodes, options, mongosystem).mapTo[MongoConnection]

    for {
      con <- connection
      res <- f(con, mongosystem)
      _ <- con.askClose()(timeout).recover { case _ => () }
    } yield res
  }
}
