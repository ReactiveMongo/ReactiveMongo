import scala.collection.immutable.Set
import scala.concurrent.Future

import akka.actor.ActorRef
import akka.testkit.TestActorRef

import org.specs2.matcher.MatchResult
import org.specs2.concurrent.{ ExecutionEnv => EE }

import reactivemongo.bson.BSONDocument

import reactivemongo.core.nodeset.{
  Authenticated,
  Connection,
  ConnectionStatus,
  NodeStatus,
  PingInfo,
  Node
}
import reactivemongo.core.protocol.Request
import reactivemongo.core.actors.StandardDBSystem
import reactivemongo.core.netty.ChannelBufferReadableBuffer

import shaded.netty.channel.{ Channel, ChannelEvent }

trait UnresponsiveSecondarySpec { parent: NodeSetSpec =>
  import reactivemongo.api.tests._
  import NettyEmbedder.withChannel

  private val usd = reactivemongo.api.MongoDriver()
  lazy val usSys = usd.system

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run() = usd.close()
  })

  // ---

  def unresponsiveSecondarySpec =
    "mark as Unknown the unresponsive secondary" in { implicit ee: EE =>
      withConAndSys(usd) { (con, ref) =>
        def nsState: Set[(String, NodeStatus)] =
          nodeSet(ref.underlyingActor).nodes.map { n =>
            n.name -> n.status
          }.toSet

        withConMon1(ref.underlyingActor.name) { conMon =>
          (for {
            before <- isAvailable(con)

            state1 <- {
              updateNodeSet(ref.underlyingActor, "SetupTestChannel") {
                // Connect the test nodes with "embedded" channels
                _.updateAll { n =>
                  val cid = System.identityHashCode(n).toInt
                  val hfun = {
                    if (n.name startsWith "node1:") node1Handler _
                    else node2Handler _
                  }

                  withChannel(cid, nettyHandler(ref)(hfun)) { chan =>
                    n.copy(
                      authenticated = Set(
                        Authenticated(Common.commonDb, "test")),
                      connections = Vector(connectedCon(chan)))
                  }
                }
              }

              ref ! RefreshAll

              waitIsAvailable(con, failoverStrategy).map { _ => nsState }
            }

            _ = {
              // Make sure the embedded NodeSet won't expose the secondary
              secAvail = false

              updateNodeSet(ref.underlyingActor, "Test") {
                // Connect the test nodes with "embedded" channels
                _.updateAll { n =>
                  if (n.name startsWith "node2:") {
                    // Simulate a isMaster timeout for node2
                    n.copy(pingInfo = n.pingInfo.copy(
                      lastIsMasterId = 1,
                      lastIsMasterTime = (
                        System.currentTimeMillis() - PingInfo.pingTimeout)))
                  } else n
                }
              }

              ref ! RefreshAll
            }
          } yield {
            val unregistered = Common.tryUntil((0 to 5).map(_ * 500).toList)(
              nodeSet(ref.underlyingActor).nodes,
              (_: Vector[Node]).exists { node =>
                if (node.name startsWith "node2:") {
                  node.status == NodeStatus.Unknown
                } else false
              })

            (before, state1, unregistered, nsState)
          }).map {
            case (availBefore, st1, unregistered, st2) => {
              availBefore must beFalse and {
                // Once the NodeSet is available ...

                st1 must_== Set[(String, NodeStatus)](
                  "node1:27017" -> NodeStatus.Primary,
                  "node2:27017" -> NodeStatus.Secondary)
              } and {
                // Node2 has been detected as unresponsive

                unregistered aka "unregistered node2" must beTrue
              } and {
                // Then ...

                st2 must_== Set[(String, NodeStatus)](
                  "node1:27017" -> NodeStatus.Primary,
                  "node2:27017" -> NodeStatus.Unknown)
              }
            }
          }.andThen {
            case _ => // Cleanup fake NodeSet
              updateNodeSet(ref.underlyingActor, "Test") {
                _.updateAll { n =>
                  n.connections.foreach(_.channel.close())

                  n.copy(authenticated = Set.empty, connections = Vector.empty)
                }
              }

              usd.close()
          }
        }
      }.await(1, timeout)
    }

  // ---

  private def withConMon1[T](name: String)(f: ActorRef => Future[MatchResult[T]])(implicit ee: EE): Future[MatchResult[T]] =
    usSys.actorSelection(s"/user/Monitor-$name").
      resolveOne(timeout).flatMap(f)

  private def connectedCon(channel: Channel) = Connection(
    channel = channel,
    status = ConnectionStatus.Connected,
    authenticated = Set(Authenticated(Common.commonDb, "test")),
    authenticating = None)

  private def isPrim = BSONDocument(
    "ok" -> 1,
    "ismaster" -> true,
    "minWireVersion" -> 4,
    "maxWireVersion" -> 5,
    "me" -> "node1:27017",
    "setName" -> "rs0",
    "setVersion" -> 0,
    "secondary" -> false,
    "hosts" -> nodes,
    "primary" -> "node1:27017")

  private def isSeco = BSONDocument(
    "ok" -> 1,
    "ismaster" -> false,
    "minWireVersion" -> 4,
    "maxWireVersion" -> 5,
    "me" -> "node2:27017",
    "setName" -> "rs0",
    "setVersion" -> 0,
    "secondary" -> true, // !!
    "hosts" -> nodes,
    "primary" -> "node1:27017")

  @volatile private var secAvail = true

  private def nettyHandler(ref: TestActorRef[StandardDBSystem])(isMasterResp: Boolean => Option[BSONDocument]): ChannelEvent => Unit = {
    case evt @ (msg: shaded.netty.channel.DownstreamMessageEvent) if (
      msg.getMessage.isInstanceOf[Request]) => {
      val req = msg.getMessage.asInstanceOf[Request]
      val bson = ChannelBufferReadableBuffer.document(
        req.documents.merged)

      bson.getAs[reactivemongo.bson.BSONNumberLike]("ismaster") match {
        case Some(num) if (num.toInt == 1) => {
          isMasterResp(secAvail).foreach { resp =>
            ref ! fakeResponse(
              resp,
              reqID = isMasterReqId,
              respTo = req.requestID,
              chanId = evt.getChannel.getId)
          }
        }

        case _ => {}
      }
    }

    case req => Common.logger.debug(s"Skip request: $req")
  }

  private def node1Handler(secAvail: Boolean): Option[BSONDocument] = {
    def prim = if (secAvail) isPrim else {
      isPrim -- "hosts" ++ (
        "hosts" -> nodes.filter(_ startsWith "node1"))
    }

    Option(prim)
  }

  private def node2Handler(secAvail: Boolean): Option[BSONDocument] =
    if (secAvail) Some(isSeco) else None

}
