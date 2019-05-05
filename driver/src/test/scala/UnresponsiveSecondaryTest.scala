package reactivemongo

import scala.collection.immutable.Set
import scala.concurrent.{ Await, Future }

import akka.actor.ActorRef
import akka.testkit.TestActorRef

import org.specs2.matcher.MatchResult

import reactivemongo.bson.BSONDocument

import reactivemongo.api.MongoConnectionOptions

import reactivemongo.core.nodeset.{
  Authenticated,
  Connection,
  ConnectionStatus,
  NodeStatus,
  Node
}
import reactivemongo.core.protocol.Request
import reactivemongo.core.actors.StandardDBSystem
import reactivemongo.core.netty.ChannelBufferReadableBuffer

import reactivemongo.io.netty.channel.{ Channel, DefaultChannelId }

import _root_.tests.{ Common, NettyEmbedder }

trait UnresponsiveSecondaryTest { parent: NodeSetSpec =>
  import reactivemongo.api.tests._

  private val usd = Common.newDriver()
  @inline private def usSys = usd.system

  // ---

  def unresponsiveSecondarySpec =
    "mark as Unknown the unresponsive secondary" in {
      val opts = MongoConnectionOptions(nbChannelsPerNode = 1)
      val pingTimeout = opts.heartbeatFrequencyMS * 1000000L

      withConAndSys(usd, opts) { (con, ref) =>
        def nsState: Set[(String, NodeStatus)] =
          nodeSet(ref.underlyingActor).nodes.map { n =>
            n.name -> n.status
          }.toSet

        withConMon1(ref.underlyingActor.name) { conMon =>
          val channels = List.newBuilder[Channel]

          (for {
            state1 <- {
              updateNodeSet(ref.underlyingActor, "SetupTestChannel") {
                // Connect the test nodes with "embedded" channels
                _.updateAll { n =>
                  val cid = DefaultChannelId.newInstance()
                  val hfun = {
                    if (n.name startsWith "nodesetspec.node1:") node1Handler _
                    else node2Handler _
                  }

                  val chan = Await.result(
                    NettyEmbedder.simpleChannel(cid, nettyHandler(ref)(hfun)),
                    Common.timeout)

                  channels += chan

                  n.copy(
                    authenticated = Set(
                      Authenticated(Common.commonDb, "test")),
                    connections = Vector(connectedCon(chan)))
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
                  if (n.name startsWith "nodesetspec.node2:") {
                    // Simulate a isMaster timeout for node2
                    n.copy(pingInfo = n.pingInfo.copy(
                      lastIsMasterId = 1,
                      lastIsMasterTime = System.nanoTime() - pingTimeout))
                  } else n
                }
              }

              ref ! RefreshAll
            }
          } yield {
            val unregistered = Common.tryUntil(
              (0 to 5).map(_ * 500).toList)(
                nodeSet(ref.underlyingActor).nodes,
                (_: Vector[Node]).exists { node =>
                  if (node.name startsWith "nodesetspec.node2:") {
                    node.status == NodeStatus.Unknown
                  } else false
                })

            (state1, unregistered, nsState)
          }).map {
            case (st1, unregistered, st2) => {
              st1 must_== Set[(String, NodeStatus)](
                "nodesetspec.node1:27017" -> NodeStatus.Primary,
                "nodesetspec.node2:27017" -> NodeStatus.Secondary) and {
                  // Node2 has been detected as unresponsive

                  unregistered aka "unregistered node2" must beTrue
                } and {
                  // Then ...

                  st2 must_== Set[(String, NodeStatus)](
                    "nodesetspec.node1:27017" -> NodeStatus.Primary,
                    "nodesetspec.node2:27017" -> NodeStatus.Unknown)
                }
            }
          }.andThen {
            case _ => // Cleanup fake NodeSet
              updateNodeSet(ref.underlyingActor, "Test") {
                _.updateAll { n =>
                  //n.connections.foreach(_.channel.close())
                  // ... no need as channels were created by withChannel

                  n.copy(authenticated = Set.empty, connections = Vector.empty)
                }
              }

              channels.result().foreach {
                _.close()
              }
          }
        }
      }.andThen { case _ => usd.close() }.await(1, timeout)
    }

  // ---

  private def withConMon1[T](name: String)(f: ActorRef => Future[MatchResult[T]]): Future[MatchResult[T]] =
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
    "me" -> "nodesetspec.node1:27017",
    "setName" -> "rs0",
    "setVersion" -> 0,
    "secondary" -> false,
    "hosts" -> nodes,
    "primary" -> "nodesetspec.node1:27017")

  private def isSeco = BSONDocument(
    "ok" -> 1,
    "ismaster" -> false,
    "minWireVersion" -> 4,
    "maxWireVersion" -> 5,
    "me" -> "nodesetspec.node2:27017",
    "setName" -> "rs0",
    "setVersion" -> 0,
    "secondary" -> true, // !!
    "hosts" -> nodes,
    "primary" -> "nodesetspec.node1:27017")

  @volatile private var secAvail = true

  private def nettyHandler(ref: TestActorRef[StandardDBSystem])(isMasterResp: Boolean => Option[BSONDocument]): (Channel, Object) => Unit = {
    case (chan, req: Request) => {
      val bson = ChannelBufferReadableBuffer.document(
        req.documents.merged)

      bson.getAs[reactivemongo.bson.BSONNumberLike]("ismaster") match {
        case Some(num) if (num.toInt == 1) => {
          isMasterResp(secAvail).foreach { resp =>
            ref ! fakeResponse(
              resp,
              reqID = isMasterReqId,
              respTo = req.requestID,
              chanId = chan.id)
          }
        }

        case _ => {}
      }
    }

    case (chan, req) => Common.logger.debug(s"Skip request @ ${chan.id}: $req")
  }

  private def node1Handler(secAvail: Boolean): Option[BSONDocument] = {
    def prim = if (secAvail) isPrim else {
      isPrim -- "hosts" ++ (
        "hosts" -> nodes.filter(_ startsWith "nodesetspec.node1"))
    }

    Option(prim)
  }

  private def node2Handler(secAvail: Boolean): Option[BSONDocument] =
    if (secAvail) Some(isSeco) else None
}
