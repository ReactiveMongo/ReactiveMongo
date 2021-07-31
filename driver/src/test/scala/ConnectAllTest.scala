package reactivemongo

import java.net.SocketAddress
import java.nio.channels.ClosedChannelException

import scala.concurrent.Future

import reactivemongo.io.netty.channel.{
  DefaultChannelPromise,
  ChannelFuture,
  ChannelId,
  DefaultChannelId
}
import reactivemongo.io.netty.channel.embedded.EmbeddedChannel

import reactivemongo.core.nodeset.{
  Connection,
  ConnectionStatus,
  Node,
  NodeSet,
  NodeStatus,
  PingInfo
}
import reactivemongo.core.protocol.ProtocolMetadata

import reactivemongo.core.actors.StandardDBSystem

import reactivemongo.api.MongoConnectionOptions

import _root_.tests.NettyEmbedder

trait ConnectAllTest { _: NodeSetSpec =>
  import reactivemongo.api.tests.connectAll

  private val testhost = java.net.InetAddress.getLocalHost.getHostName

  def connectAllSpec = {
    withNodeSet("connect all the nodes without synchronization") { sys =>
      // !! override the nodeset synchronization that takes place normally
      { ns =>
        connectAll(sys, ns)
      }
    }

    withNodeSet("connect all the nodes with synchronization") { sys =>
      { ns =>
        ns.synchronized {
          connectAll(sys, ns)
        }
      }
    }

    { // Make sure closed channels are properly handled
      import _root_.tests.Common.slowTimeout

      lazy val ChanId1 = DefaultChannelId.newInstance()
      lazy val ChanId2 = DefaultChannelId.newInstance()

      def connections(): Future[Vector[Connection]] = {
        Future.sequence(Seq(ChanId1, ChanId2).map { chanId =>
          NettyEmbedder.simpleChannel(chanId, false).map { chan =>
            val channel: EmbeddedChannel = {
              if (chanId != ChanId2) {
                chan
              } else {
                new EmbeddedChannel(chanId, false, false) {
                  override def connect(addr: SocketAddress): ChannelFuture = {
                    val p = new DefaultChannelPromise(chan)

                    // See MongoDBSystem#cch3
                    p.setFailure(new ClosedChannelException())

                    p
                  }

                  // See MongoDBSystem#cch4
                  override def isOpen(): Boolean = false
                }
              }
            }

            new Connection(
              channel, ConnectionStatus.Disconnected,
              authenticated = Set.empty,
              authenticating = None,
              signaling = false)
          }
        }).map(_.toVector)
      }

      "connect all with closed channel" in {
        def opts = MongoConnectionOptions.default.copy(
          nbChannelsPerNode = 1,
          heartbeatFrequencyMS = 500)

        withConAndSys(md, options = opts, _nodes = Seq.empty) { (_, ref) =>
          import ref.{ underlyingActor => sys }

          val nodeSet: Future[NodeSet] = (for {
            conns <- connections()
            node = new Node(
              s"${testhost}:27017",
              Set.empty,
              NodeStatus.Unknown,
              conns,
              Set.empty,
              tags = Map.empty[String, String],
              ProtocolMetadata.Default,
              PingInfo(),
              false,
              System.nanoTime())
            ns = new NodeSet(Some("foo"), None, Vector(node), Set.empty)
            _ = reactivemongo.api.tests.updateNodeSet(sys, "_test")(_ => ns)
            _ = connectAll(sys, ns)
          } yield reactivemongo.api.tests.nodeSet(sys))

          val conns = nodeSet.map(_.nodes.flatMap(_.connections))

          @annotation.tailrec
          def check(i: Int): Boolean = {
            val cs = reactivemongo.api.tests.
              nodeSet(sys).nodes.flatMap(_.connections).map { con =>
                con.status -> con.channel.id
              }.toSeq

            cs match {
              case Seq(
                Tuple2(ConnectionStatus.Connected, ChanId1),
                Tuple2(ConnectionStatus.Connecting, chanId)) if (chanId != ChanId2) =>
                // See #cch5: ChanId2 has been replaced
                //println("_ok")
                true

              case _ if (i < 10) => {
                Thread.sleep(500L)
                check(i + 1)
              }

              case _ =>
                false
            }
          }

          conns.map {
            _.map(c => c.status -> c.channel.id).toSeq -> check(0)
          }
        } must beTypedEqualTo(Seq[(ConnectionStatus, ChannelId)](
          Tuple2(ConnectionStatus.Disconnected, ChanId1),
          Tuple2(ConnectionStatus.Connecting, ChanId2)) -> true).awaitFor(slowTimeout)
      }
    }
  }

  // ---

  private lazy val connectAllNodes: Vector[Tuple4[ChannelId, String, Boolean, ConnectionStatus]] = {
    val ChanId1 = DefaultChannelId.newInstance()
    val ChanId2 = DefaultChannelId.newInstance()
    val ChanId3 = DefaultChannelId.newInstance()
    val ChanId4 = DefaultChannelId.newInstance()

    val node1 = Tuple4(ChanId1, s"$testhost:27017",
      true, ConnectionStatus.Connected)

    val node2 = Tuple4(ChanId2, s"$testhost:27018",
      true, ConnectionStatus.Disconnected)

    val node3 = Tuple4(ChanId3, s"$testhost:27019",
      false, ConnectionStatus.Connecting)

    val node4 = Tuple4(ChanId4, s"$testhost:27020",
      false, ConnectionStatus.Disconnected)

    Vector(node1, node2, node3, node4)
  }

  private def withNodeSet(specTitle: String)(conAll: StandardDBSystem => NodeSet => NodeSet) = specTitle in {
    withConAndSys(md, _nodes = Seq.empty) { (_, ref) =>
      def node(
        chanId: ChannelId,
        host: String,
        chanConnected: Boolean,
        status: ConnectionStatus): Future[Node] = {
        NettyEmbedder.simpleChannel(chanId, chanConnected).map { chan =>
          val con = new Connection(
            chan, status,
            authenticated = Set.empty,
            authenticating = None,
            signaling = false)

          new Node(
            host,
            Set.empty,
            NodeStatus.Unknown,
            Vector(con),
            Set.empty,
            tags = Map.empty[String, String],
            ProtocolMetadata.Default,
            PingInfo(),
            false,
            System.nanoTime())
        }
      }

      lazy val nsNodes = Future.sequence(connectAllNodes.map {
        case (chanId, name, connected, status) =>
          node(chanId, name, connected, status)
      })

      def ns = nsNodes.map { nodes =>
        new NodeSet(Some("foo"), None, nodes, Set.empty)
      }

      lazy val concurCon = ns.flatMap { nodes =>
        Future(conAll(ref.underlyingActor)(nodes))
      }

      Future.sequence((0 to 10).map(_ => concurCon)).map {
        _.flatMap(_.nodes.flatMap { node =>
          node.connections.map { node.name -> _.status }
        }).toSet
      }.andThen {
        case _ => nsNodes.foreach {
          _.foreach {
            _.connected.foreach(_.channel.close())
          }
        }
      }
    } must contain(exactly[(String, ConnectionStatus)](
      s"$testhost:27017" -> ConnectionStatus.Connected, // already Connected
      // Disconnected to Connected for :27018 as chan is active/connected
      s"$testhost:27018" -> ConnectionStatus.Connected,
      s"$testhost:27019" -> ConnectionStatus.Connecting, // already Connecting
      // connecting `node4` (transition from Disconnected to Connected)
      s"$testhost:27020" -> ConnectionStatus.Connected)).await(0, timeout)
  }
}
