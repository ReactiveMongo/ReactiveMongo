package reactivemongo

import scala.concurrent.Future

import reactivemongo.io.netty.channel.{ ChannelId, DefaultChannelId }

import reactivemongo.core.nodeset.{
  Connection,
  ConnectionStatus,
  Node,
  NodeSet,
  NodeStatus,
  ProtocolMetadata
}

import reactivemongo.core.actors.StandardDBSystem

import _root_.tests.NettyEmbedder

trait ConnectAllTest { _: NodeSetSpec =>
  import reactivemongo.api.tests.connectAll

  private val testhost = java.net.InetAddress.getLocalHost.getHostName

  def connectAllSpec = {
    builder("connect all the nodes without synchronization") { sys =>
      // !! override the nodeset synchronization that takes place normally
      { ns =>
        connectAll(sys, ns)
      }
    }

    builder("connect all the nodes with synchronization") { sys =>
      { ns =>
        ns.synchronized {
          connectAll(sys, ns)
        }
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

  private def builder(specTitle: String)(conAll: StandardDBSystem => NodeSet => NodeSet) = specTitle in {
    withConAndSys(md, _nodes = Seq.empty) { (con, ref) =>
      def node(
        chanId: ChannelId,
        host: String,
        chanConnected: Boolean,
        status: ConnectionStatus): Future[Node] = {
        NettyEmbedder.simpleChannel(chanId, chanConnected).map { chan =>
          val con = Connection(
            chan, status,
            authenticated = Set.empty,
            authenticating = None)

          Node(
            host,
            NodeStatus.Unknown,
            Vector(con),
            Set.empty,
            None,
            ProtocolMetadata.Default)
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
