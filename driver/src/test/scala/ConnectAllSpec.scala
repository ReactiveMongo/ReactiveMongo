import scala.concurrent.Future

import shaded.netty.channel.ChannelFuture

import org.specs2.matcher.MatchResult
import org.specs2.concurrent.{ ExecutionEnv => EE }

import reactivemongo.core.nodeset.{
  Connection,
  ConnectionStatus,
  Node,
  NodeSet,
  NodeStatus,
  ProtocolMetadata
}

import reactivemongo.core.actors.StandardDBSystem

trait ConnectAllSpec { parent: NodeSetSpec =>
  import reactivemongo.api.tests.connectAll

  private val testhost = java.net.InetAddress.
    getLocalHost.getHostName

  def connectAllSpec = {
    (builder("connect all the nodes without synchronization", {
      _.toSet must_== Set(s"$testhost:27018", s"$testhost:27020")
    }) { sys =>
      // !! override the nodeset synchronization that takes place normally
      { ns =>
        connectAll(sys, ns, _)
      }
    })

    (builder("connect all the nodes with synchronization", {
      _.sorted must_== List(s"$testhost:27018", s"$testhost:27020")
    }) { sys =>
      { ns =>
        { f =>
          ns.synchronized {
            connectAll(sys, ns, f)
          }
        }
      }
    })
  }

  // ---

  private def builder(specTitle: String, checkCon: List[String] => MatchResult[Any])(conAll: StandardDBSystem => NodeSet => ((Node, ChannelFuture) => (Node, ChannelFuture)) => NodeSet) = specTitle in { implicit ee: EE =>
    val coned = List.newBuilder[String]

    withConAndSys(md) { (con, ref) =>
      def node(chanId: Int, host: String, chanConnected: Boolean, status: ConnectionStatus): Node =
        NettyEmbedder.withChannel(chanId, chanConnected, _ => ()) { chan =>
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

      val node1 = node(1, s"$testhost:27017",
        true, ConnectionStatus.Connected)

      val node2 = node(2, s"$testhost:27018",
        false, ConnectionStatus.Disconnected)

      val node3 = node(3, s"$testhost:27019",
        false, ConnectionStatus.Connecting)

      val node4 = node(4, s"$testhost:27020",
        false, ConnectionStatus.Disconnected)

      val ns = NodeSet(Some("foo"), None,
        Vector(node1, node2, node3, node4), Set.empty)

      def concurCon = Future(conAll(ref.underlyingActor)(ns) { (n, c) =>
        coned.synchronized {
          coned += n.name
        }

        n -> c
      })

      Future.sequence((0 to 10).map(_ => concurCon)).
        map(_.flatMap(_.nodes).flatMap { node =>
          node.connections.map { node.name -> _.status }
        }.toSet)
    } must beEqualTo(Set[(String, ConnectionStatus)](
      s"$testhost:27017" -> ConnectionStatus.Connected, // already Connected
      // connecting `node2` (transition from Connecting to Connected)
      s"$testhost:27018" -> ConnectionStatus.Connecting,
      s"$testhost:27018" -> ConnectionStatus.Connected,
      s"$testhost:27019" -> ConnectionStatus.Connecting, // already Connecting
      // connecting `node4` (transition from Connecting to Connected)
      s"$testhost:27020" -> ConnectionStatus.Connecting,
      s"$testhost:27020" -> ConnectionStatus.Connected)).await(0, timeout) and checkCon(coned.result())
  }
}
