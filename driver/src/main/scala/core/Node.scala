package reactivemongo.core

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Actor.Receive
import akka.actor._
import akka.io.Tcp._
import akka.io.{Tcp, IO}
import reactivemongo.bson.BSONDocument
import reactivemongo.core.actors.RequestIds
import reactivemongo.core.nodeset._

/**
 * Created by sh1ng on 10/05/15.
 */

case class Node(
            address: String,
            authenticated: Set[Authenticate],
            var nbOfConnections: Int
            ) extends Actor with ActorLogging {
  import Node._
  import context.system

  val requestIds = new RequestIds

  var connections: List[ActorRef] = List.empty
  var pingInfo: PingInfo = PingInfo()
  var isMongos: Boolean = false
  var protocolMetadata: ProtocolMetadata = null
  var tags: Option[BSONDocument] = None
  var awaitingConnections = 0



  val (host: String, port: Int) = {
    val splitted = address.span(_ != ':')
    splitted._1 -> (try {
      splitted._2.drop(1).toInt
    } catch {
      case _: Throwable => 27017
    })
  }

//    val authenticatedConnections = new RoundRobiner(connected.filter(_.authenticated.forall { auth =>
//      authenticated.exists(_ == auth)
//    }))
  //
  //  def createNeededChannels(receiver: => ActorRef, connectionManager: => ActorRef, upTo: Int)
  //                          (implicit timeout: Timeout): Node = {
  //    import java.net.InetSocketAddress
  //
  //      copy(connections = connections.++(
  //        for( i <- 0 until upTo;
  //          port <- connectionManager.ask(ConnectionManager.AddConnection(new InetSocketAddress(host, port), receiver)).mapTo[Int])
  //          yield Connection(receiver, connectionManager, ConnectionStatus.Connected, Set.empty, None, port)
  //          ))
  //  }

  //  def establishConnections(receiver: ActorRef, builder: ActorRef, upTo: Int): Unit ={
  //
  //  }
  override def receive: Receive = {
    case ConnectAll => {
      awaitingConnections = nbOfConnections
      val manager = IO(Tcp)
      for(i <- 0 until nbOfConnections)
        yield manager ! Connect(new InetSocketAddress(host, port))
    }
    case Connected(remote, local) => {
      val connection = system.actorOf(Props(classOf[Connection], sender()))
      awaitingConnections = awaitingConnections - 1;
      connections = connection +: connections
      if(awaitingConnections == 0)
        context.become(connected)
    }
  }

  private def connected: Receive = {

  }

}

object Node {
  object ConnectAll
  case class Connected(connections: List[ActorRef])
}

case class ConnectionState(isMongos: Boolean, isPrimary: Boolean, channel: Int, authenticated: Boolean, ping: PingInfo)
