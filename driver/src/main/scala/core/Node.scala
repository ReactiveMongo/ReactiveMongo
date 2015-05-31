package reactivemongo.core

import java.net.InetSocketAddress

import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor._
import akka.io.Tcp._
import akka.io.{IO, Tcp}
import akka.pattern.pipe
import reactivemongo.bson.BSONDocument
import reactivemongo.core.commands.IsMaster
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
    case Tcp.Connected(remote, local) => {
      val connection = system.actorOf(Props(classOf[Connection], sender()))
      awaitingConnections = awaitingConnections - 1;
      connections = connection +: connections
      if(awaitingConnections == 0){
        sendIsMaster()
        //context.parent ! Node.Connected(connections)
        //context.become(connected)
      }
    }
    //case IsMasterResult =>
  }

  private def sendIsMaster() = {
    val request = IsMaster().maker
    connections.head ! request
    request.future pipeTo context.parent
  }

  private def connected: Receive = {
    case _ => {

    }
  }

}

object Node {
  object ConnectAll
  case class Connected(connections: List[ActorRef])
  case class DiscoveredNodes(hosts: Seq[String])
  object PrimaryUnavailable
  object IsMater
}

case class ConnectionState(isMongos: Boolean, isPrimary: Boolean, channel: Int, authenticated: Boolean, ping: PingInfo)
