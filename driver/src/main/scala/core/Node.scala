package reactivemongo.core

import java.net.InetSocketAddress

import reactivemongo.api.commands.bson.BSONIsMasterCommand

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
      }
    }
    case Node.IsMater => sendIsMaster()
    case IsMasterInfo(isMaster, ping) => {
      log.debug(isMaster.toString)
      if(pingInfo.lastIsMasterTime < ping.lastIsMasterTime)
        pingInfo = ping
      else
        self ! Node.IsMater

      isMaster.replicaSet match {
        case Some(replica) => {
          context.parent ! Node.DiscoveredNodes(replica.hosts)
        }
      }
      val state = ConnectionState(isMaster.isMongos, isMaster.isMaster, -1, false, ping)
      context.parent ! Node.Connected(connections.map((_, state)))
    }
  }

  private def sendIsMaster() = {
    val initialInfo = PingInfo(Int.MaxValue, System.currentTimeMillis())
    val request = IsMaster().maker
    connections.head ! request
    request.future.map(response => {
      import reactivemongo.api.BSONSerializationPack
      import reactivemongo.api.commands.bson.BSONIsMasterCommandImplicits
      import reactivemongo.api.commands.Command
      val isMaster = Command.deserialize(BSONSerializationPack, response)(BSONIsMasterCommandImplicits.IsMasterResultReader)
      IsMasterInfo(isMaster, initialInfo.copy(ping = System.currentTimeMillis() - initialInfo.lastIsMasterTime ))
    }) pipeTo self
  }

  private def connected: Receive = {
    case _ => {

    }
  }

}

object Node {
  object ConnectAll
  case class Connected(connections: List[(ActorRef, ConnectionState)])
  case class DiscoveredNodes(hosts: Seq[String])
  object PrimaryUnavailable
  object IsMater
  case class IsMasterInfo(response: BSONIsMasterCommand.IsMasterResult, ping: PingInfo)
}

case class ConnectionState(isMongos: Boolean, isPrimary: Boolean, channel: Int, authenticated: Boolean, ping: PingInfo)
