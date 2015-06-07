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
            authenticated: Seq[Authenticate],
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

  override def receive: Receive = {
    case Node.Connect => {
      awaitingConnections = nbOfConnections
      val manager = IO(Tcp)
      for(i <- 0 until nbOfConnections)
        yield manager ! Tcp.Connect(new InetSocketAddress(host, port))
    }
    case Tcp.Connected(remote, local) => {
      log.info("Connected from {} to {}", local, remote)
      val connection = context.actorOf(Props(classOf[Connection], sender()))
      awaitingConnections = awaitingConnections - 1;
      connections = connection +: connections
      if(awaitingConnections == 0){
        connections.head ! Node.IsMaster
      }
    }
    case IsMasterInfo(isMaster, ping) => {
      log.debug(isMaster.toString)
      if(pingInfo.lastIsMasterTime < ping.lastIsMasterTime)
        pingInfo = ping
      else
        connections.head ! Node.IsMaster

      isMaster.replicaSet.map(_.hosts).map(context.parent ! Node.DiscoveredNodes(_))

      isMaster.status
      val state = ConnectionState(isMaster.status, -1, false, ping)
      context.parent ! Node.Connected(connections.map((_, state)))
    }
  }
//
//  private def sendIsMaster() = {
//    val initialInfo = PingInfo(Int.MaxValue, System.currentTimeMillis())
//    val request = IsMaster().maker
//    log.debug("send IsMaster to {}", connections.head)
//    request.future.map(response => {
//      import reactivemongo.api.BSONSerializationPack
//      import reactivemongo.api.commands.bson.BSONIsMasterCommandImplicits
//      import reactivemongo.api.commands.Command
//      try{
//        val isMaster = Command.deserialize(BSONSerializationPack, response)(BSONIsMasterCommandImplicits.IsMasterResultReader)
//        IsMasterInfo(isMaster, initialInfo.copy(ping = System.currentTimeMillis() - initialInfo.lastIsMasterTime ))
//      } catch{
//        case th: Throwable => log.error(th, "")
//        case e: Exception => log.error(e.toString)
//      }
//    }) pipeTo self
//
//    connections.head ! request
//  }

}

object Node {
  object Connect
  case class Connected(connections: List[(ActorRef, ConnectionState)])
  case class DiscoveredNodes(hosts: Seq[String])
  object PrimaryUnavailable
  object IsMaster
  case class IsMasterInfo(response: BSONIsMasterCommand.IsMasterResult, ping: PingInfo)
}

case class ConnectionState(status: NodeStatus, channel: Int, authenticated: Boolean, ping: PingInfo)
