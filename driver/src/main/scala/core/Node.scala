package reactivemongo.core

import java.net.InetSocketAddress

import akka.actor._
import akka.event.Logging
import akka.io.{IO, Tcp}
import reactivemongo.api.commands.bson.BSONIsMasterCommand
import reactivemongo.bson.BSONDocument
import reactivemongo.core.actors.{Close, Closed}
import reactivemongo.core.nodeset._
import reactivemongo.core.protocol.MongoWireVersion

/**
 * Created by sh1ng on 10/05/15.
 */

case class Node(
            address: String,
            authenticated: Seq[Authenticate],
            nbOfConnections: Int
            ) extends Actor with ActorLogging {
  import Node._
  import context.system


  var connections: Vector[ActorRef] = Vector.empty
  var pingInfo: PingInfo = PingInfo()
  var isMongos: Boolean = false
  var protocolMetadata: ProtocolMetadata = null
  var tags: Option[BSONDocument] = None
  var awaitingConnections = 0
  var localPort = -1

  val (host: String, port: Int) = {
    val splitted = address.span(_ != ':')
    splitted._1 -> (try {
      splitted._2.drop(1).toInt
    } catch {
      case _: Throwable => 27017
    })
  }

  override def receive: Receive = {
    case Close => {
      connections.foreach(_ ! Close)
      context.become(closing)
    }
    case Node.Connect => {
      awaitingConnections = nbOfConnections
      val manager = IO(Tcp)
      for(i <- 0 until nbOfConnections)
        yield manager ! Tcp.Connect(new InetSocketAddress(host, port))
    }
    case Tcp.Connected(remote, local) => {
      log.info("Connected from {} to {}", local, remote)
      localPort = local.getPort
      val connection = context.actorOf(Props(classOf[Connection], sender(), localPort))
      awaitingConnections -= 1;
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

      val state = ConnectionState(isMaster.status, isMaster.isMaster, isMaster.isMongos, localPort, false, ping)
      val protocolMetadata = ProtocolMetadata(MongoWireVersion(isMaster.minWireVersion), MongoWireVersion(isMaster.maxWireVersion), isMaster.maxBsonObjectSize, isMaster.maxMessageSizeBytes, isMaster.maxWriteBatchSize)
      context.parent ! Node.Connected(connections.map((_, state)), protocolMetadata)
    }
  }

  private def closing: Receive = {
    case Tcp.Connected(remote, local) => {
      log.info("Connected from {} to {} will be closed", local, remote)
      val connection = context.actorOf(Props(classOf[Connection], sender()))
      connections = connection +: connections
      connection ! Close
      awaitingConnections -= 1
    }
    case Closed => {
      log.info("Closed connection to node")
      connections = connections diff List(sender())
      if (connections.isEmpty && awaitingConnections == 0)
        context.parent ! Closed
    }
    case _ => log.info("Connection in closing state, all messages are ignored")
  }
}

object Node {
  object Connect
  case class Connected(connections: Seq[(ActorRef, ConnectionState)], metadata: ProtocolMetadata)
  case class DiscoveredNodes(hosts: Seq[String])
  object PrimaryUnavailable
  object IsMaster
  case class IsMasterInfo(response: BSONIsMasterCommand.IsMasterResult, ping: PingInfo)
}

case class ConnectionState(status: NodeStatus, isPrimary: Boolean, isMongos: Boolean,  channel: Int, authenticated: Boolean, ping: PingInfo)
