package reactivemongo.core

import java.net.InetSocketAddress

import akka.actor._
import akka.io.Tcp.SO
import akka.io.{IO, Tcp}
import reactivemongo.api.MongoConnectionOptions
import reactivemongo.api.commands.bson.BSONIsMasterCommand
import reactivemongo.bson.BSONDocument
import reactivemongo.core.actors.{AuthRequest, Close, Closed}
import reactivemongo.core.nodeset._
import reactivemongo.core.protocol.MongoWireVersion

/**
 * Created by sh1ng on 10/05/15.
 */

case class Node(
            address: String,
            options: MongoConnectionOptions
            ) extends Actor with ActorLogging {
  import Node._
  import context.system

  //var connections: Vector[ActorRef] = Vector.empty
  var connections = Map.empty[ActorRef, Int]
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
    case Close => {
      connections.foreach(_._1 ! Close)
      context.become(closing)
    }
    case Node.Connect => {
      awaitingConnections = options.nbChannelsPerNode
      val manager = IO(Tcp)
      for(i <- 0 until options.nbChannelsPerNode)
        yield manager ! Tcp.Connect(new InetSocketAddress(host, port), options = List(SO.KeepAlive(options.keepAlive), SO.TcpNoDelay(options.tcpNoDelay)),
          timeout = options.timeoutDuration)
    }
    case Tcp.Connected(remote, local) => {
      log.info("Connected from {} to {}", local, remote)
      val connection = context.actorOf(Props(classOf[Connection], sender(), local.getPort))
      awaitingConnections -= 1;
      connections = connections + ((connection, local.getPort))
      if(awaitingConnections == 0){
        connections.head._1 ! Node.IsMaster
      }
    }
    case auth: AuthRequest => {
      auth.promise completeWith connections.map(_._1).map(connection => {
        val authRequest = AuthRequest(auth.authenticate)
        connection ! authRequest
        authRequest.future
      }).reduce((a,b) => b)
    }
    case IsMasterInfo(isMaster, ping) => {
      log.debug(isMaster.toString)
      if(pingInfo.lastIsMasterTime < ping.lastIsMasterTime)
        pingInfo = ping
      else
        connections.head._1 ! Node.IsMaster

      isMaster.replicaSet.map(_.hosts).map(context.parent ! Node.DiscoveredNodes(_))
      val protocolMetadata = ProtocolMetadata(MongoWireVersion(isMaster.minWireVersion), MongoWireVersion(isMaster.maxWireVersion), isMaster.maxBsonObjectSize, isMaster.maxMessageSizeBytes, isMaster.maxWriteBatchSize)
      context.parent ! Node.Connected(connections.map(p=> ((p._1, ConnectionState(isMaster.status, isMaster.isMaster, isMaster.isMongos, p._2, false, ping)))), protocolMetadata)
    }
  }

  private def closing: Receive = {
    case Tcp.Connected(remote, local) => {
      log.info("Connected from {} to {} will be closed", local, remote)
      val connection = context.actorOf(Props(classOf[Connection], sender()))
      connections = connections + ((connection, local.getPort))
      connection ! Close
      awaitingConnections -= 1
    }
    case Closed => {
      log.info("Closed connection to node")
      connections = connections - sender()
      if (connections.isEmpty && awaitingConnections == 0)
        context.parent ! Closed
    }
    case _ => log.info("Connection in closing state, all messages are ignored")
  }
}

object Node {
  object Connect
  case class Connected(connections: Iterable[(ActorRef, ConnectionState)], metadata: ProtocolMetadata)
  case class DiscoveredNodes(hosts: Seq[String])
  object PrimaryUnavailable
  object IsMaster
  object Authenticated
  case class IsMasterInfo(response: BSONIsMasterCommand.IsMasterResult, ping: PingInfo)
}

case class ConnectionState(status: NodeStatus, isPrimary: Boolean, isMongos: Boolean,  channel: Int, authenticated: Boolean, ping: PingInfo)
