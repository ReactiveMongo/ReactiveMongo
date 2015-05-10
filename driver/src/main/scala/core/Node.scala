package reactivemongo.core

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import akka.io.Tcp._
import akka.io.{Tcp, IO}
import reactivemongo.bson.BSONDocument
import reactivemongo.core.nodeset._

/**
 * Created by sh1ng on 10/05/15.
 */

class Node(
            address: InetSocketAddress,
            authenticated: Set[Authenticated],
            var nbOfConnections: Int
            ) extends Actor with ActorLogging {
  import Node._

  val connectionManager = context.actorOf(Props(classOf[ConnectionManager]))
  var connections: Vector[ActorRef] = Vector.empty
  var pingInfo: PingInfo = PingInfo()
  var isMongos: Boolean = false
  var protocolMetadata: ProtocolMetadata = null
  var tags: Option[BSONDocument] = None

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
    case EstablishConnections => if(connections.length < nbOfConnections) {
      val manager = IO(Tcp)
      manager ! Connect(address)
    } else
      log.warning("All connections have been already established.")
  }

  private def connecting: Receive = {
    case Connected(remote, local) =>
      log.info("Connected from {} to {}", local, remote)
      val connection = context.actorOf(Props(classOf[Connection], sender(), authenticated, None, Node.nextChannel))
      connections = connection +: connections
      if(connections.length == nbOfConnections) context.become(connected)
      else{
        context.unbecome()
        self ! EstablishConnections
      }
    case CommandFailed(_: Connected) => {
      log.error("Unable to establish Connection")
      context stop self
    }
  }

  private def connected: Receive ={
    case _ =>
  }
}

case class ConnectionState(isMongos: Boolean, isPrimary: Boolean, channel: Int, authenticated: Boolean)

object Node{
  object EstablishConnections

  private val _counter = new AtomicInteger(1)
  def nextChannel = _counter.getAndIncrement
}
