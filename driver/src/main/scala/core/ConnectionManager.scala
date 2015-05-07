package reactivemongo.core

import java.net.InetSocketAddress

import akka.actor.{ActorRef, Props, ActorLogging, Actor, ActorSystem}
import akka.io.Tcp._
import akka.io.{IO, Tcp}

import scala.util._

/**
 * Created by sh1ng on 03/05/15.
 */
class ConnectionManager() extends Actor with ActorLogging {
  import context.system

  var handler : ActorRef = null
  var responseTo: ActorRef = null

  override def receive: Receive = {
    case ConnectionManager.AddConnection(address, h) => {
      val manager = IO(Tcp)

      handler = h;
      manager ! Connect(address)
      responseTo = sender()
    }
    case Connected(remote, local) => {
      log.info("connected from {} to {}", local, remote)
      sender() ! Register(handler , keepOpenOnPeerClosed = true)
      responseTo ! Success(local.getPort)
    }
    case CommandFailed(conn : Connect) => {
      log.error("Unable to establish connection %s".format(conn.remoteAddress))
      responseTo ! Failure(new Throwable("Unable establish connection to %s".format(conn.remoteAddress)))
    }
  }
}

object ConnectionManager {
  case class AddConnection(address: InetSocketAddress, handler: ActorRef)
}

