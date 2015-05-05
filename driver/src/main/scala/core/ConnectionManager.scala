package reactivemongo.core

import java.net.InetSocketAddress

import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import akka.io.Tcp._
import akka.io.{IO, Tcp}

import scala.collection.immutable.HashMap
import scala.util._

/**
 * Created by sh1ng on 03/05/15.
 */
class ConnectionManager(handlerClass: Class[_]) extends Actor with ActorLogging {
  //val manager = IO(Tcp)
  var unregisteredHandlers = List[ActorRef]()


  override def receive: Receive = {
    case ConnectionManager.AddConnection => {
      val handler = context.actorOf(Props(handlerClass))
      unregisteredHandlers = handler :: unregisteredHandlers
    }
    case Connected(remote, local) => {
      log.info("connected from {} to {}", local, remote)
      sender() ! Register(unregisteredHandlers.head, keepOpenOnPeerClosed = true)
      unregisteredHandlers = unregisteredHandlers.tail
    }
    case CommandFailed(cmd) => {
      log.error("Unable to establish connection {}", cmd.failureMessage)
    }
  }
}

object ConnectionManager {
  object AddConnection
}

