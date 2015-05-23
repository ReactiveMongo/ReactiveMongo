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

  override def receive: Receive = {
    case _ =>
  }
}

object ConnectionManager {

  case class AddConnection(address: InetSocketAddress, handler: ActorRef)
}

