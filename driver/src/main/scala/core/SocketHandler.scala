package reactivemongo.core

import akka.actor.Actor.Receive
import akka.actor.{ActorRef, ActorLogging, Actor}
import akka.io.Tcp._

import scala.collection.immutable.HashMap

/**
 * Created by sh1ng on 03/05/15.
 */
class SocketHandler() extends Actor with ActorLogging{
  import SocketHandler._

  var connection: ActorRef = null


  //var perdingResponses = new HashMap[Int, ]

  override def receive: Receive = {
    case RegisterConnection(conn) =>
      connection = conn
      context.become(connected)
    case _  @msg =>
      log.error("Unable to handle message {}", msg)
  }

  def connected: Receive ={
    case Write(msg, nak) =>

  }
}

object SocketHandler {
  case class RegisterConnection(connection: ActorRef)
}


