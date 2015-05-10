package reactivemongo.core

import akka.actor.Actor.Receive
import akka.actor.{ActorRef, ActorLogging, Actor}
import akka.io.Tcp._

import scala.collection.immutable.HashMap

/**
 * Created by sh1ng on 03/05/15.
 */
class SocketHandler(val connection: ActorRef) extends Actor with ActorLogging{

  //var perdingResponses = new HashMap[Int, ]

  override def receive: Receive = {
    case _  @msg =>
      log.error("Unable to handle message {}", msg)
  }
}


