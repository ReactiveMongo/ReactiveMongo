package reactivemongo.core

import java.nio.ByteOrder

import akka.actor.{ActorRef, ActorLogging, Actor}
import akka.io.Tcp._
import akka.util.{ByteString, ByteStringBuilder}

import scala.collection.immutable.{Queue, HashMap}

/**
 * Created by sh1ng on 03/05/15.
 */
class SocketReader(val connection: ActorRef) extends Actor with ActorLogging {
  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN
  var buffer = ByteString.empty
  //var perdingResponses = new HashMap[Int, ]

  override def receive: Receive = {
    case Received(data) => {
      buffer = buffer ++ data
      buffer = process(buffer)
    }
    case _  @msg =>
      log.error("Unable to handle message {}", msg)
  }

  def process(data: ByteString): ByteString ={
    if(data.length < 4) data
    val l = data.iterator.getInt
    if(data.length < l) data
    val splitted = data.splitAt(l)
    context.parent ! splitted._1.drop(4)
    process(splitted._2)
  }
}



