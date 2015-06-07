package core

import akka.actor.{ActorLogging, ActorRef, Actor}
import akka.io.Tcp
import akka.util.ByteString

import scala.collection.immutable.Queue

/**
 * Created by sh1ng on 26/05/15.
 */
class SocketWriter(connection: ActorRef) extends Actor with ActorLogging {
  var buffer = Queue.empty[ByteString]

  override def receive: Receive = {
    case data : ByteString => {
      log.debug("Write {} bytes to socket", data.size)
      connection ! Tcp.Write(data, SocketWriter.Ack)
      context.become(buffering)
    }
  }

  def buffering: Receive = {
    case data : ByteString => {
      buffer = buffer.enqueue[ByteString](data)
      log.debug("Write {} bytes to buffer", data.size)
    }
    case SocketWriter.Ack => buffer.dequeueOption match {
        case Some((head, tail)) => {
          connection ! Tcp.Write(head, SocketWriter.Ack)
          buffer = tail
        }
        case None => context.unbecome()
    }
  }
}

object SocketWriter{
  object Ack extends Tcp.Event
}
