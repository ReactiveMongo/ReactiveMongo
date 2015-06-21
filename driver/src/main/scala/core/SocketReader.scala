package reactivemongo.core

import java.nio.ByteOrder

import akka.actor.{ActorRef, ActorLogging, Actor}
import akka.io.Tcp.Received
import akka.util.ByteString
import reactivemongo.core.actors.Close
import reactivemongo.core.protocol.{ Response, Reply, MessageHeader}

import scala.annotation.tailrec

/**
 * Created by sh1ng on 03/05/15.
 */
class SocketReader(val connection: ActorRef, port: Int) extends Actor with ActorLogging {
  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN
  var buffer = ByteString.empty

  override def receive: Receive = {
    case Received(data) => {
      log.debug("Received {} bytes", data.size)
      buffer = buffer ++ data
      buffer = process(buffer)
    }
    case Close => context.become(awaitingClosed orElse receive)
    case _  @msg =>
      log.error("Unable to handle message {}", msg)
  }

  private def awaitingClosed: Receive = {
    case akka.io.Tcp.Closed => log.info("socket reader is closed")
  }

  @tailrec
  private def process(data: ByteString): ByteString = {
    log.debug("length {}", data.size)
    if(data.length < 4) {
      return data
    }
    val l = data.iterator.getInt
    log.debug("message length {}", l)
    if(data.length < l) {
      return data
    }
    val splittedResponse = data.splitAt(l)
    val splittedHeader = splittedResponse._1.splitAt(MessageHeader.size)
    val splittedReply = splittedHeader._2.splitAt(Reply.size)
    val header = MessageHeader(splittedHeader._1)
    val reply = Reply(splittedReply._1)
    val response = Response(header, reply, splittedReply._2, port)
    context.parent ! response
    process(splittedResponse._2)
  }
}



