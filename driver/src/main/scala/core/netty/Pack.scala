package reactivemongo.core.netty

import shaded.netty.channel.{ Channel, EventLoopGroup }

import shaded.netty.channel.nio.NioEventLoopGroup
import shaded.netty.channel.socket.nio.NioSocketChannel

import reactivemongo.util.LazyLogger

/**
 * @param eventLoopGroup the event loop group
 * @param channelClassTag the channel class tag
 */
private[core] final class Pack(
  val eventLoopGroup: () => EventLoopGroup,
  val channelClass: Class[_ <: Channel])

private[core] object Pack {
  private val logger = LazyLogger("reactivemongo.core.netty.Pack")

  def apply(): Pack = kqueue.orElse(epoll).getOrElse(nio)

  private val kqueuePkg = "shaded.io.netty.channel.kqueue"
  private def kqueue: Option[Pack] = try {
    Some(Class.forName(
      s"${kqueuePkg}.KQueueSocketChannel")).map { cls =>
      println(s"--_> $cls")
      val chanClass = cls.asInstanceOf[Class[_ <: Channel]]
      val groupClass = Class.forName(s"${kqueuePkg}.KQueueEventLoopGroup").
        asInstanceOf[Class[_ <: EventLoopGroup]]

      new Pack(() => groupClass.newInstance(), chanClass)
    }
  } catch {
    case cause: Exception =>
      //logger.debug
      println("Cannot use Netty KQueue", cause)
      None
  }

  private val epollPkg = "shaded.io.netty.channel.epoll"
  private def epoll: Option[Pack] = try {
    Some(Class.forName(
      s"${epollPkg}.EpollSocketChannel")).map { cls =>

      val chanClass = cls.asInstanceOf[Class[_ <: Channel]]
      val groupClass = Class.forName(s"${epollPkg}.EpollEventLoopGroup").
        asInstanceOf[Class[_ <: EventLoopGroup]]

      new Pack(() => groupClass.newInstance(), chanClass)
    }
  } catch {
    case cause: Exception =>
      logger.debug("Cannot use Netty EPoll", cause)
      None
  }

  @inline private def nio = new Pack(
    () => new NioEventLoopGroup(), classOf[NioSocketChannel])

}
