package reactivemongo.core.netty

import scala.util.control.NonFatal

import reactivemongo.io.netty.channel.{ Channel, EventLoopGroup }

import reactivemongo.io.netty.channel.nio.NioEventLoopGroup
import reactivemongo.io.netty.channel.socket.nio.NioSocketChannel

import reactivemongo.util.LazyLogger

/**
 * @param eventLoopGroup the event loop group
 * @param channelClassTag the channel class tag
 */
private[core] final class Pack(
    val eventLoopGroup: () => EventLoopGroup,
    val channelClass: Class[_ <: Channel]) {

  override def equals(that: Any): Boolean = that match {
    case other: Pack =>
      other.channelClass.getName == channelClass.getName

    case _ => false
  }

  override def hashCode: Int = channelClass.getName.hashCode

  override def toString = s"NettyPack(${channelClass.getName})"
}

private[core] object Pack {

  private val shaded: Boolean =
    try {
      // Type alias but no class if not shaded
      Class.forName("reactivemongo.io.netty.channel.Channel")

      true
    } catch {
      case NonFatal(_) => false
    }

  private val logger = LazyLogger("reactivemongo.core.netty.Pack")

  def apply(): Pack = {
    val pack = kqueue.orElse(epoll).getOrElse(nio)

    logger.info(s"Instantiated ${pack.getClass.getName}")

    pack
  }

  private val kqueuePkg: String = {
    if (shaded) "reactivemongo.io.netty.channel.kqueue"
    else "io.netty.channel.kqueue"
  }

  @SuppressWarnings(Array("AsInstanceOf"))
  private[core] def kqueue: Option[Pack] =
    try {
      Some(Class.forName(s"${kqueuePkg}.KQueueSocketChannel")).map { cls =>
        val chanClass = cls.asInstanceOf[Class[_ <: Channel]]
        val groupClass = Class
          .forName(s"${kqueuePkg}.KQueueEventLoopGroup")
          .asInstanceOf[Class[_ <: EventLoopGroup]]

        val pack = new Pack(
          () => groupClass.getDeclaredConstructor().newInstance(),
          chanClass
        )

        logger.info(s"Netty KQueue successfully loaded (shaded: $shaded)")

        pack
      }
    } catch {
      case NonFatal(cause) =>
        logger.debug(s"Cannot use Netty KQueue (shaded: $shaded)", cause)
        None
    }

  private val epollPkg: String = {
    if (shaded) "reactivemongo.io.netty.channel.epoll"
    else "io.netty.channel.epoll"
  }

  @SuppressWarnings(Array("AsInstanceOf"))
  private[core] def epoll: Option[Pack] =
    try {
      Some(Class.forName(s"${epollPkg}.EpollSocketChannel")).map { cls =>
        val chanClass = cls.asInstanceOf[Class[_ <: Channel]]
        val groupClass = Class
          .forName(s"${epollPkg}.EpollEventLoopGroup")
          .asInstanceOf[Class[_ <: EventLoopGroup]]

        val pack = new Pack(
          () => groupClass.getDeclaredConstructor().newInstance(),
          chanClass
        )

        logger.info(s"Netty EPoll successfully loaded (shaded: $shaded)")

        pack
      }
    } catch {
      case NonFatal(cause) =>
        logger.debug(s"Cannot use Netty EPoll (shaded: $shaded)", cause)
        None
    }

  @inline private[core] def nio =
    new Pack(() => new NioEventLoopGroup(), classOf[NioSocketChannel])

}
