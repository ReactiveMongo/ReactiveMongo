import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import shaded.netty.channel.{
  Channel,
  ChannelId,
  ChannelFuture,
  ChannelHandler,
  ChannelPromise,
  ChannelHandlerContext
}
import shaded.netty.channel.embedded.EmbeddedChannel

object NettyEmbedder extends LowPriorityNettyEmbedder {
  sealed trait OnComplete[T] {
    def onComplete(underlying: T, f: () => Unit): Unit
  }

  implicit def futureOnComplete[T]: OnComplete[Future[T]] =
    new OnComplete[Future[T]] {
      def onComplete(underlying: Future[T], f: () => Unit): Unit =
        underlying.onComplete {
          case _ => f()
        }
    }

  private final class EmChannel(
    chanId: ChannelId,
    initiallyActive: Boolean = false)
    extends EmbeddedChannel(chanId) {

    import java.net.SocketAddress

    private val lock = new Object {}

    // Override the getter to be able the have an initial inactive embedded chan
    private var active: () => Boolean = () => initiallyActive
    private var open: () => Boolean = () => true

    private var toggleActiveCtrl: () => Unit = { () =>
      // Restore the default getter
      active = () => super.isActive
      open = () => super.isOpen

      toggleActiveCtrl = () => {}
    }

    override def isOpen(): Boolean = lock.synchronized { open() }

    override def isActive(): Boolean = lock.synchronized { active() }

    override def connect(r: SocketAddress): ChannelFuture = lock.synchronized {
      toggleActiveCtrl()
      super.connect(r)
    }

    override def connect(r: SocketAddress, p: ChannelPromise): ChannelFuture =
      lock.synchronized {
        toggleActiveCtrl()
        super.connect(r, p)
      }

    override def connect(r: SocketAddress, l: SocketAddress): ChannelFuture =
      lock.synchronized {
        toggleActiveCtrl()
        super.connect(r, l)
      }

    override def connect(
      r: SocketAddress,
      l: SocketAddress,
      p: ChannelPromise): ChannelFuture = lock.synchronized {
      toggleActiveCtrl()
      super.connect(r, l, p)
    }

    register() // on init (required for initially inactive to get connected
  }

  private def withChannel[T: OnComplete](
    chanId: ChannelId,
    connected: Boolean,
    beforeWrite: (Channel, Object) => Unit)(f: EmbeddedChannel => T): T = {
    object WithChannelHandler
      extends shaded.netty.channel.ChannelOutboundHandlerAdapter {

      override def write(
        ctx: ChannelHandlerContext,
        msg: Object,
        promise: ChannelPromise): Unit = {
        beforeWrite(ctx.channel, msg)

        ctx.write(msg, promise)
      }
    }

    val chan = new EmChannel(chanId, connected)

    chan.pipeline.addLast(WithChannelHandler)

    try {
      val res = f(chan)

      implicitly[OnComplete[T]].onComplete(res, { () =>
        val log = org.apache.logging.log4j.LogManager.
          getLogger("shaded.netty.channel.AbstractChannelHandlerContext").
          asInstanceOf[org.apache.logging.log4j.core.Logger]

        val level = log.getLevel
        log.setLevel(org.apache.logging.log4j.Level.ERROR)

        chan.close()

        log.setLevel(level)
      })

      res
    } catch {
      case cause: Exception =>
        chan.close()
        throw cause
    }
  }

  def withChannel1[T](chanId: ChannelId, handler: (Channel, Object) => Unit)(f: EmbeddedChannel => T): T = withChannel(chanId, false, handler)(f)

  def withChannel2[T](chanId: ChannelId, connected: Boolean)(f: EmbeddedChannel => T): T = withChannel(chanId, connected, (_, _) => {})(f)
}

sealed trait LowPriorityNettyEmbedder { _: NettyEmbedder.type =>
  implicit def defaultOnComplete[T]: OnComplete[T] = new OnComplete[T] {
    def onComplete(underlying: T, f: () => Unit): Unit = f()
  }
}
