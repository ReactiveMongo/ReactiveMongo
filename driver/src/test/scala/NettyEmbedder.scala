package tests

import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.ExecutionContext.Implicits.global

import reactivemongo.io.netty.channel.{
  Channel,
  ChannelFuture,
  ChannelFutureListener,
  ChannelHandlerContext,
  ChannelId,
  ChannelPromise
}
import reactivemongo.io.netty.channel.embedded.EmbeddedChannel

object NettyEmbedder extends LowPriorityNettyEmbedder {

  sealed trait OnComplete[T] {
    def onComplete(underlying: T, f: () => Unit): Unit
  }

  implicit def futureOnComplete[T]: OnComplete[Future[T]] =
    new OnComplete[Future[T]] {

      def onComplete(underlying: Future[T], f: () => Unit): Unit =
        underlying.onComplete { case _ => f() }
    }

  private[tests] final class EmChannel(
      chanId: ChannelId,
      initiallyActive: Boolean = false)
      extends EmbeddedChannel(chanId, false, false) {

    import java.net.SocketAddress

    private val lock = new Object {}

    // Override the getter to be able the have an initial inactive embedded chan
    @volatile private var active: () => Boolean = () => initiallyActive
    @volatile private var open: () => Boolean = () => true

    @volatile private var toggleActiveCtrl: () => Unit = { () =>
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
        p: ChannelPromise
      ): ChannelFuture = lock.synchronized {
      toggleActiveCtrl()
      super.connect(r, l, p)
    }

    config.setAutoRead(false)
    register() // on init (required for initially inactive to get connected
  }

  private def withChannel[T: OnComplete](
      chanId: ChannelId,
      connected: Boolean,
      beforeWrite: (Channel, Object) => Unit
    )(f: EmbeddedChannel => T
    ): T = {
    object WithChannelHandler
        extends reactivemongo.io.netty.channel.ChannelOutboundHandlerAdapter {

      override def write(
          ctx: ChannelHandlerContext,
          msg: Object,
          promise: ChannelPromise
        ): Unit = {
        beforeWrite(ctx.channel, msg)

        ctx.write(msg, promise)

        ()
      }
    }

    val chan = new EmChannel(chanId, connected)

    chan.pipeline.addLast(WithChannelHandler)

    val ready = Promise[Unit]()

    chan
      .connect(new java.net.InetSocketAddress(27017))
      .addListener(new ChannelFutureListener {

        def operationComplete(op: ChannelFuture) = {
          if (op.isSuccess) ready.success({})
          else ready.failure(op.cause)

          ()
        }
      })

    @annotation.tailrec
    def release(): Unit =
      Option(chan.readOutbound[reactivemongo.io.netty.buffer.ByteBuf]) match {
        case Some(remaining) => {
          remaining.release()
          release()
        }

        case _ => chan.close(); ()
      }

    def close(): Unit = {
      if (chan.finish) release()
      else {
        chan.close()
        ()
      }
    }

    try {
      Await.result(ready.future, tests.Common.timeout)

      val res = f(chan)

      implicitly[OnComplete[T]].onComplete(res, { () => close() })

      res
    } catch {
      case cause: Exception =>
        close()
        throw cause
    }
  }

  def withChannel2[T](
      chanId: ChannelId,
      connected: Boolean
    )(f: EmbeddedChannel => T
    ): T = withChannel(chanId, connected, (_, _) => {})(f)

  def simpleChannel(
      chanId: ChannelId,
      connected: Boolean
    ): Future[EmbeddedChannel] = {
    val chan = new EmChannel(chanId, connected)
    val ready = Promise[EmbeddedChannel]()

    chan
      .connect(new java.net.InetSocketAddress(27017))
      .addListener(new ChannelFutureListener {

        def operationComplete(op: ChannelFuture) = {
          if (op.isSuccess) ready.success(chan)
          else ready.failure(op.cause)

          ()
        }
      })

    ready.future
  }

  def simpleChannel(
      chanId: ChannelId,
      handler: (Channel, Object) => Unit
    ): Future[EmbeddedChannel] = {
    object WithChannelHandler
        extends reactivemongo.io.netty.channel.ChannelOutboundHandlerAdapter {

      override def write(
          ctx: ChannelHandlerContext,
          msg: Object,
          promise: ChannelPromise
        ): Unit = {
        handler(ctx.channel, msg)

        ctx.write(msg, promise)

        ()
      }
    }

    val chan = new EmChannel(chanId, false)

    chan.pipeline.addLast(WithChannelHandler)

    val ready = Promise[EmbeddedChannel]()

    chan
      .connect(new java.net.InetSocketAddress(27017))
      .addListener(new ChannelFutureListener {

        def operationComplete(op: ChannelFuture) = {
          if (op.isSuccess) ready.success(chan)
          else ready.failure(op.cause)

          ()
        }
      })

    ready.future
  }
}

sealed trait LowPriorityNettyEmbedder { _self: NettyEmbedder.type =>

  implicit def defaultOnComplete[T]: OnComplete[T] = new OnComplete[T] {
    def onComplete(underlying: T, f: () => Unit): Unit = f()
  }
}
