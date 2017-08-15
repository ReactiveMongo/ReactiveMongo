import java.net.InetSocketAddress
import java.util.concurrent.Executors

import shaded.netty.bootstrap.{ ClientBootstrap, ServerBootstrap }

import shaded.netty.util.Version
import shaded.netty.logging.{ InternalLoggerFactory, Slf4JLoggerFactory }
import shaded.netty.buffer.ChannelBuffers

import shaded.netty.channel.{
  Channel,
  ChannelFuture,
  ChannelFutureListener,
  ChannelHandlerContext,
  ChannelPipeline,
  ChannelPipelineFactory,
  ChannelStateEvent,
  Channels,
  ExceptionEvent,
  MessageEvent,
  SimpleChannelUpstreamHandler
}
import shaded.netty.channel.socket.nio.{
  NioClientSocketChannelFactory,
  NioServerSocketChannelFactory
}
import shaded.netty.channel.socket.ClientSocketChannelFactory

import reactivemongo.util.LazyLogger

/**
 * Simple TCP proxy using Netty.
 */
final class NettyProxy(
  localAddresses: Seq[InetSocketAddress],
  remoteAddress: InetSocketAddress,
  delay: Option[Long] = None) {

  private val log = LazyLogger("reactivemongo.test.NettyProxy")

  private val beforeProxy: () => Unit = delay.fold(() => {})(d => { () =>
    try {
      Thread.sleep(d)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
  })

  private class RemoteChannelHandler(
    val clientChannel: Channel)
    extends SimpleChannelUpstreamHandler {

    override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.info(s"remote channel open ${e.getChannel}")
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      log.warn(s"remote channel exception caught ${e.getChannel}", e.getCause)
      e.getChannel.close()
      ()
    }

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.info(s"remote channel closed ${e.getChannel}")
      closeOnFlush(clientChannel)
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      beforeProxy()
      clientChannel.write(e.getMessage)
      ()
    }
  }

  private class ClientChannelHandler(
    remoteAddress: InetSocketAddress,
    clientSocketChannelFactory: ClientSocketChannelFactory)
    extends SimpleChannelUpstreamHandler {

    @volatile
    private var remoteChannel: Channel = null

    override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      val clientChannel = e.getChannel

      beforeProxy()
      log.info(s"Client channel open $clientChannel")

      clientChannel.setReadable(false)

      val clientBootstrap = new ClientBootstrap(
        clientSocketChannelFactory)
      clientBootstrap.setOption("connectTimeoutMillis", 1000)
      clientBootstrap.getPipeline.addLast(
        "handler",
        new RemoteChannelHandler(clientChannel))

      val connectFuture = clientBootstrap.connect(remoteAddress)

      remoteChannel = connectFuture.getChannel
      connectFuture.addListener(new ChannelFutureListener {
        override def operationComplete(future: ChannelFuture) {
          if (future.isSuccess) {
            log.info(s"remote channel connect success $remoteChannel")
            clientChannel.setReadable(true)
          } else {
            log.info(s"remote channel connect failure $remoteChannel")
            clientChannel.close()
          }
          ()
        }
      })
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      log.info(s"Client channel exception caught ${e.getChannel}", e.getCause)

      beforeProxy()
      e.getChannel.close
      ()
    }

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.info(s"Client channel closed ${e.getChannel}")

      if (remoteChannel != null) {
        beforeProxy()
        closeOnFlush(remoteChannel)
      }
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      if (remoteChannel != null) {
        beforeProxy()
        remoteChannel.write(e.getMessage)
      }
      ()
    }
  }

  private class ProxyPipelineFactory(
    val remoteAddress: InetSocketAddress,
    val clientSocketChannelFactory: ClientSocketChannelFactory)
    extends ChannelPipelineFactory {

    override def getPipeline: ChannelPipeline =
      Channels.pipeline(new ClientChannelHandler(
        remoteAddress,
        clientSocketChannelFactory))

  }

  private val started = new java.util.concurrent.atomic.AtomicBoolean(false)
  private lazy val executor = Executors.newCachedThreadPool

  private lazy val srvChannelFactory =
    new NioServerSocketChannelFactory(executor, executor)

  private lazy val clientSocketChannelFactory =
    new NioClientSocketChannelFactory(executor, executor)

  def start(): Unit = {
    if (started.getAndSet(true)) {}
    else {
      InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory)

      val serverBootstrap = new ServerBootstrap(srvChannelFactory)

      serverBootstrap.setPipelineFactory(new ProxyPipelineFactory(
        remoteAddress, clientSocketChannelFactory))

      serverBootstrap.setOption("reuseAddress", true)

      log.info(s"Netty version ${Version.ID}")

      localAddresses.map(serverBootstrap.bind(_)).foreach { serverChannel =>
        log.info(s"Listening on ${serverChannel.getLocalAddress}")
      }

      log.info(s"Remote address: $remoteAddress")
    }
  }

  /**
   * Close the channel after all pending writes are complete.
   */
  private def closeOnFlush(channel: Channel) {
    if (channel.isConnected) {
      channel.write(ChannelBuffers.EMPTY_BUFFER).
        addListener(ChannelFutureListener.CLOSE)
    } else {
      channel.close
    }
    ()
  }

  def stop(): Unit = if (started.get) {
    try {
      srvChannelFactory.releaseExternalResources()
    } catch {
      case e: Throwable => log.warn("fails to stop the server factory", e)
    }

    try {
      clientSocketChannelFactory.releaseExternalResources()
    } catch {
      case e: Throwable => log.warn("fails to stop client socket", e)
    }
  }
}
