import java.net.InetSocketAddress

import scala.util.Try

import scala.concurrent.{ Future, ExecutionContext, Promise }

import shaded.netty.util.Version
import shaded.netty.buffer.Unpooled

import shaded.netty.channel.{
  Channel,
  ChannelFuture,
  ChannelOption,
  ChannelFutureListener,
  ChannelHandlerContext,
  ChannelInitializer,
  ChannelInboundHandlerAdapter
}
import shaded.netty.bootstrap.{ Bootstrap, ServerBootstrap }

import shaded.netty.channel.socket.nio.NioSocketChannel

import reactivemongo.util.LazyLogger

/**
 * Simple TCP proxy using Netty.
 */
final class NettyProxy(
  localAddresses: Seq[InetSocketAddress],
  remoteAddress: InetSocketAddress,
  delay: Option[Long] = None) {

  import NettyProxy.log

  private val beforeProxy: () => Unit = delay.fold(() => {})(d => { () =>
    try {
      Thread.sleep(d)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
  })

  private class RemoteChannelHandler(
    val clientChannel: Channel)
    extends ChannelInboundHandlerAdapter {

    override def channelActive(ctx: ChannelHandlerContext) {
      log.info(s"Remote channel open ${ctx.channel}")
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
      log.warn(s"Remote channel exception caught ${ctx.channel}", cause)
      ctx.channel.close()
      ()
    }

    override def channelInactive(ctx: ChannelHandlerContext) {
      log.info(s"Remote channel closed ${ctx.channel}")
      closeOnFlush(clientChannel)
    }

    override def channelRead(ctx: ChannelHandlerContext, msg: Object) {
      beforeProxy()

      log.debug(s"Read remote message to forward it to client (${clientChannel}): $msg")

      clientChannel.writeAndFlush(msg)
      ()
    }
  }

  private lazy val clientGroup =
    new shaded.netty.channel.nio.NioEventLoopGroup()

  private class ClientChannelHandler(
    clientBootstrap: Bootstrap,
    remoteAddress: InetSocketAddress)
    //clientSocketChannelFactory: ClientSocketChannelFactory)
    extends ChannelInboundHandlerAdapter {

    @volatile private var remoteChannel: Channel = null

    override def channelActive(ctx: ChannelHandlerContext) {
      val clientChannel = ctx.channel

      beforeProxy()
      log.info(s"Client channel open $clientChannel")

      //clientChannel.setReadable(false)

      //val b = clientBootstrap.handler(this)
      val b = clientBootstrap.handler(new RemoteChannelHandler(clientChannel))
      val connectFuture = b.connect(remoteAddress)

      remoteChannel = connectFuture.channel

      /*
      remoteChannel.pipeline().addLast(
        "handler", new RemoteChannelHandler(clientChannel))
       */

      connectFuture.addListener(new ChannelFutureListener {
        def operationComplete(future: ChannelFuture) {
          if (future.isSuccess) {
            log.info(s"Remote channel connect success $remoteChannel")
            //clientChannel.setReadable(true)
          } else {
            log.warn(
              s"Remote channel connect failure $remoteChannel",
              future.cause)

            clientChannel.close()
          }

          ()
        }
      })

      ()
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
      log.warn(s"Client channel exception caught ${ctx.channel}", cause)

      beforeProxy()
      ctx.channel.close()
      ()
    }

    override def channelInactive(ctx: ChannelHandlerContext) {
      log.info(s"Client channel closed ${ctx.channel}")

      if (remoteChannel != null) {
        beforeProxy()
        closeOnFlush(remoteChannel)
      }
    }

    override def channelRead(ctx: ChannelHandlerContext, msg: Object) {
      log.debug(s"Read client message to forward it to remote (${remoteChannel}): $msg")

      if (remoteChannel != null) {
        beforeProxy()
        remoteChannel.writeAndFlush(msg)
      }
      ()
    }
  }

  private val started = new java.util.concurrent.atomic.AtomicBoolean(false)

  @inline def isStarted: Boolean = started.get

  private lazy val serverGroup =
    new shaded.netty.channel.nio.NioEventLoopGroup()

  def start()(implicit ec: ExecutionContext): Future[Unit] = {
    if (started.getAndSet(true)) Future.successful({})
    else {
      lazy val serverBootstrap = new ServerBootstrap().
        option(ChannelOption.SO_REUSEADDR, java.lang.Boolean.TRUE).
        channel(
          classOf[shaded.netty.channel.socket.nio.NioServerSocketChannel]).
        group(serverGroup).
        childOption(ChannelOption.AUTO_READ, java.lang.Boolean.TRUE).
        childHandler(new ChannelInitializer[NioSocketChannel] {
          def initChannel(ch: NioSocketChannel) {
            log.debug(s"Initializing channel $ch")

            val clientBootstrap = new Bootstrap().
              group(clientGroup).
              channel(classOf[NioSocketChannel]).
              option(ChannelOption.AUTO_READ, java.lang.Boolean.TRUE).
              option(
                ChannelOption.CONNECT_TIMEOUT_MILLIS,
                Integer.valueOf(1000))

            ch.pipeline.addLast(new ClientChannelHandler(
              clientBootstrap, remoteAddress)); ()
          }
        })

      log.debug(s"Netty version ${Version.identify}")

      Future.sequence(localAddresses.map { local =>
        val bound = Promise[Channel]()

        log.debug(s"Binding server socket on $local")

        serverBootstrap.bind(local).addListener(new ChannelFutureListener {
          def operationComplete(ch: ChannelFuture) {
            if (ch.isSuccess) {
              log.info(s"Listening on ${ch.channel.localAddress}")

              bound.success(ch.channel)
            } else {
              log.error("Fails to bind server", ch.cause)

              bound.failure(ch.cause)
            }

            ()
          }
        })

        bound.future
      }).map { _ =>
        log.info(s"Remote address: $remoteAddress")
      }

      Future({})
    }
  }

  /**
   * Close the channel after all pending writes are complete.
   */
  private def closeOnFlush(channel: Channel) {
    if (channel.isActive) {
      channel.writeAndFlush(Unpooled.EMPTY_BUFFER).
        addListener(ChannelFutureListener.CLOSE)

    } else if (channel.isRegistered) {
      channel.close
    }
    ()
  }

  def stop() {
    clientGroup.shutdownGracefully()
    serverGroup.shutdownGracefully()

    ()
  }
}

object NettyProxy {
  import scala.concurrent.Await
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  val log = LazyLogger("reactivemongo.test.NettyProxy")

  /**
   * Usage:
   *
   *   test:run localhost:27019 localhost:27017
   *
   *   test:run localhost:27019 localhost:27017 300
   */
  def main(args: Array[String]): Unit = args.toList match {
    case InetAddress(local) :: InetAddress(remote) :: opts => {
      val delay = opts.headOption.flatMap { opt =>
        Try(opt.toLong).toOption
      }

      val proxy = new NettyProxy(Seq(local), remote, delay)

      Await.result(proxy.start(), 5.seconds)
    }

    case _ => log.info(s"Invalid arguments: ${args mkString " "}")
  }

  // ---

  object InetAddress {
    def unapply(repr: String): Option[InetSocketAddress] =
      Option(repr).flatMap {
        _.span(_ != ':') match {
          case (host, p) => try {
            val port = p.drop(1).toInt
            Some(new InetSocketAddress(host, port))
          } catch {
            case e: Throwable =>
              log.error(s"fails to prepare address '$repr'", e)
              None
          }

          case _ =>
            log.error(s"invalid remote address: $repr")
            None
        }
      }
  }
}
