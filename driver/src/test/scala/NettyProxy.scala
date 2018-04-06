import java.lang.{ Boolean => JBool }

import java.net.InetSocketAddress

import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

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

import shaded.netty.channel.nio.NioEventLoopGroup
import shaded.netty.channel.socket.nio.{
  NioSocketChannel,
  NioServerSocketChannel
}

import reactivemongo.util.LazyLogger

final class NettyProxy(
  localAddresses: Seq[InetSocketAddress],
  remoteAddress: InetSocketAddress,
  delay: Option[Long] = None) {

  import NettyProxy.log

  case class State(
    groups: Seq[NioEventLoopGroup],
    channels: Seq[Channel])

  private val starting = new java.util.concurrent.atomic.AtomicBoolean(false)
  private val state = Promise[State]()

  @inline def isStarted: Boolean = state.future.value.exists(_.isSuccess)

  def start()(implicit ec: ExecutionContext): Future[State] = {
    if (starting.getAndSet(true)) {
      state.future
    } else {
      log.debug(s"Netty version ${Version.identify}")

      // Configure the bootstrap.
      val bossGroup = new NioEventLoopGroup(1)
      val workerGroup = new NioEventLoopGroup()

      lazy val serverBootstrap = new ServerBootstrap().
        option(ChannelOption.SO_REUSEADDR, JBool.TRUE).
        channel(classOf[NioServerSocketChannel]).
        group(bossGroup, workerGroup).
        childOption(ChannelOption.AUTO_READ, JBool.FALSE).
        childHandler(new ChannelInitializer[NioSocketChannel] {
          def initChannel(ch: NioSocketChannel): Unit = {
            ch.pipeline().addLast(
              new NettyProxyFrontendHandler(remoteAddress, delay))

            ()
          }
        })

      def bind = Future.sequence(localAddresses.map { localAddr =>
        val bound = Promise[Channel]()

        log.debug(s"Binding server socket on $localAddr")

        serverBootstrap.bind(localAddr).addListener(new ChannelFutureListener {
          def operationComplete(ch: ChannelFuture): Unit = {
            if (ch.isSuccess) {
              log.info(s"Listen on ${ch.channel.localAddress}")

              bound.success(ch.channel)
            } else {
              log.error("Fails to bind server", ch.cause)

              bound.failure(ch.cause)
            }

            ()
          }
        })

        bound.future
      }).andThen {
        case Failure(cause) =>
          log.warn("Fails to bind Netty proxy", cause)

          try {
            bossGroup.shutdownGracefully()
          } catch {
            case NonFatal(err) => log.warn("Fails to shutdown bossGroup", err)
          }

          workerGroup.shutdownGracefully()
      }.map { channels =>
        State(
          groups = Seq(bossGroup, workerGroup),
          channels = channels)
      }

      state.completeWith(bind).future
    }
  }

  def stop(): Unit = state.future.value.foreach {
    case Success(st) => {
      st.groups.foreach { g =>
        try {
          g.shutdownGracefully()
        } catch {
          case NonFatal(err) => log.warn("Fail to shutdown group", err)
        }
      }

      st.channels.foreach { ch =>
        try {
          ch.closeFuture().sync()
        } catch {
          case NonFatal(err) => log.warn("Fail to close channel", err)
        }
      }
    }

    case _ => ()
  }
}

final class NettyProxyFrontendHandler(
  remoteAddress: InetSocketAddress,
  delay: Option[Long]) extends ChannelInboundHandlerAdapter {

  @volatile private var outboundChannel: Channel = null

  private val beforeProxy: () => Unit = delay.fold(() => {})(d => { () =>
    try {
      Thread.sleep(d)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
  })

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    val inboundChannel: Channel = ctx.channel()

    // Start the connection attempt.
    val b = new Bootstrap()

    b.group(inboundChannel.eventLoop).
      channel(ctx.channel().getClass).
      handler(new NettyProxyBackendHandler(this, beforeProxy, inboundChannel)).
      option(ChannelOption.AUTO_READ, JBool.FALSE)

    val f = b.connect(remoteAddress)

    outboundChannel = f.channel()

    f.addListener(new ChannelFutureListener() {
      def operationComplete(future: ChannelFuture): Unit = {
        if (future.isSuccess) {
          // connection complete start to read first data
          inboundChannel.read()
        } else {
          // Close the connection if the connection attempt has failed.
          inboundChannel.close()
        }

        ()
      }
    })

    ()
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    if (outboundChannel != null && outboundChannel.isActive()) {
      beforeProxy()

      NettyProxy.log.debug(s"Read in proxy frontend (${outboundChannel}): $msg")

      outboundChannel.writeAndFlush(msg).
        addListener(new ChannelFutureListener() {
          def operationComplete(future: ChannelFuture) {
            if (future.isSuccess()) {
              // was able to flush out data, start to read the next chunk
              ctx.channel().read()
            } else {
              future.channel().close()
            }

            ()
          }
        })
    }

    ()
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit =
    if (outboundChannel != null) {
      closeOnFlush(outboundChannel)
    }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    NettyProxy.log.warn("Error in proxy frontend", cause)
    closeOnFlush(ctx.channel())
  }

  def closeOnFlush(ch: Channel): Unit = {
    if (ch.isActive()) {
      ch.writeAndFlush(Unpooled.EMPTY_BUFFER).
        addListener(ChannelFutureListener.CLOSE)
    }

    ()
  }
}

final class NettyProxyBackendHandler(
  frontend: NettyProxyFrontendHandler,
  beforeProxy: () => Unit,
  inboundChannel: Channel) extends ChannelInboundHandlerAdapter {

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    ctx.read(); ()
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    NettyProxy.log.debug(s"Read in backend proxy (${inboundChannel}): $msg")

    beforeProxy()

    inboundChannel.writeAndFlush(msg).addListener(new ChannelFutureListener() {
      def operationComplete(future: ChannelFuture): Unit = {
        if (future.isSuccess()) {
          ctx.channel().read()
        } else {
          future.channel().close()
        }

        ()
      }
    })

    ()
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit =
    frontend.closeOnFlush(inboundChannel)

  override def exceptionCaught(
    ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    NettyProxy.log.warn("Error in proxy backend", cause)
    frontend.closeOnFlush(ctx.channel())
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

      ()
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
