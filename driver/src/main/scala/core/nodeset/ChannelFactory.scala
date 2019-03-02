package reactivemongo.core.nodeset // TODO: Move to `netty` package

import java.lang.{ Boolean => JBool }

import scala.concurrent.Promise

import reactivemongo.io.netty.util.concurrent.{ Future, GenericFutureListener }

import reactivemongo.io.netty.bootstrap.Bootstrap

import reactivemongo.io.netty.channel.{
  Channel,
  ChannelFuture,
  ChannelFutureListener,
  ChannelOption,
  EventLoopGroup
}, ChannelOption.{ CONNECT_TIMEOUT_MILLIS, SO_KEEPALIVE, TCP_NODELAY }

import reactivemongo.io.netty.channel.ChannelInitializer

import akka.actor.ActorRef

import reactivemongo.util.LazyLogger

import reactivemongo.core.protocol.{
  MongoHandler,
  RequestEncoder,
  ResponseFrameDecoder,
  ResponseDecoder
}
import reactivemongo.core.actors.ChannelDisconnected

import reactivemongo.api.MongoConnectionOptions

/**
 * @param supervisor the name of the driver supervisor
 * @param connection the name of the connection pool
 */
private[reactivemongo] final class ChannelFactory(
  supervisor: String,
  connection: String,
  options: MongoConnectionOptions) extends ChannelInitializer[Channel] {

  private val pack = reactivemongo.core.netty.Pack()
  private val parentGroup: EventLoopGroup = pack.eventLoopGroup()
  private val childGroup: EventLoopGroup = pack.eventLoopGroup()

  private val logger = LazyLogger("reactivemongo.core.nodeset.ChannelFactory")

  private lazy val channelFactory = new Bootstrap().
    group(parentGroup).
    channel(pack.channelClass).
    option(TCP_NODELAY, JBool.valueOf(options.tcpNoDelay)).
    option(SO_KEEPALIVE, JBool.valueOf(options.keepAlive)).
    option(CONNECT_TIMEOUT_MILLIS, Integer.valueOf(options.connectTimeoutMS)).
    handler(this)

  private[reactivemongo] def create(
    host: String = "localhost",
    port: Int = 27017,
    receiver: ActorRef): Channel = {
    val resolution = channelFactory.connect(host, port).addListener(
      new ChannelFutureListener {
        def operationComplete(op: ChannelFuture) {
          if (!op.isSuccess) {
            val chanId = op.channel.id

            debug(
              s"Connection to ${host}:${port} refused for channel #${chanId}",
              op.cause)

            receiver ! ChannelDisconnected(chanId)
          }
        }
      })

    val channel = resolution.channel

    debug(s"Created new channel #${channel.id} to ${host}:${port} (registered = ${channel.isRegistered})")

    if (channel.isRegistered) {
      initChannel(channel, host, port, receiver)
    } else {
      // Set state as attributes so available for the coming init
      channel.attr(ChannelFactory.hostKey).set(host)
      channel.attr(ChannelFactory.portKey).set(port)
      channel.attr(ChannelFactory.actorRefKey).set(receiver)
    }

    channel
  }

  def initChannel(channel: Channel): Unit = {
    val host = channel.attr(ChannelFactory.hostKey).get

    if (host == null) {
      info("Skip channel init as host is null")
    } else {
      val port = channel.attr(ChannelFactory.portKey).get
      val receiver = channel.attr(ChannelFactory.actorRefKey).get

      initChannel(channel, host, port, receiver)
    }
  }

  private[reactivemongo] def initChannel(
    channel: Channel,
    host: String, port: Int,
    receiver: ActorRef): Unit = {
    debug(s"Initializing channel ${channel.id} to ${host}:${port} ($receiver)")

    val pipeline = channel.pipeline

    if (options.sslEnabled) {
      val sslEng = reactivemongo.core.SSL.
        createEngine(sslContext, host, port)

      val sslHandler =
        new reactivemongo.io.netty.handler.ssl.SslHandler(sslEng, false /* TLS */ )

      pipeline.addFirst("ssl", sslHandler)
    }

    val mongoHandler = new MongoHandler(
      supervisor, connection, receiver, options.maxIdleTimeMS.toLong)

    pipeline.addLast(
      new ResponseFrameDecoder(), new ResponseDecoder(),
      new RequestEncoder(), mongoHandler)

    trace(s"Netty channel configuration:\n- connectTimeoutMS: ${options.connectTimeoutMS}\n- maxIdleTimeMS: ${options.maxIdleTimeMS}ms\n- tcpNoDelay: ${options.tcpNoDelay}\n- keepAlive: ${options.keepAlive}\n- sslEnabled: ${options.sslEnabled}\n- keyStore: ${options.keyStore}")
  }

  private def keyStore: Option[MongoConnectionOptions.KeyStore] =
    options.keyStore.orElse {
      sys.props.get("javax.net.ssl.keyStore").map { path =>
        MongoConnectionOptions.KeyStore(
          resource = new java.io.File(path).toURI,
          storeType = sys.props.getOrElse("javax.net.ssl.keyStoreType", "JKS"),
          password = sys.props.get(
            "javax.net.ssl.keyStorePassword").map(_.toCharArray))

      }
    }

  private def sslContext = {
    import java.security.KeyStore
    import javax.net.ssl.{ KeyManagerFactory, TrustManager }

    val keyManagers = keyStore.map { settings =>
      val password = settings.password.getOrElse(Array.empty[Char])

      val ks = reactivemongo.util.withContent(settings.resource) { storeIn =>
        val res = KeyStore.getInstance(settings.storeType)

        res.load(storeIn, password)

        res
      }

      val kmf = {
        val res = KeyManagerFactory.getInstance(
          KeyManagerFactory.getDefaultAlgorithm)

        res.init(ks, password)
        res
      }

      kmf.getKeyManagers
    }

    val sslCtx = {
      val res = javax.net.ssl.SSLContext.getInstance("SSL")

      val tm: Array[TrustManager] =
        if (options.sslAllowsInvalidCert) Array(TrustAny) else null

      val rand = new scala.util.Random(System.identityHashCode(tm))
      val seed = Array.ofDim[Byte](128)
      rand.nextBytes(seed)

      res.init(keyManagers.orNull, tm, new java.security.SecureRandom(seed))

      res
    }

    sslCtx
  }

  private[reactivemongo] def release(callback: Promise[Unit]): Unit = {
    parentGroup.shutdownGracefully().
      addListener(new GenericFutureListener[Future[Any]] {
        def operationComplete(f: Future[Any]): Unit = {
          childGroup.shutdownGracefully().
            addListener(new GenericFutureListener[Future[Any]] {
              def operationComplete(f: Future[Any]): Unit = {
                callback.success({}); ()
              }
            })

          ()
        }
      })

    ()
  }

  @inline private def debug(msg: => String) =
    logger.debug(s"[$supervisor/$connection] ${msg}")

  @inline private def debug(msg: => String, cause: Throwable) =
    logger.debug(s"[$supervisor/$connection] ${msg}", cause)

  @inline private def trace(msg: => String) =
    logger.trace(s"[$supervisor/$connection] ${msg}")

  @inline private def info(msg: => String) =
    logger.info(s"[$supervisor/$connection] ${msg}")

  private object TrustAny extends javax.net.ssl.X509TrustManager {
    import java.security.cert.X509Certificate

    override def checkClientTrusted(cs: Array[X509Certificate], a: String) = {}
    override def checkServerTrusted(cs: Array[X509Certificate], a: String) = {}
    override def getAcceptedIssuers(): Array[X509Certificate] = null
  }
}

private[reactivemongo] object ChannelFactory {
  import reactivemongo.io.netty.util.AttributeKey

  val hostKey = AttributeKey.newInstance[String]("mongoHost")

  val portKey = AttributeKey.newInstance[Int]("mongoPort")

  val actorRefKey = AttributeKey.newInstance[ActorRef]("actorRef")
}
