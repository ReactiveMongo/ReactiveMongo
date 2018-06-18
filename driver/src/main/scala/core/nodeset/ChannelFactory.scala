package reactivemongo.core.nodeset // TODO: Move to `netty` package

import java.lang.{ Boolean => JBool }

import scala.concurrent.Promise

import shaded.netty.util.concurrent.{ Future, GenericFutureListener }

import shaded.netty.bootstrap.Bootstrap

import shaded.netty.channel.{
  Channel,
  ChannelOption,
  EventLoopGroup
}, ChannelOption.{ CONNECT_TIMEOUT_MILLIS, SO_KEEPALIVE, TCP_NODELAY }

import shaded.netty.channel.ChannelInitializer

import akka.actor.ActorRef

import reactivemongo.util.LazyLogger

import reactivemongo.core.protocol.{
  MongoHandler,
  RequestEncoder,
  ResponseFrameDecoder,
  ResponseDecoder
}

import reactivemongo.api.MongoConnectionOptions

/**
 * @param supervisor the name of the driver supervisor
 * @param connection the name of the connection pool
 */
@deprecated("Internal class: will be made private", "0.11.14")
final class ChannelFactory private[reactivemongo] (
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
    option(TCP_NODELAY, new JBool(options.tcpNoDelay)).
    option(SO_KEEPALIVE, new JBool(options.keepAlive)).
    option(CONNECT_TIMEOUT_MILLIS, new Integer(options.connectTimeoutMS)).
    handler(this)
  //childHandler(new shaded.netty.channel.ChannelHandlerAdapter {})
  //config.setBufferFactory(bufferFactory)

  private[reactivemongo] def create(
    host: String = "localhost",
    port: Int = 27017,
    receiver: ActorRef): Channel = {
    val resolution = channelFactory.connect(host, port)
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

  def initChannel(channel: Channel) {
    val host = channel.attr(ChannelFactory.hostKey).get

    if (host == null) {
      warn("Skip channel init as host is null")
    } else {
      val port = channel.attr(ChannelFactory.portKey).get
      val receiver = channel.attr(ChannelFactory.actorRefKey).get

      initChannel(channel, host, port, receiver)
    }
  }

  private[reactivemongo] def initChannel(
    channel: Channel,
    host: String, port: Int,
    receiver: ActorRef) {
    debug(s"Initializing channel ${channel.id} to ${host}:${port} ($receiver)")

    val pipeline = channel.pipeline

    if (options.sslEnabled) {
      val sslEng = reactivemongo.core.SSL.
        createEngine(sslContext, host, port)

      val sslHandler =
        new shaded.netty.handler.ssl.SslHandler(sslEng, false /* TLS */ )

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
        def operationComplete(f: Future[Any]) {
          childGroup.shutdownGracefully().
            addListener(new GenericFutureListener[Future[Any]] {
              def operationComplete(f: Future[Any]) {
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

  @inline private def trace(msg: => String) =
    logger.trace(s"[$supervisor/$connection] ${msg}")

  @inline private def warn(msg: => String) =
    logger.warn(s"[$supervisor/$connection] ${msg}")

  private object TrustAny extends javax.net.ssl.X509TrustManager {
    import java.security.cert.X509Certificate

    override def checkClientTrusted(cs: Array[X509Certificate], a: String) = {}
    override def checkServerTrusted(cs: Array[X509Certificate], a: String) = {}
    override def getAcceptedIssuers(): Array[X509Certificate] = null
  }
}

private[reactivemongo] object ChannelFactory {
  import shaded.netty.util.AttributeKey

  val hostKey = AttributeKey.newInstance[String]("mongoHost")

  val portKey = AttributeKey.newInstance[Int]("mongoPort")

  val actorRefKey = AttributeKey.newInstance[ActorRef]("actorRef")
}
