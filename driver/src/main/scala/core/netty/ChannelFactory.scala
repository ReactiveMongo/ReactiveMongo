package reactivemongo.core.netty

import java.util.concurrent.TimeUnit

import java.lang.{ Boolean => JBool }

import scala.util.{ Failure, Success, Try }

import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration

import reactivemongo.io.netty.bootstrap.Bootstrap
import reactivemongo.io.netty.channel.{
  Channel,
  ChannelFuture,
  ChannelFutureListener,
  ChannelInitializer,
  ChannelOption,
  EventLoopGroup
}
import reactivemongo.io.netty.handler.timeout.IdleStateHandler
import reactivemongo.io.netty.util.concurrent.{ Future, GenericFutureListener }

import reactivemongo.core.actors.ChannelDisconnected
import reactivemongo.core.errors.GenericDriverException
import reactivemongo.core.protocol.{
  MongoHandler,
  RequestEncoder,
  ResponseDecoder,
  ResponseFrameDecoder
}

import reactivemongo.api.MongoConnectionOptions

import reactivemongo.actors.actor.ActorRef
import reactivemongo.util.LazyLogger

import ChannelOption.{ CONNECT_TIMEOUT_MILLIS, SO_KEEPALIVE, TCP_NODELAY }

/**
 * @param supervisor the name of the driver supervisor
 * @param connection the name of the connection pool
 */
private[reactivemongo] final class ChannelFactory(
    supervisor: String,
    connection: String,
    options: MongoConnectionOptions)
    extends ChannelInitializer[Channel] {

  private val pack = reactivemongo.core.netty.Pack()
  private val parentGroup: EventLoopGroup = pack.eventLoopGroup()

  private val logger = LazyLogger("reactivemongo.core.nodeset.ChannelFactory")

  private val tcpNoDelay = JBool.valueOf(options.tcpNoDelay)
  private val keepAlive = JBool.valueOf(options.keepAlive)
  private val timeoutMs = Integer.valueOf(options.connectTimeoutMS)

  private[reactivemongo] def create(
      host: String = "localhost",
      port: Int = 27017,
      maxIdleTimeMS: Int = options.maxIdleTimeMS,
      receiver: ActorRef
    ): Try[Channel] = {
    if (
      parentGroup.isShuttingDown ||
      parentGroup.isShutdown || parentGroup.isTerminated
    ) {

      val msg =
        s"Cannot create channel to '${host}:${port}' from inactive factory"

      info(msg)

      Failure(new GenericDriverException(s"$msg ($supervisor/$connection)"))
    } else {
      // Create a channel bootstrap from config, with state as attributes
      // so available for the coming init (event before calling connect)
      val f = channelFactory()
      f.attr(ChannelFactory.hostKey, host)
      f.attr(ChannelFactory.portKey, port)
      f.attr(ChannelFactory.actorRefKey, receiver)
      f.attr(ChannelFactory.maxIdleTimeKey, maxIdleTimeMS)

      val resolution = f
        .connect(host, port)
        .addListener(new ChannelFutureListener {
          def operationComplete(op: ChannelFuture): Unit = {
            if (!op.isSuccess) {
              val chanId = op.channel.id

              debug(
                s"Connection to ${host}:${port} refused for channel #${chanId}",
                op.cause
              )

              receiver ! ChannelDisconnected(chanId)
            }
          }
        })

      val channel = resolution.channel

      debug(s"Created new channel #${channel.id} to ${host}:${port} (registered = ${channel.isRegistered})")

      Success(channel)
    }
  }

  def initChannel(channel: Channel): Unit = {
    val host = channel.attr(ChannelFactory.hostKey).get
    val port = channel.attr(ChannelFactory.portKey).get
    val maxIdleTimeMS = channel.attr(ChannelFactory.maxIdleTimeKey).get
    val receiver = channel.attr(ChannelFactory.actorRefKey).get

    initChannel(channel, host, port, maxIdleTimeMS, receiver)
  }

  private[reactivemongo] def initChannel(
      channel: Channel,
      host: String,
      port: Int,
      maxIdleTimeMS: Int,
      receiver: ActorRef
    ): Unit = {
    debug(s"Initializing channel ${channel.id} to ${host}:${port} ($receiver)")

    val pipeline = channel.pipeline
    val idleTimeMS = maxIdleTimeMS.toLong

    pipeline.addLast(
      "idleState",
      new IdleStateHandler(idleTimeMS, idleTimeMS, 0, TimeUnit.MILLISECONDS)
    )

    if (options.sslEnabled) {
      val sslEng = reactivemongo.core.SSL.createEngine(sslContext, host, port)
      val sslHandler = new reactivemongo.io.netty.handler.ssl.SslHandler(
        sslEng,
        false /* TLS */
      )
      pipeline.addLast("ssl", sslHandler)
    }

    pipeline.addLast(
      new ResponseFrameDecoder(),
      new ResponseDecoder(),
      new RequestEncoder(),
      new MongoHandler(supervisor, connection, receiver)
    )

    trace(s"Netty channel configuration:\n- connectTimeoutMS: ${options.connectTimeoutMS}\n- maxIdleTimeMS: ${options.maxIdleTimeMS}ms\n- tcpNoDelay: ${options.tcpNoDelay}\n- keepAlive: ${options.keepAlive}\n- sslEnabled: ${options.sslEnabled}\n- keyStore: ${options.keyStore.fold("None")(_.toString)}")
  }

  private def keyStore: Option[MongoConnectionOptions.KeyStore] =
    options.keyStore.orElse {
      sys.props.get("javax.net.ssl.keyStore").map { path =>
        MongoConnectionOptions.KeyStore(
          resource = new java.io.File(path).toURI,
          storeType = sys.props.getOrElse("javax.net.ssl.keyStoreType", "JKS"),
          password =
            sys.props.get("javax.net.ssl.keyStorePassword").map(_.toCharArray),
          trust = true
        )

      }
    }

  private def sslContext = {
    import java.security.KeyStore
    import javax.net.ssl.{
      KeyManagerFactory,
      TrustManager,
      TrustManagerFactory
    }

    lazy val loadedStore = keyStore.map { settings =>
      val password = settings.password.getOrElse(Array.empty[Char])

      reactivemongo.util.withContent(settings.resource) { storeIn =>
        val res = KeyStore.getInstance(settings.storeType)

        res.load(storeIn, password)

        res -> password
      }
    }

    val keyManagers = loadedStore.map {
      case (ks, password) =>
        val kmf = {
          val res =
            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)

          res.init(ks, password)
          res
        }

        kmf.getKeyManagers
    }

    @SuppressWarnings(Array("AsInstanceOf"))
    def sslCtx = {
      val res = javax.net.ssl.SSLContext.getInstance("SSL")

      val tm: Array[TrustManager] = {
        def trust = keyStore.fold[Boolean](true)(_.trust)

        if (options.sslAllowsInvalidCert) Array(TrustAny)
        else if (!trust) null
        else
          loadedStore.fold(null.asInstanceOf[Array[TrustManager]]) {
            case (ks, _) =>
              val tmf = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm
              )

              tmf.init(ks)

              tmf.getTrustManagers()
          }
      }

      val rand = new scala.util.Random(System.identityHashCode(tm))
      val seed = Array.ofDim[Byte](128)
      rand.nextBytes(seed)

      res.init(keyManagers.orNull, tm, new java.security.SecureRandom(seed))

      res
    }

    sslCtx
  }

  @inline private def channelFactory() = new Bootstrap()
    .group(parentGroup)
    .channel(pack.channelClass)
    .option(TCP_NODELAY, tcpNoDelay)
    .option(SO_KEEPALIVE, keepAlive)
    .option(CONNECT_TIMEOUT_MILLIS, timeoutMs)
    .handler(this)

  private[reactivemongo] def release(
      callback: Promise[Unit],
      timeout: FiniteDuration
    ): Unit = {
    def ok(): Unit = { callback.success({}); () }

    if (parentGroup.iterator.hasNext) {
      parentGroup
        .shutdownGracefully(0L, timeout.length, timeout.unit)
        .addListener(new GenericFutureListener[Future[Any]] {
          def operationComplete(f: Future[Any]): Unit = ok()
        })

      ()
    } else {
      ok()
    }
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

  val maxIdleTimeKey = AttributeKey.newInstance[Int]("maxIdleTimeMS")
}
