package reactivemongo.core.nodeset // TODO: Move to `netty` package?

import java.util.concurrent.{ TimeUnit, Executor, Executors }

import shaded.netty.util.HashedWheelTimer
import shaded.netty.buffer.HeapChannelBufferFactory
import shaded.netty.channel.socket.nio.NioClientSocketChannelFactory
import shaded.netty.channel.{
  Channel,
  ChannelPipeline,
  Channels
}
import shaded.netty.handler.timeout.IdleStateHandler

import akka.actor.ActorRef

import reactivemongo.util.LazyLogger

import reactivemongo.core.protocol.{
  MongoHandler,
  RequestEncoder,
  ResponseDecoder,
  ResponseFrameDecoder
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
  options: MongoConnectionOptions,
  bossExecutor: Executor = Executors.newCachedThreadPool,
  workerExecutor: Executor = Executors.newCachedThreadPool) {

  @deprecated("Initialize with related mongosystem", "0.11.14")
  def this(opts: MongoConnectionOptions) =
    this(
      s"unknown-${System identityHashCode opts}",
      s"unknown-${System identityHashCode opts}", opts)

  @deprecated("Initialize with related mongosystem", "0.11.14")
  def this(opts: MongoConnectionOptions, bossEx: Executor) =
    this(
      s"unknown-${System identityHashCode opts}",
      s"unknown-${System identityHashCode opts}", opts, bossEx)

  @deprecated("Initialize with related mongosystem", "0.11.14")
  def this(opts: MongoConnectionOptions, bossEx: Executor, workerEx: Executor) =
    this(
      s"unknown-${System identityHashCode opts}",
      s"unknown-${System identityHashCode opts}", opts, bossEx, workerEx)

  private val logger = LazyLogger("reactivemongo.core.nodeset.ChannelFactory")
  private val timer = new HashedWheelTimer()

  def create(host: String = "localhost", port: Int = 27017, receiver: ActorRef): Channel = {
    val channel = makeChannel(host, port, receiver)

    logger.trace(s"[$supervisor/$connection] Created a new channel to ${host}:${port}: $channel")

    channel
  }

  val channelFactory = new NioClientSocketChannelFactory(
    bossExecutor, workerExecutor)

  private val bufferFactory = new HeapChannelBufferFactory(
    java.nio.ByteOrder.LITTLE_ENDIAN)

  private def makePipeline(
    timeoutMS: Long,
    host: String,
    port: Int,
    receiver: ActorRef): ChannelPipeline = {
    val idleHandler = new IdleStateHandler(
      timer, 0, 0, timeoutMS, TimeUnit.MILLISECONDS)

    val pipeline = Channels.pipeline(idleHandler, new ResponseFrameDecoder(),
      new ResponseDecoder(), new RequestEncoder(),
      new MongoHandler(supervisor, connection, receiver))

    if (options.sslEnabled) {
      val sslEng = reactivemongo.core.SSL.createEngine(sslContext, host, port)
      val sslHandler =
        new shaded.netty.handler.ssl.SslHandler(sslEng, false /* TLS */ )

      pipeline.addFirst("ssl", sslHandler)
    }

    pipeline
  }

  private def sslContext = {
    import java.io.FileInputStream
    import java.security.KeyStore
    import javax.net.ssl.{ KeyManagerFactory, TrustManager }

    val keyManagers = sys.props.get("javax.net.ssl.keyStore").map { path =>
      val password = sys.props.getOrElse("javax.net.ssl.keyStorePassword", "")

      val ks = {
        val ksType = sys.props.getOrElse("javax.net.ssl.keyStoreType", "JKS")
        val res = KeyStore.getInstance(ksType)

        val fis = new FileInputStream(path)
        try {
          res.load(fis, password.toCharArray)
        } finally {
          fis.close()
        }

        res
      }

      val kmf = {
        val res = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
        res.init(ks, password.toCharArray)
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

  private def makeChannel(
    host: String,
    port: Int,
    receiver: ActorRef): Channel = {

    val channel = channelFactory.newChannel(makePipeline(
      options.maxIdleTimeMS.toLong, host, port, receiver))
    val config = channel.getConfig

    config.setTcpNoDelay(options.tcpNoDelay)
    config.setBufferFactory(bufferFactory)
    config.setKeepAlive(options.keepAlive)
    config.setConnectTimeoutMillis(options.connectTimeoutMS)

    logger.trace(s"Netty channel configuration:\n- connectTimeoutMS: ${options.connectTimeoutMS}\n- maxIdleTimeMS: ${options.maxIdleTimeMS}ms\n- tcpNoDelay: ${options.tcpNoDelay}\n- keepAlive: ${options.keepAlive}\n- sslEnabled: ${options.sslEnabled}")

    channel
  }

  private object TrustAny extends javax.net.ssl.X509TrustManager {
    import java.security.cert.X509Certificate

    override def checkClientTrusted(cs: Array[X509Certificate], a: String) = {}
    override def checkServerTrusted(cs: Array[X509Certificate], a: String) = {}
    override def getAcceptedIssuers(): Array[X509Certificate] = null
  }
}
