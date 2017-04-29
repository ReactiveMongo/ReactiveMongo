import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.duration._
import reactivemongo.api.{
  CrAuthentication,
  FailoverStrategy,
  MongoDriver,
  MongoConnectionOptions
}

object Common {
  val logger = reactivemongo.util.LazyLogger("tests")

  val replSetOn =
    Option(System getProperty "test.replicaSet").fold(false) {
      case "true" => true
      case _      => false
    }

  val crMode = Option(System getProperty "test.authMode").
    filter(_ == "cr").map(_ => CrAuthentication)

  val primaryHost =
    Option(System getProperty "test.primaryHost").getOrElse("localhost:27017")

  val failoverRetries = Option(System getProperty "test.failoverRetries").
    flatMap(r => scala.util.Try(r.toInt).toOption).getOrElse(7)

  @volatile private var driverStarted = false
  lazy val driver = {
    val d = MongoDriver()
    driverStarted = true
    d
  }

  val failoverStrategy = FailoverStrategy(retries = failoverRetries)

  val DefaultOptions = {
    val a = MongoConnectionOptions(
      failoverStrategy = failoverStrategy,
      nbChannelsPerNode = 20
    )

    val b = {
      if (Option(System getProperty "test.enableSSL").exists(_ == "true")) {
        a.copy(sslEnabled = true, sslAllowsInvalidCert = true)
      } else a
    }

    crMode.fold(b) { mode => b.copy(authMode = mode) }
  }

  lazy val connection = driver.connection(List(primaryHost), DefaultOptions)

  private val timeoutFactor = 1.25D
  def estTimeout(fos: FailoverStrategy): FiniteDuration =
    (1 to fos.retries).foldLeft(fos.initialDelay) { (d, i) =>
      d + (fos.initialDelay * ((timeoutFactor * fos.delayFactor(i)).toLong))
    }

  val timeout: FiniteDuration = {
    val maxTimeout = estTimeout(failoverStrategy)

    if (maxTimeout < 10.seconds) 10.seconds
    else maxTimeout
  }

  val commonDb = "specs2-test-reactivemongo"

  // ---

  val slowFailover = {
    val retries = Option(System getProperty "test.slowFailoverRetries").
      fold(20)(_.toInt)

    failoverStrategy.copy(retries = retries)
  }

  val SlowOptions = DefaultOptions.copy(
    failoverStrategy = slowFailover
  )

  val slowPrimary = Option(
    System getProperty "test.slowPrimaryHost"
  ).getOrElse("localhost:27019")

  val slowTimeout: FiniteDuration = {
    val maxTimeout = estTimeout(slowFailover)

    if (maxTimeout < 10.seconds) 10.seconds
    else maxTimeout
  }

  val slowProxy: NettyProxy = {
    import java.net.InetSocketAddress

    val delay = Option(System getProperty "test.slowProxyDelay").
      fold(500L /* ms */ )(_.toLong)

    val AddressPort = """^(.*):(.*)$""".r
    def localAddr: InetSocketAddress = slowPrimary match {
      case AddressPort(addr, p) => try {
        val port = p.toInt
        new InetSocketAddress(addr, port)
      } catch {
        case e: Throwable =>
          logger.error(s"fails to prepare local address: $e")
          throw e
      }

      case _ => sys.error(s"invalid local address: $slowPrimary")
    }

    def remoteAddr: InetSocketAddress = primaryHost.span(_ != ':') match {
      case (host, p) => try {
        val port = p.drop(1).toInt
        new InetSocketAddress(host, port)
      } catch {
        case e: Throwable =>
          logger.error(s"fails to prepare remote address: $e")
          throw e
      }

      case _ => sys.error(s"invalid remote address: $primaryHost")
    }

    val proxy = new NettyProxy(Seq(localAddr), remoteAddr, Some(delay))
    proxy.start()
    proxy
  }

  lazy val slowConnection = driver.connection(List(slowPrimary), SlowOptions)

  lazy val (db, slowDb) = {
    import ExecutionContext.Implicits.global

    val _db = connection.database(commonDb, failoverStrategy).
      flatMap { d => d.drop.map(_ => d) }

    Await.result(_db, timeout) -> Await.result(
      slowConnection.database(commonDb, slowFailover), slowTimeout
    )
  }

  @annotation.tailrec
  def tryUntil[T](retries: List[Int])(f: => T, test: T => Boolean): Boolean =
    if (test(f)) true else retries match {
      case delay :: next => {
        Thread.sleep(delay.toLong)
        tryUntil(next)(f, test)
      }

      case _ => false
    }

  // ---

  def close(): Unit = {
    if (driverStarted) {
      try {
        driver.close()
      } catch {
        case e: Throwable =>
          logger.warn(s"Fails to stop the default driver: $e")
          logger.debug("Fails to stop the default driver", e)
      }
    }

    slowProxy.stop()
  }
}
