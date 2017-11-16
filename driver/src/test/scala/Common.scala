import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.duration._
import reactivemongo.api.{
  CrAuthentication,
  DefaultDB,
  FailoverStrategy,
  MongoDriver,
  MongoConnection,
  MongoConnectionOptions,
  X509Authentication
}
import reactivemongo.core.nodeset.Authenticate

object Common extends CommonAuth {
  val logger = reactivemongo.util.LazyLogger("tests")

  val replSetOn =
    Option(System getProperty "test.replicaSet").fold(false) {
      case "true" => true
      case _      => false
    }

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
      nbChannelsPerNode = 20)

    val b = {
      if (Option(System getProperty "test.enableSSL").exists(_ == "true")) {
        a.copy(sslEnabled = true, sslAllowsInvalidCert = true)
      } else a
    }

    authMode.fold(b) { mode => b.copy(authMode = mode) }
  }

  lazy val connection = makeConnection(List(primaryHost), DefaultOptions)

  private val timeoutFactor = 1.25D
  def estTimeout(fos: FailoverStrategy): FiniteDuration =
    (1 to fos.retries).foldLeft(fos.initialDelay) { (d, i) =>
      d + (fos.initialDelay * ((timeoutFactor * fos.delayFactor(i)).toLong))
    }

  val timeout: FiniteDuration = increaseTimeoutIfX509 {
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
    failoverStrategy = slowFailover)

  val slowPrimary = Option(
    System getProperty "test.slowPrimaryHost").getOrElse("localhost:27019")

  val slowTimeout: FiniteDuration = increaseTimeoutIfX509 {
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

  lazy val slowConnection = makeConnection(List(slowPrimary), SlowOptions)

  def databases(con: MongoConnection, slowCon: MongoConnection): (DefaultDB, DefaultDB) = {
    import ExecutionContext.Implicits.global

    val _db = con.database(
      commonDb, failoverStrategy).flatMap { d => d.drop.map(_ => d) }

    Await.result(_db, timeout) -> Await.result(
      slowCon.database(commonDb, slowFailover), slowTimeout)
  }

  lazy val (db, slowDb) = databases(connection, slowConnection)

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
        driver.close(timeout)
      } catch {
        case e: Throwable =>
          logger.warn(s"Fails to stop the default driver: $e")
          logger.debug("Fails to stop the default driver", e)
      }
    }

    slowProxy.stop()
  }

}

trait CommonAuth {

  import reactivemongo.api.AuthenticationMode

  def driver: MongoDriver

  def authMode: Option[AuthenticationMode] =
    Option(System.getProperty("test.authMode")).flatMap {
      case "cr"   => Some(CrAuthentication)
      case "x509" => Some(X509Authentication)
      case _      => None
    }

  def defaultAuthentications: Seq[Authenticate] =
    ifX509(Seq(Authenticate("", certSubject, "")))(otherwise = Nil)

  def certSubject: String =
    Option(System.getProperty("test.clientCertSubject"))
      .getOrElse(sys.error("Client cert subject required if X509 auth enabled"))

  def increaseTimeoutIfX509(timeout: FiniteDuration): FiniteDuration =
    ifX509(timeout * 10)(otherwise = timeout)

  def makeConnection(nodes: Seq[String], options: MongoConnectionOptions): MongoConnection = {
    makeConnection(driver)(nodes, options)
  }

  def makeConnection(driver: MongoDriver)(
    nodes: Seq[String],
    options: MongoConnectionOptions): MongoConnection = {
    driver.connection(nodes, options, defaultAuthentications)
  }

  def ifX509[T](block: => T)(otherwise: => T): T =
    authMode match {
      case Some(X509Authentication) => block
      case _                        => otherwise
    }
}
