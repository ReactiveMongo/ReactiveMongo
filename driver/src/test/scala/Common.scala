import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.duration._

import reactivemongo.api.{
  AsyncDriver,
  CrAuthentication,
  DefaultDB,
  FailoverStrategy,
  MongoDriver,
  MongoConnection,
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

  private val driverReg = Seq.newBuilder[MongoDriver]
  def newDriver(): MongoDriver = driverReg.synchronized {
    val drv = MongoDriver()

    driverReg += drv

    drv
  }

  lazy val driver = newDriver()

  val failoverStrategy = FailoverStrategy(retries = failoverRetries)

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

  val DefaultOptions = {
    val a = MongoConnectionOptions(
      failoverStrategy = failoverStrategy,
      nbChannelsPerNode = 20,
      monitorRefreshMS = (timeout.toMillis / 2).toInt)

    val b = {
      if (Option(System getProperty "test.enableSSL").exists(_ == "true")) {
        a.copy(sslEnabled = true, sslAllowsInvalidCert = true)
      } else a
    }

    crMode.fold(b) { mode => b.copy(authMode = mode) }
  }

  lazy val connection = driver.connection(List(primaryHost), DefaultOptions)

  val commonDb = "specs2-test-reactivemongo"

  // ---

  val slowFailover = {
    val retries = Option(System getProperty "test.slowFailoverRetries").
      fold(20)(_.toInt)

    failoverStrategy.copy(retries = retries)
  }

  val slowPrimary = Option(
    System getProperty "test.slowPrimaryHost").getOrElse("localhost:27019")

  val slowTimeout: FiniteDuration = {
    val maxTimeout = estTimeout(slowFailover)

    if (maxTimeout < 10.seconds) 10.seconds
    else maxTimeout
  }

  val SlowOptions = DefaultOptions.copy(
    failoverStrategy = slowFailover,
    monitorRefreshMS = (slowTimeout.toMillis / 2).toInt)

  lazy val slowProxy: NettyProxy = {
    import java.net.InetSocketAddress
    import ExecutionContext.Implicits.global

    val delay = Option(System getProperty "test.slowProxyDelay").
      fold(500L /* ms */ )(_.toLong)

    import NettyProxy.InetAddress

    def localAddr: InetSocketAddress = InetAddress.unapply(slowPrimary).get
    def remoteAddr: InetSocketAddress = InetAddress.unapply(primaryHost).get

    val proxy = new NettyProxy(Seq(localAddr), remoteAddr, Some(delay))

    Await.result(proxy.start().map(_ => proxy), slowTimeout)
  }

  lazy val slowConnection = driver.connection(List(slowPrimary), SlowOptions)

  def databases(con: MongoConnection, slowCon: MongoConnection): (DefaultDB, DefaultDB) = {
    import ExecutionContext.Implicits.global

    val _db = con.database(
      commonDb, failoverStrategy).flatMap { d => d.drop.map(_ => d) }

    if (slowProxy.isStarted) {
      Await.result(_db, timeout) -> Await.result(
        slowCon.database(commonDb, slowFailover), slowTimeout)
    } else {
      sys.error("Slow proxy is not up")
    }
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

  private val asyncDriverReg = Seq.newBuilder[AsyncDriver]
  def newAsyncDriver(): AsyncDriver = asyncDriverReg.synchronized {
    val drv = AsyncDriver()

    asyncDriverReg += drv

    drv
  }

  // ---

  def close(): Unit = {
    import ExecutionContext.Implicits.global

    driverReg.result().foreach { driver =>
      try {
        driver.close(timeout)
      } catch {
        case e: Throwable =>
          logger.warn(s"Fails to stop driver: $e")
          logger.debug("Fails to stop driver", e)
      }
    }

    asyncDriverReg.result().foreach { driver =>
      try {
        Await.result(driver.close(timeout), timeout * 1.2D)
      } catch {
        case e: Throwable =>
          logger.warn(s"Fails to stop async driver: $e")
          logger.debug("Fails to stop async driver", e)
      }
    }

    slowProxy.stop()
  }
}
