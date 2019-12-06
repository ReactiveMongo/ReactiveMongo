package tests

import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.duration._

import reactivemongo.api.{
  AsyncDriver,
  CrAuthentication,
  DefaultDB,
  FailoverStrategy,
  MongoDriver,
  MongoConnection,
  MongoConnectionOptions,
  X509Authentication
}

object Common extends CommonAuth {
  val logger = reactivemongo.util.LazyLogger("tests")

  val replSetOn = sys.props.get("test.replicaSet").fold(false) {
    case "true" => true
    case _      => false
  }

  val primaryHost = sys.props.getOrElse("test.primaryHost", "localhost:27017")

  val nettyNativeArch = sys.props.get("test.nettyNativeArch")

  val failoverRetries = sys.props.get("test.failoverRetries").
    flatMap(r => scala.util.Try(r.toInt).toOption).getOrElse(7)

  val failoverStrategy = FailoverStrategy(retries = failoverRetries)

  private val timeoutFactor = 1.18D
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
    val a = MongoConnectionOptions.default.copy(
      failoverStrategy = failoverStrategy,
      heartbeatFrequencyMS = (timeout.toMillis / 2).toInt,
      credentials = DefaultCredentials.map("" -> _).toMap,
      keyStore = sys.props.get("test.keyStore").map { uri =>
        MongoConnectionOptions.KeyStore(
          resource = new java.net.URI(uri), // file://..
          storeType = "PKCS12",
          password = sys.props.get("test.keyStorePassword").map(_.toCharArray))
      })

    val b = {
      if (sys.props.get("test.enableSSL").exists(_ == "true")) {
        a.copy(sslEnabled = true, sslAllowsInvalidCert = true)
      } else a
    }

    authMode.fold(b) { mode =>
      b.copy(authenticationMechanism = mode)
    }
  }

  lazy val connection =
    Await.result(driver.connect(List(primaryHost), DefaultOptions), timeout)

  val commonDb = "specs2-test-reactivemongo"

  // ---

  val slowFailover = {
    val retries = sys.props.get("test.slowFailoverRetries").fold(20)(_.toInt)

    failoverStrategy.copy(retries = retries)
  }

  val slowPrimary = sys.props.getOrElse(
    "test.slowPrimaryHost", "localhost:27019")

  val slowTimeout: FiniteDuration = {
    val maxTimeout = estTimeout(slowFailover)

    if (maxTimeout < 10.seconds) 10.seconds
    else maxTimeout
  }

  val SlowOptions = DefaultOptions.copy(
    failoverStrategy = slowFailover,
    heartbeatFrequencyMS = (slowTimeout.toMillis / 2).toInt)

  val slowProxy: NettyProxy = {
    import java.net.InetSocketAddress
    import ExecutionContext.Implicits.global

    val delay = sys.props.get("test.slowProxyDelay").
      fold(500L /* ms */ )(_.toLong)

    import NettyProxy.InetAddress

    def localAddr: InetSocketAddress = InetAddress.unapply(slowPrimary).get
    def remoteAddr: InetSocketAddress = InetAddress.unapply(primaryHost).get

    val prx = new NettyProxy(Seq(localAddr), remoteAddr, Some(delay))

    prx.start()

    prx
  }

  lazy val slowConnection =
    Await.result(driver.connect(List(slowPrimary), SlowOptions), slowTimeout)

  def databases(name: String, con: MongoConnection, slowCon: MongoConnection): (DefaultDB, DefaultDB) = {
    import ExecutionContext.Implicits.global

    val _db = for {
      d <- con.database(name, failoverStrategy)
      _ <- d.drop
    } yield d

    Await.result(_db, timeout) -> Await.result((for {
      _ <- slowProxy.start()
      resolved <- slowCon.database(name, slowFailover)
    } yield resolved), timeout + slowTimeout)
  }

  lazy val (db, slowDb) = databases(commonDb, connection, slowConnection)

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

  lazy val driver = newAsyncDriver()

  // ---

  def close(): Unit = {
    import ExecutionContext.Implicits.global

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

sealed trait CommonAuth {
  import reactivemongo.api.AuthenticationMode

  def driver: AsyncDriver

  def authMode: Option[AuthenticationMode] =
    sys.props.get("test.authenticationMechanism").flatMap {
      case "cr"   => Some(CrAuthentication)
      case "x509" => Some(X509Authentication)
      case _      => None
    }

  private lazy val certSubject = sys.props.get("test.clientCertSubject")

  lazy val DefaultCredentials = certSubject.toSeq.map { cert =>
    MongoConnectionOptions.Credential(cert, None)
  }

  def ifX509[T](block: => T)(otherwise: => T): T =
    authMode match {
      case Some(X509Authentication) => block
      case _                        => otherwise
    }
}
