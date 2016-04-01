import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.duration._
import reactivemongo.api.{
  FailoverStrategy,
  MongoDriver,
  MongoConnectionOptions
}

object Common {
  val DefaultOptions = {
    val opts = MongoConnectionOptions()

    if (Option(System getProperty "test.enableSSL").exists(_ == "true")) {
      opts.copy(sslEnabled = true, sslAllowsInvalidCert = true)
    } else opts
  }
  val primaryHost =
    Option(System getProperty "test.primaryHost").getOrElse("localhost:27017")

  val failoverRetries = Option(System getProperty "test.failoverRetries").
    flatMap(r => scala.util.Try(r.toInt).toOption).getOrElse(7)

  lazy val driver = new MongoDriver
  lazy val connection = driver.connection(List(primaryHost), DefaultOptions)

  val failoverStrategy = FailoverStrategy(retries = failoverRetries)

  private val timeoutFactor = 1.2D
  def estTimeout(fos: FailoverStrategy): FiniteDuration =
    (1 to fos.retries).foldLeft(fos.initialDelay) { (d, i) =>
      d + (fos.initialDelay * ((timeoutFactor * fos.delayFactor(i)).toLong))
    }

  val timeout: FiniteDuration = {
    val maxTimeout = estTimeout(failoverStrategy)

    if (maxTimeout < 10.seconds) 10.seconds
    else maxTimeout
  }

  val timeoutMillis = timeout.toMillis.toInt

  val commonDb = "specs2-test-reactivemongo"
  lazy val db = {
    import ExecutionContext.Implicits.global
    val _db = connection.database(commonDb, failoverStrategy)
    Await.result(_db.flatMap { d => d.drop.map(_ => d) }, timeout)
  }

  val tcpProxy: Option[NettyProxy] =
    Option(System getProperty "test.primaryBackend").flatMap { backend =>
      import java.net.InetSocketAddress

      val delay = Option(System getProperty "test.proxyDelay").
        fold(500L /* ms */ )(_.toLong)

      def localAddr = primaryHost.span(_ != ':') match {
        case (host, p) => try {
          val port = p.drop(1).toInt
          Some(new InetSocketAddress(host, port))
        } catch {
          case e: Throwable =>
            println(s"fails to prepare local address: $e")
            Option.empty[InetSocketAddress]
        }

        case _ =>
          println(s"invalid local address: $primaryHost")
          Option.empty[InetSocketAddress]
      }

      val AddressPort = """^(.*):(.*)$""".r
      def remoteAddr = backend match {
        case AddressPort(addr, p) => try {
          val port = p.toInt
          Some(new InetSocketAddress(addr, port))
        } catch {
          case e: Throwable =>
            println(s"fails to prepare remote address: $e")
            Option.empty[InetSocketAddress]
        }

        case _ =>
          println(s"invalid remote address: $backend")
          Option.empty[InetSocketAddress]
      }

      for {
        la <- localAddr
        ra <- remoteAddr
      } yield {
        val proxy = new NettyProxy(Seq(la), ra, Some(delay))
        proxy.start()
        proxy
      }
    }

  def close(): Unit = {
    driver.close()
    tcpProxy.foreach(_.stop())
  }
}

package object tests {
  val utility = Common
}
