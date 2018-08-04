object Common extends CommonAuth {
  import scala.concurrent.{ Await, ExecutionContext }
  import scala.concurrent.duration._
  import reactivemongo.api.{
    FailoverStrategy,
    MongoDriver,
    MongoConnectionOptions
  }

  implicit val ec = ExecutionContext.Implicits.global

  val replSetOn = sys.props.get("test.replicaSet").fold(false) {
    case "true" => true
    case _      => false
  }

  val DefaultOptions = {
    val a = MongoConnectionOptions(
      nbChannelsPerNode = 2,
      credentials = DefaultCredentials.map("" -> _)(scala.collection.breakOut),
      keyStore = sys.props.get("test.keyStore").map { uri =>
        MongoConnectionOptions.KeyStore(
          resource = new java.net.URI(uri), // file://..
          storeType = "PKCS12",
          password = sys.props.get("test.keyStorePassword").map(_.toCharArray))
      })

    val b = if (sys.props.get("test.enableSSL").exists(_ == "true")) {
      a.copy(sslEnabled = true, sslAllowsInvalidCert = true)
    } else a

    authMode.fold(b) { mode => b.copy(authenticationMechanism = mode) }
  }

  val primaryHost =
    sys.props.getOrElse("test.primaryHost", "localhost:27017")

  val failoverRetries = sys.props.get("test.failoverRetries").
    flatMap(r => scala.util.Try(r.toInt).toOption).getOrElse(7)

  lazy val driver = new MongoDriver
  lazy val connection = driver.connection(List(primaryHost), DefaultOptions)

  val failoverStrategy = FailoverStrategy(retries = failoverRetries)

  val timeout = 10.seconds //increaseTimeoutIfX509(10.seconds)
  val timeoutMillis = timeout.toMillis.toInt

  val dbName = "specs2-reactivemongo-jmx"

  lazy val db = {
    val _db = connection.database(dbName, failoverStrategy)

    Await.result(_db.flatMap { d => d.drop.map(_ => d) }, timeout)
  }

  def close(): Unit = try {
    driver.close()
  } catch { case _: Throwable => () }
}

sealed trait CommonAuth {
  import reactivemongo.api._

  def authMode: Option[AuthenticationMode] =
    Option(System.getProperty("test.authenticationMechanism")).flatMap {
      case "cr"   => Some(CrAuthentication)
      case "x509" => Some(X509Authentication)
      case _      => None
    }

  private lazy val certSubject = sys.props.get("test.clientCertSubject")

  lazy val DefaultCredentials = certSubject.toSeq.map { cert =>
    MongoConnectionOptions.Credential(cert, None)
  }

}
