object Common {
  import scala.concurrent.{ Await, ExecutionContext }
  import scala.concurrent.duration._
  import reactivemongo.api.{
    FailoverStrategy,
    MongoDriver,
    MongoConnectionOptions
  }

  implicit val ec = ExecutionContext.Implicits.global

  val DefaultOptions = {
    val opts = MongoConnectionOptions(nbChannelsPerNode = 2)

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

  val timeout = 10.seconds
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
