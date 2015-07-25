object Common {
  import scala.concurrent.{ Await, ExecutionContext }
  import scala.concurrent.duration._
  import reactivemongo.api.{ MongoDriver, MongoConnectionOptions }

  implicit val ec = ExecutionContext.Implicits.global

  val timeout = 10 seconds
  val timeoutMillis = timeout.toMillis.toInt
  val DefaultOptions = {
    val opts = MongoConnectionOptions()

    if (Option(System getProperty "test.enableSSL").exists(_ == "true")) {
      opts.copy(sslEnabled = true, sslAllowsInvalidCert = true)
    }
    else opts
  }

  lazy val driver = new MongoDriver
  lazy val connection = driver.connection(
    List("localhost:27017"), DefaultOptions)

  lazy val db = {
    val _db = connection("specs2-test-reactivemongo")
    Await.ready(_db.drop, timeout)
    _db
  }

  def closeDriver(): Unit = try {
    driver.close()
  }
  catch { case _: Throwable => () }
}
