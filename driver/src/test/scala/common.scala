object Common {
  import scala.concurrent._
  import scala.concurrent.duration._
  import reactivemongo.api._

  implicit val ec = ExecutionContext.Implicits.global

  val timeout = 10 seconds
  val timeoutMillis = timeout.toMillis.toInt

  lazy val driver = new MongoDriver
  lazy val connection = driver.connection(List("localhost:27017"))
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
