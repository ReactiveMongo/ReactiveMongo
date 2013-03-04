object Common {
  import scala.concurrent._
  import scala.concurrent.duration._
  import reactivemongo.api._

  implicit val ec = ExecutionContext.Implicits.global
  //implicit val writer = DefaultBSONHandlers.DefaultBSONDocumentWriter
 // implicit val reader = DefaultBSONHandlers.DefaultBSONDocumentReader
  
  val timeout = 10 seconds
  
  lazy val driver = new MongoDriver
  lazy val connection = driver.connection(List("localhost:27017"))
  lazy val db = {
    val _db = connection("specs2-test-reactivemongo")
    Await.ready(_db.drop, timeout)
    _db
  }
}