object Common {
  import scala.concurrent._
  import scala.concurrent.duration._
  import reactivemongo.api._
  import reactivemongo.bson.handlers.DefaultBSONHandlers

  implicit val ec = ExecutionContext.Implicits.global
  implicit val writer = DefaultBSONHandlers.DefaultBSONDocumentWriter
  implicit val reader = DefaultBSONHandlers.DefaultBSONDocumentReader
  implicit val handler = DefaultBSONHandlers.DefaultBSONReaderHandler
  
  val timeout = 5 seconds
  
  lazy val connection = MongoConnection(List("localhost:27017"))
  lazy val db = {
    val _db = connection("specs2-test-reactivemongo")
    Await.ready(_db.drop, timeout)
    _db
  }
}