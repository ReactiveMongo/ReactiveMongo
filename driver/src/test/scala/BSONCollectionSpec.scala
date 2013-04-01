import reactivemongo.api._
import reactivemongo.bson._
import DefaultBSONHandlers._
import reactivemongo.core.commands.Count
import scala.concurrent._

import org.specs2.mutable._

class BSONCollectionSpec extends Specification {
  import Common._

  sequential

  import reactivemongo.api.collections.default._

  lazy val collection = db("somecollection_bsoncollectionspec")

  case class Person(name: String, age: Int)
  case class CustomException(msg: String) extends Exception

  object BuggyPersonWriter extends BSONDocumentWriter[Person] {
    def write(p: Person): BSONDocument = throw CustomException("PersonWrite error")
  }

  object BuggyPersonReader extends BSONDocumentReader[Person] {
    def read(doc: BSONDocument): Person = throw CustomException("hey hey hey")
  }

  val PersonWriter = Macros.writer[Person]
  val PersonReader = Macros.reader[Person]

  val person = Person("Jack", 25)

  "BSONCollection" should {
    "write a doc with success" in {
      implicit val writer = PersonWriter
      Await.result(collection.insert(person), timeout).ok mustEqual true
    }
    "read a doc with success" in {
      implicit val reader = PersonReader
      Await.result(collection.find(BSONDocument()).one[Person], timeout).get mustEqual person
    }
    "read a doc with error" in {
      implicit val reader = BuggyPersonReader
      val future = collection.find(BSONDocument()).one[Person].map(_ => 0).recover {
        case ce: CustomException => -1
        case e =>
          e.printStackTrace()
          -2
      }
      Await.result(future, timeout) mustEqual -1
    }
    "write a doc with error" in {
      implicit val writer = BuggyPersonWriter
      Await.result(
        collection.insert(person).map { lastError =>
          println(s"person write succeed??  $lastError")
          0
        }.recover {
          case ce: CustomException => -1
          case e =>
            e.printStackTrace()
            -2
        }, timeout) mustEqual -1
    }
  }
}