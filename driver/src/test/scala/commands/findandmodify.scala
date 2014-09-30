import org.specs2.mutable._
import play.api.libs.iteratee.Enumerator
import scala.concurrent._
import scala.util.{ Try, Failure }

import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.api.commands.Command
import reactivemongo.api.commands.bson._
import BSONFindAndModifyCommand._
import BSONFindAndModifyImplicits._

class FindAndModifySpec extends Specification {
  import Common._

  sequential

  val collection = db("FindAndModifySpec")

  case class Person(
    firstName: String,
    lastName: String,
    age: Int
  )

  implicit object PersonReader extends BSONDocumentReader[Person] {
    def read(doc: BSONDocument): Person =
      Person(
        doc.getAs[String]("firstName").getOrElse(""),
        doc.getAs[String]("lastName").getOrElse(""),
        doc.getAs[Int]("age").getOrElse(0))
  }

  implicit object PersonWriter extends BSONDocumentWriter[Person] {
    def write(person: Person): BSONDocument =
      BSONDocument(
        "firstName" -> person.firstName,
        "lastName" -> person.lastName,
        "age" -> person.age
      )
  }

  "FindAndModify" should {
    "upsert a doc and fetch it" in {
      val jack = Person("Jack", "London", 27)
      val future = collection.runCommand(FindAndModify(jack, Update(BSONDocument("$set" -> BSONDocument("age" -> 40)), fetchNewObject = true), upsert = true))
      val result = Await.result(future, timeout)
      println(s"FAM(upsert) result is $result")
      println(result.value.map(BSONDocument.pretty))
      result.lastError.exists(_.upsertedId.isDefined) mustEqual true
      val upserted = result.result[Person]
      upserted.isDefined mustEqual true
      upserted.get.firstName mustEqual "Jack"
      upserted.get.lastName mustEqual "London"
      upserted.get.age mustEqual 40
    }
    "modify a doc and fetch its previous value" in {
      val jack = Person("Jack", "London", 40)
      //val future = collection.runCommand(FindAndModify(jack, Update(BSONDocument("$inc" -> "age"))))
      val future = collection.runCommand(FindAndModify(jack, Update(BSONDocument("$inc" -> BSONDocument("age" -> 1)))))
      val result = Await.result(future, timeout)
      println(s"FAM(modify) result is $result")
      result.lastError.exists(_.upsertedId.isEmpty) mustEqual true
      val previousValue = result.result[Person]
      previousValue.isDefined mustEqual true
      previousValue.get.firstName mustEqual "Jack"
      previousValue.get.lastName mustEqual "London"
      previousValue.get.age mustEqual 40
      Await.result(collection.find(jack.copy(age = jack.age + 1)).one[Person], timeout).exists(_.age == 41) mustEqual true
    }
  }
}