import org.specs2.mutable._
import scala.util.{ Try, Failure }
import scala.util.control.NonFatal

import reactivemongo.bson._
import reactivemongo.api.commands.{ Command, CommandError }
import reactivemongo.api.commands.bson._
import BSONFindAndModifyCommand._
import BSONFindAndModifyImplicits._

import org.specs2.concurrent.{ ExecutionEnv => EE }

object FindAndModifySpec extends Specification {
  import Common._

  sequential

  lazy val collection = db("FindAndModifySpec")

  case class Person(
    firstName: String,
    lastName: String,
    age: Int)

  implicit object PersonReader extends BSONDocumentReader[Person] {
    def read(doc: BSONDocument): Person = Person(
      doc.getAs[String]("firstName").getOrElse(""),
      doc.getAs[String]("lastName").getOrElse(""),
      doc.getAs[Int]("age").getOrElse(0))
  }

  implicit object PersonWriter extends BSONDocumentWriter[Person] {
    def write(person: Person): BSONDocument = BSONDocument(
      "firstName" -> person.firstName,
      "lastName" -> person.lastName,
      "age" -> person.age)
  }

  "FindAndModify" should {
    "upsert a doc and fetch it" in { implicit ee: EE =>
      val jack = Person("Jack", "London", 27)
      val upsertOp = Update(BSONDocument("$set" -> BSONDocument("age" -> 40)), fetchNewObject = true, upsert = true)
      def future = collection.runCommand(FindAndModify(jack, upsertOp))

      future must (beLike[FindAndModifyResult] {
        case result =>
          result.lastError.exists(_.upsertedId.isDefined) must beTrue and (
            result.result[Person] aka "upserted" must beSome[Person].like {
              case Person("Jack", "London", 40) => ok
            })
      }).await(1, timeout)
    }

    "modify a doc and fetch its previous value" in { implicit ee: EE =>
      val jack = Person("Jack", "London", 40)
      val incrementAge = Update(BSONDocument(
        "$inc" -> BSONDocument("age" -> 1)))

      def future = collection.runCommand(FindAndModify(jack, incrementAge))

      future must (beLike[FindAndModifyResult] {
        case result =>
          result.result[Person] aka "previous value" must beSome.like {
            case Person("Jack", "London", 40) =>
              collection.find(jack.copy(age = jack.age + 1)).
                one[Person].map(_.map(_.age)) must beSome(41).
                await(1, timeout)
          }
      }).await(1, timeout)
    }

    "make a failing FindAndModify" in { implicit ee: EE =>
      val query = BSONDocument()
      val future = collection.runCommand(
        FindAndModify(query, Update(BSONDocument("$inc" -> "age"))))

      future.map(_ => Option.empty[Int]).recover {
        case e: CommandError =>
          //e.printStackTrace
          e.code
      } must beSome( /*code = */ 9).await(1, timeout)
    }
  }
}
