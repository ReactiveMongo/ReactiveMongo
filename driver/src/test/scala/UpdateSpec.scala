import reactivemongo.api.commands.{ UpdateWriteResult, Upserted }
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.bson.BSONUpdateCommand._
import reactivemongo.api.commands.bson.BSONUpdateCommandImplicits._
import reactivemongo.bson._

import org.specs2.concurrent.{ ExecutionEnv => EE }

object UpdateSpec extends org.specs2.mutable.Specification {
  "Update" title

  sequential

  import Common._

  val col1: BSONCollection = db("UpdateSpec")
  val col2: BSONCollection = db("UpdateSpec2")

  case class Person(firstName: String,
                    lastName: String,
                    age: Int)

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
        "age" -> person.age)
  }

  "Update" should {
    "upsert" >> {
      "a person" in { implicit ee: EE =>
        val jack = Person("Jack", "London", 27)

        col1.update(jack,
          BSONDocument("$set" -> BSONDocument("age" -> 33)),
          upsert = true) must beLike[UpdateWriteResult]({
            case result => result.upserted.toList must beLike[List[Upserted]] {
              case Upserted(0, id: BSONObjectID) :: Nil =>
                col1.find(BSONDocument("_id" -> id)).one[Person].
                  aka("found") must beSome(jack.copy(age = 33)).
                  await(1, timeout)
            }
          }).await(1, timeout)
      }

      "a document" in { implicit ee: EE =>
        val doc = BSONDocument("_id" -> "foo", "bar" -> 2)

        col2.update(BSONDocument(), doc, upsert = true).
          map(_.upserted.toList) must beLike[List[Upserted]]({
            case Upserted(0, id @ BSONString("foo")) :: Nil =>
              col2.find(BSONDocument("_id" -> id)).one[BSONDocument].
                aka("found") must beSome(doc).await(1, timeout)
          }).await(1, timeout)
      }
    }

    "update" >> {
      "a person" in { implicit ee: EE =>
        val jack = Person("Jack", "London", 33)

        col1.runCommand(Update(UpdateElement(
          q = jack, u = BSONDocument("$set" -> BSONDocument("age" -> 66))))).
          aka("result") must beLike[UpdateWriteResult]({
            case result => result.nModified mustEqual 1 and (
              col1.find(BSONDocument("age" -> 66)).
              one[Person] must beSome(jack.copy(age = 66)).
              await(1, timeout))
          }).await(1, timeout)
      }

      "a document" in { implicit ee: EE =>
        val doc = BSONDocument("_id" -> "foo", "bar" -> 2)

        col2.runCommand(Update(UpdateElement(
          q = doc, u = BSONDocument("$set" -> BSONDocument("bar" -> 3))))).
          aka("result") must beLike[UpdateWriteResult]({
            case result => result.nModified must_== 1 and (
              col2.find(BSONDocument("_id" -> "foo")).one[BSONDocument].
              aka("updated") must beSome(BSONDocument(
                "_id" -> "foo", "bar" -> 3)).await(1, timeout))
          }).await(1, timeout)
      }
    }
  }
}
