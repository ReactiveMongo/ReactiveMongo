import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.bson.BSONUpdateCommand._
import reactivemongo.api.commands.bson.BSONUpdateCommandImplicits._
import reactivemongo.bson._

import scala.concurrent._

object UpdateSpec extends org.specs2.mutable.Specification {
  "Update" title

  sequential

  import Common._

  val collection: BSONCollection = db("UpdateSpec")

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
    "upsert a doc" in {
      val jack = Person("Jack", "London", 27)
      val result = Await.result(
        collection.update(jack,
          BSONDocument("$set" -> BSONDocument("age" -> 33)),
          upsert = true), timeout)

      result.upserted must have size (1)

      collection.find(BSONDocument("_id" -> result.upserted(0)._id.asInstanceOf[BSONObjectID])).one[Person] must beSome(jack.copy(age = 33)).
        await(timeoutMillis)
    }

    "update a doc" in {
      val jack = Person("Jack", "London", 33)
      val result = Await.result(
        collection.runCommand(Update(UpdateElement(
          q = jack,
          u = BSONDocument("$set" -> BSONDocument("age" -> 66))))),
        timeout)

      result.nModified mustEqual 1

      collection.find(BSONDocument("age" -> 66)).one[Person].
        aka("updated") must beSome(jack.copy(age = 66)).await(timeoutMillis)
    }
  }
}
