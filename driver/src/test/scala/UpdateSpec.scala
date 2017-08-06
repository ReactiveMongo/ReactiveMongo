import scala.concurrent.duration.FiniteDuration

import reactivemongo.bson._

import reactivemongo.api.ReadPreference
import reactivemongo.api.collections.bson.BSONCollection

import reactivemongo.api.commands.{
  CommandError,
  DefaultWriteResult,
  UpdateWriteResult,
  WriteResult,
  Upserted
}
import reactivemongo.api.commands.bson.BSONUpdateCommand._
import reactivemongo.api.commands.bson.BSONUpdateCommandImplicits._

import org.specs2.concurrent.{ ExecutionEnv => EE }

class UpdateSpec extends org.specs2.mutable.Specification {
  "Update" title

  sequential

  import Common._

  lazy val col1 = db(s"update1${System identityHashCode db}")
  lazy val slowCol1 = slowDb(s"slowup1{System identityHashCode db}")
  lazy val col2 = db(s"update2${System identityHashCode slowDb}")
  lazy val slowCol2 = slowDb(s"slowup2${System identityHashCode slowDb}")

  case class Person(firstName: String, lastName: String, age: Int)

  implicit object PersonReader extends BSONDocumentReader[Person] {
    def read(doc: BSONDocument) = Person(
      doc.getAs[String]("firstName").getOrElse(""),
      doc.getAs[String]("lastName").getOrElse(""),
      doc.getAs[Int]("age").getOrElse(0)
    )
  }

  implicit object PersonWriter extends BSONDocumentWriter[Person] {
    def write(person: Person) = BSONDocument(
      "firstName" -> person.firstName,
      "lastName" -> person.lastName,
      "age" -> person.age
    )
  }

  "Update" should {
    {
      def spec(c: BSONCollection, timeout: FiniteDuration)(implicit ee: EE) = {
        val jack = Person("Jack", "London", 27)

        c.update(jack, BSONDocument("$set" -> BSONDocument("age" -> 33)),
          upsert = true) must beLike[UpdateWriteResult]({
          case result => result.upserted.toList must beLike[List[Upserted]] {
            case Upserted(0, id: BSONObjectID) :: Nil =>
              c.find(BSONDocument("_id" -> id)).one[Person].
                aka("found") must beSome(jack.copy(age = 33)).
                await(1, timeout)
          }
        }).await(1, timeout)

      }

      "upsert a person with the default connection" in { implicit ee: EE =>
        spec(col1, timeout)
      }

      "upsert a person with the slow connection and Secondary preference" in {
        implicit ee: EE =>
          val coll = slowCol1.withReadPreference(ReadPreference.secondary)

          coll.readPreference must_== ReadPreference.secondary and {
            spec(coll, timeout)
          }
      }
    }

    {
      def spec(c: BSONCollection, timeout: FiniteDuration)(implicit ee: EE) = {
        val doc = BSONDocument("_id" -> "foo", "bar" -> 2)

        c.update(BSONDocument.empty, doc, upsert = true).
          map(_.upserted.toList) must beLike[List[Upserted]] {
            case Upserted(0, id @ BSONString("foo")) :: Nil =>
              c.find(BSONDocument("_id" -> id)).one[BSONDocument].
                aka("found") must beSome(doc).await(1, timeout)
          }.await(1, timeout) and {
            c.insert(doc).map(_ => true).recover {
              case WriteResult.Code(11000) => false
            } must beFalse.await(0, timeout)
          }
      }

      "upsert a document with the default connection" in { implicit ee: EE =>
        spec(col2, timeout)
      }

      "upsert a document with the slow connection" in { implicit ee: EE =>
        spec(slowCol2, slowTimeout)
      }
    }

    {
      def spec(c: BSONCollection, timeout: FiniteDuration)(implicit ee: EE) = {
        val jack = Person("Jack", "London", 33)

        c.runCommand(Update(UpdateElement(
          q = jack, u = BSONDocument("$set" -> BSONDocument("age" -> 66))
        )), ReadPreference.primary) must beLike[UpdateWriteResult]({
          case result => result.nModified mustEqual 1 and (
            c.find(BSONDocument("age" -> 66)).
            one[Person] must beSome(jack.copy(age = 66)).await(1, timeout)
          )
        }).await(1, timeout)
      }

      "update a person with the default connection" in { implicit ee: EE =>
        spec(col1, timeout)
      }

      "update a person with the slow connection" in { implicit ee: EE =>
        spec(slowCol1, slowTimeout)
      }
    }

    "update a document" in { implicit ee: EE =>
      val doc = BSONDocument("_id" -> "foo", "bar" -> 2)

      col2.runCommand(Update(UpdateElement(
        q = doc, u = BSONDocument("$set" -> BSONDocument("bar" -> 3))
      ))) aka "result" must beLike[UpdateWriteResult]({
        case result => result.nModified must_== 1 and (
          col2.find(BSONDocument("_id" -> "foo")).one[BSONDocument].
          aka("updated") must beSome(BSONDocument(
            "_id" -> "foo", "bar" -> 3
          )).await(1, timeout)
        )
      }).await(1, timeout)
    }
  }

  "WriteResult" should {
    val error = DefaultWriteResult(
      ok = false,
      n = 1,
      writeErrors = Nil,
      writeConcernError = None,
      code = Some(23),
      errmsg = Some("Foo")
    )

    "be matched as a CommandError when failed" in {
      error must beLike {
        case CommandError.Code(code) => code must_== 23
      } and (error must beLike {
        case CommandError.Message(msg) => msg must_== "Foo"
      })
    } tag "unit"

    "not be matched as a CommandError when successful" in {
      (error.copy(ok = true) match {
        case CommandError.Code(_) | CommandError.Message(_) => true
        case _ => false
      }) must beFalse
    } tag "unit"
  }
}
