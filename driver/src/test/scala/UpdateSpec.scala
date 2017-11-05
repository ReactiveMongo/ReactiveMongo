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

import org.specs2.concurrent.ExecutionEnv

class UpdateSpec(implicit val ee: ExecutionEnv)
  extends org.specs2.mutable.Specification with UpdateFixtures {

  "Update" title

  sequential

  import Common._

  lazy val col1 = db(s"update1${System identityHashCode db}")
  lazy val slowCol1 = slowDb(s"slowup1${System identityHashCode db}")
  lazy val col2 = db(s"update2${System identityHashCode slowDb}")
  lazy val slowCol2 = slowDb(s"slowup2${System identityHashCode slowDb}")

  "ReactiveMongo" should {
    // with fixtures ...
    val jack3 = Person3("Jack", "London", 27, BigDecimal("12.345"))
    val jack = Person("Jack", "London", 27)
    val jane3 = Person3("Jane", "London", 18, BigDecimal("3.45"))
    val jane = Person("Jane", "London", 18)

    {
      def spec[T: BSONDocumentWriter: BSONDocumentReader](c: BSONCollection, timeout: FiniteDuration, f: => T)(upd: T => T) = {
        val person = f

        c.update(person, BSONDocument("$set" -> BSONDocument("age" -> 33)),
          upsert = true) must beLike[UpdateWriteResult]({
          case result => result.upserted.toList must beLike[List[Upserted]] {
            case Upserted(0, id: BSONObjectID) :: Nil =>
              c.find(BSONDocument("_id" -> id)).one[T].
                aka("found") must beSome(upd(person)).await(1, timeout)
          }
        }).await(1, timeout)
      }

      section("mongo2", "mongo24", "not_mongo26")
      "upsert with MongoDB < 3" >> {
        "a person with the default connection" in {
          spec(col1, timeout, jack)(_.copy(age = 33))
        }

        "a person with the slow connection and Secondary preference" in {
          val coll = slowCol1.withReadPreference(
            ReadPreference.secondaryPreferred)

          coll.readPreference must_== ReadPreference.secondaryPreferred and {
            spec(coll, slowTimeout, jane)(_.copy(age = 33))
          }
        }
      }
      section("mongo2", "mongo24", "not_mongo26")

      section("gt_mongo3")
      "upsert with MongoDB 3.4+" >> {
        "a person with the default connection" in {
          spec(col1, timeout, jack3)(_.copy(age = 33))
        }

        "a person with the slow connection and Secondary preference" in {
          val coll = slowCol1.withReadPreference(
            ReadPreference.secondaryPreferred)

          coll.readPreference must_== ReadPreference.secondaryPreferred and {
            spec(coll, slowTimeout, jane3)(_.copy(age = 33))
          }
        }
      }
      section("gt_mongo3")
    }

    "upsert a document" >> {
      def spec(c: BSONCollection, timeout: FiniteDuration) = {
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

      "with the default connection" in {
        spec(col2, timeout)
      }

      "with the slow connection" in {
        spec(slowCol2, slowTimeout)
      }
    }

    "update a document directly using the Update command" in {
      val doc = BSONDocument("_id" -> "foo", "bar" -> 2)

      col2.runCommand(
        Update(UpdateElement(
          q = doc, u = BSONDocument("$set" -> BSONDocument("bar" -> 3)))),
        ReadPreference.primary) aka "result" must beLike[UpdateWriteResult]({
          case result => result.nModified must_== 1 and (
            col2.find(BSONDocument("_id" -> "foo")).one[BSONDocument].
            aka("updated") must beSome(BSONDocument(
              "_id" -> "foo", "bar" -> 3)).await(1, timeout))
        }).await(1, timeout)
    }

    {
      def spec[T: BSONDocumentWriter: BSONDocumentReader](c: BSONCollection, timeout: FiniteDuration, f: => T)(upd: T => T) = {
        val person = f

        c.runCommand(
          Update(UpdateElement(
            q = person, u = BSONDocument("$set" -> BSONDocument("age" -> 66)))),
          ReadPreference.primary) must beLike[UpdateWriteResult]({
            case result => result.nModified mustEqual 1 and (
              c.find(BSONDocument("age" -> 66)).
              one[T] must beSome(upd(person)).await(1, timeout))
          }).await(1, timeout)
      }

      section("mongo2", "mongo24", "not_mongo26")
      "update with MongoDB < 3" >> {
        "a person with the default connection" in {
          val person = jack.copy(age = 33) // as after previous upsert test

          spec(col1, timeout, person)(_.copy(age = 66))
        }

        "a person with the slow connection" in {
          val person = jane.copy(age = 33) // as after previous upsert test

          spec(slowCol1, slowTimeout, person)(_.copy(age = 66))
        }
      }
      section("mongo2", "mongo24", "not_mongo26")

      section("gt_mongo3")
      "update with MongoDB 3.4+" >> {
        "a person with the default connection" in {
          val person = jack3.copy(age = 33) // as after previous upsert test

          spec(col1, timeout, person)(_.copy(age = 66))
        }

        "a person with the slow connection" in {
          val person = jane3.copy(age = 33) // as after previous upsert test

          spec(slowCol1, slowTimeout, person)(_.copy(age = 66))
        }
      }
      section("gt_mongo3")
    }
  }

  "WriteResult" should {
    val error = DefaultWriteResult(
      ok = false,
      n = 1,
      writeErrors = Nil,
      writeConcernError = None,
      code = Some(23),
      errmsg = Some("Foo"))

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

sealed trait UpdateFixtures { _: UpdateSpec =>
  case class Person(
    firstName: String,
    lastName: String,
    age: Int)

  object Person {
    implicit val personReader: BSONDocumentReader[Person] =
      BSONDocumentReader[Person] { doc: BSONDocument =>
        Person(
          doc.getAs[String]("firstName").getOrElse(""),
          doc.getAs[String]("lastName").getOrElse(""),
          doc.getAs[Int]("age").getOrElse(0))
      }

    implicit val personWriter: BSONDocumentWriter[Person] =
      BSONDocumentWriter[Person] { person: Person =>
        BSONDocument(
          "firstName" -> person.firstName,
          "lastName" -> person.lastName,
          "age" -> person.age)
      }
  }

  case class Person3(
    firstName: String,
    lastName: String,
    age: Int,
    score: BigDecimal) // Mongo +3.4

  object Person3 {
    implicit val personReader: BSONDocumentReader[Person3] =
      BSONDocumentReader[Person3] { doc: BSONDocument =>
        Person3(
          doc.getAs[String]("firstName").getOrElse(""),
          doc.getAs[String]("lastName").getOrElse(""),
          doc.getAs[Int]("age").getOrElse(0),
          doc.getAs[BigDecimal]("score").getOrElse(BigDecimal(-1L)))
      }

    implicit val personWriter: BSONDocumentWriter[Person3] =
      BSONDocumentWriter[Person3] { person: Person3 =>
        BSONDocument(
          "firstName" -> person.firstName,
          "lastName" -> person.lastName,
          "age" -> person.age,
          "score" -> person.score)
      }
  }
}
