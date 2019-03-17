import scala.concurrent.duration.FiniteDuration

import reactivemongo.bson._

import reactivemongo.api.ReadPreference
import reactivemongo.api.collections.bson.BSONCollection

import reactivemongo.api.commands.{ UpdateWriteResult, WriteResult, Upserted }
import reactivemongo.api.commands.bson.BSONUpdateCommand._
import reactivemongo.api.commands.bson.BSONUpdateCommandImplicits._

import _root_.tests.Common

trait UpdateSpec extends UpdateFixtures { collectionSpec: CollectionSpec =>

  import Common._

  private lazy val updCol1 = db(s"update1${System identityHashCode db}")
  private lazy val slowUpdCol1 = slowDb(s"slowup1${System identityHashCode db}")
  private lazy val updCol2 = db(s"update2${System identityHashCode slowDb}")

  private lazy val slowUpdCol2 = slowDb(
    s"slowup2${System identityHashCode slowDb}")

  def updateSpecs = {
    implicit val personReader = PersonReader
    implicit val personWriter = PersonWriter

    // with fixtures ...
    val jack3 = Person3("Jack", "London", 27, BigDecimal("12.345"))
    val jack = Person("Jack London", 27)
    val jane3 = Person3("Jane", "London", 18, BigDecimal("3.45"))
    val jane = Person("Jack London", 18)

    {
      def spec[T: BSONDocumentWriter: BSONDocumentReader](c: BSONCollection, timeout: FiniteDuration, f: => T)(upd: T => T) = {
        val person = f

        c.update.one(
          q = person,
          u = BSONDocument("$set" -> BSONDocument("age" -> 33)),
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
          spec(updCol1, timeout, jack)(_.copy(age = 33))
        }

        "a person with the slow connection and Secondary preference" in {
          val coll = slowUpdCol1.withReadPreference(
            ReadPreference.secondaryPreferred)

          coll.readPreference must_== ReadPreference.secondaryPreferred and {
            spec(coll, slowTimeout, jane)(_.copy(age = 33))
          }
        }
      }
      section("mongo2", "mongo24", "not_mongo26")

      section("gt_mongo32")
      "upsert with MongoDB 3.4+" >> {
        "a person with the default connection" in {
          spec(updCol1, timeout, jack3)(_.copy(age = 33))
        }

        "a person with the slow connection and Secondary preference" in {
          val coll = slowUpdCol1.withReadPreference(
            ReadPreference.secondaryPreferred)

          coll.readPreference must_== ReadPreference.secondaryPreferred and {
            spec(coll, slowTimeout, jane3)(_.copy(age = 33))
          }
        }
      }
      section("gt_mongo32")
    }

    "upsert a document" >> {
      def spec(c: BSONCollection, timeout: FiniteDuration) = {
        val doc = BSONDocument("_id" -> "foo", "bar" -> 2)

        c.update.one(q = BSONDocument.empty, u = doc, upsert = true).
          map(_.upserted.toList) must beLike[List[Upserted]] {
            case Upserted(0, id @ BSONString("foo")) :: Nil =>
              c.find(BSONDocument("_id" -> id)).one[BSONDocument].
                aka("found") must beSome(doc).await(1, timeout)
          }.await(1, timeout) and {
            c.insert.one(doc).map(_ => true).recover {
              case WriteResult.Code(11000) => false
            } must beFalse.await(0, timeout)
          }
      }

      "with the default connection" in {
        spec(updCol2, timeout)
      }

      "with the slow connection" in {
        spec(slowUpdCol2, slowTimeout)
      }
    }

    "update a document directly using the Update command" in {
      val doc = BSONDocument("_id" -> "foo", "bar" -> 2)

      updCol2.runCommand(
        Update(UpdateElement(
          q = doc, u = BSONDocument("$set" -> BSONDocument("bar" -> 3)))),
        ReadPreference.primary) aka "result" must beLike[UpdateWriteResult]({
          case result => result.nModified must_== 1 and (
            updCol2.find(BSONDocument("_id" -> "foo")).one[BSONDocument].
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

          spec(updCol1, timeout, person)(_.copy(age = 66))
        }

        "a person with the slow connection" in {
          val person = jane.copy(age = 33) // as after previous upsert test

          spec(slowUpdCol1, slowTimeout, person)(_.copy(age = 66))
        }
      }
      section("mongo2", "mongo24", "not_mongo26")

      section("gt_mongo32")
      "update with MongoDB 3.4+" >> {
        "a person with the default connection" in {
          val person = jack3.copy(age = 33) // as after previous upsert test

          spec(updCol1, timeout, person)(_.copy(age = 66))
        }

        "a person with the slow connection" in {
          val person = jane3.copy(age = 33) // as after previous upsert test

          spec(slowUpdCol1, slowTimeout, person)(_.copy(age = 66))
        }

        "support arrayFilters" in {
          // See https://docs.mongodb.com/manual/reference/command/update/#update-elements-match-arrayfilters-criteria

          val colName = s"UpdateSpec${System identityHashCode this}-5"
          val collection = db(colName)

          collection.insert.many(Seq(
            BSONDocument("_id" -> 1, "grades" -> Seq(95, 92, 90)),
            BSONDocument("_id" -> 2, "grades" -> Seq(98, 100, 102)),
            BSONDocument("_id" -> 3, "grades" -> Seq(95, 110, 100)))).
            map(_ => {}) must beTypedEqualTo({}).await(0, timeout) and {
              collection.update.one(
                q = BSONDocument("grades" -> BSONDocument(f"$$gte" -> 100)),
                u = BSONDocument(f"$$set" -> BSONDocument(
                  f"grades.$$[element]" -> 100)),
                upsert = false,
                multi = true,
                collation = None,
                arrayFilters = Seq(
                  BSONDocument("element" -> BSONDocument(f"$$gte" -> 100)))).
                map(_.n) must beTypedEqualTo(2).await(0, timeout)
            }
        } tag "wip"
      }
      section("gt_mongo32")
    }
  }
}

sealed trait UpdateFixtures { _: UpdateSpec =>
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
