import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import reactivemongo.bson.{
  BSON,
  BSONDocument,
  BSONDocumentReader,
  BSONDocumentWriter
}

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands._

import reactivemongo.api.collections.bson.BSONCollection

import org.specs2.concurrent.ExecutionEnv

final class FindAndModifySpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification { // with FindAndModifyFixtures {

  "FindAndModify" title

  import tests.Common
  import Common._

  "Raw findAndModify" should {
    type FindAndModifyResult = FindAndModifyCommand.Result[BSONSerializationPack.type]

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

    val jack1 = Person("Jack", "London", 27)
    val jack2 = jack1.copy(age = /* updated to */ 40)

    "upsert a doc and fetch it" >> {
      def upsertAndFetch(
        c: BSONCollection,
        p: Person,
        age: Int,
        timeout: FiniteDuration)(
        check: FindAndModifyResult => org.specs2.matcher.MatchResult[Any]) = {

        val after = p.copy(age = age)

        c.findAndUpdate(
          selector = p,
          update = BSONDocument(f"$$set" -> BSONDocument("age" -> age)),
          fetchNewObject = true,
          upsert = true,
          sort = Option.empty,
          fields = Option.empty,
          bypassDocumentValidation = false,
          writeConcern = WriteConcern.Default,
          maxTime = Option.empty,
          collation = Option.empty,
          arrayFilters = Seq.empty).
          aka("result") must (beLike[FindAndModifyResult] {
            case result =>
              //println(s"FindAndModify#0: $age -> ${result.lastError}")

              result.lastError.map(_.n) must beSome(1) and check(result) and {
                result.result[Person] must beSome[Person](after)
              } and {
                //println(s"FindAndModify#1{$age}: ${c.name}")

                /*c.find(BSONDocument.empty).cursor[BSONDocument]().
                  collect[List]().flatMap { docs =>
                    println(s"FindAndModify#2: docs = ${docs.map(BSONDocument.pretty)}")
                    c.count(Some(BSON.writeDocument(after)))
                  }.*/
                c.count(Some(BSON.writeDocument(after))).
                  aka("count after") must beTypedEqualTo(1).await(1, timeout)
              }
          }).await(1, timeout)
      }

      "with the default connection" in {
        val after = jack1.copy(age = 37)
        val colName = s"FindAndModifySpec${System identityHashCode this}-1"
        val collection = db(colName)

        collection.count(Some(BSON.writeDocument(jack1))).
          aka("count before") must beTypedEqualTo(0).await(1, timeout) and {
            upsertAndFetch(collection, jack1, after.age, timeout) { result =>
              result.lastError.exists(_.upsertedId.isDefined) must beTrue
            }
          }
      }

      "with the slow connection" in {
        val before = jack1
        val after = jack2
        val colName = s"FindAndModifySpec${System identityHashCode this}-2"
        val slowColl = slowDb(colName)

        slowColl.insert(ordered = true).one(before).
          map(_.n) must beTypedEqualTo(1).await(0, slowTimeout) and {
            slowColl.count(Some(BSON.writeDocument(before))).
              aka("count before") must beTypedEqualTo(1).await(1, slowTimeout)
          } and eventually(2, timeout) {
            upsertAndFetch(
              slowColl, before, after.age, slowTimeout) { result =>
              result.lastError.exists(_.upsertedId.isDefined) must beFalse
            }
          }
      }
    }

    val colName = s"FindAndModifySpec${System identityHashCode this}-3"

    "modify a document and fetch its previous value" in {
      val incrementAge = BSONDocument(f"$$inc" -> BSONDocument("age" -> 1))
      val collection = db(colName)

      def future = collection.findAndUpdate(jack2, incrementAge)

      collection.insert(ordered = true).one(jack2).
        map(_.n) must beTypedEqualTo(1).await(0, slowTimeout) and {
          future must (beLike[FindAndModifyResult] {
            case res =>
              /*
              scala.concurrent.Await.result(collection.find(BSONDocument.empty).
                cursor[BSONDocument]().collect[List]().map { docs =>
                  println(s"persons[${collection.name}]#3{${jack2}} = ${docs.map(BSONDocument.pretty)}")
                }, timeout)
               */

              res.result[Person] aka "previous value" must beSome(jack2)
          }).await(1, timeout)
        } and {
          collection.find(jack2.copy(age = jack2.age + 1)).one[Person].
            aka("incremented") must beSome[Person].await(1, timeout)
        }
    }

    if (replSetOn) {
      section("ge_mongo4")

      val james = Person("James", "Joyce", 27)

      "support transaction & rollback" in {
        val selector = BSONDocument(
          "firstName" -> james.firstName,
          "lastName" -> james.lastName)

        (for {
          _ <- db.collection(colName).create(failsIfExists = false)
          ds <- db.startSession().flatMap {
            case Some(d) => Future.successful(d)
            case _       => Future.failed(new Exception("Fails to start session"))
          }
          dt <- ds.startTransaction(None) match {
            case Some(d) => Future.successful(d)
            case _       => Future.failed(new Exception("Fails to start TX"))
          }

          coll = dt.collection(colName)
          _ <- coll.findAndUpdate(selector, james, upsert = true)
          p1 <- coll.find(selector).requireOne[Person].map(_.age)

          _ <- dt.abortTransaction().collect {
            case Some(d) => d
          }
          p2 <- coll.find(selector).one[Person].map(_.fold(-1)(_.age))
        } yield p1 -> p2).andThen {
          case _ => db.killSession()
        } must beLike[(Int, Int)] {
          case (27, -1 /*not found after rlb*/ ) => ok
        }.await(1, timeout)
      }

      section("ge_mongo4")
    }

    "support arrayFilters" in {
      // See https://docs.mongodb.com/manual/reference/method/db.collection.findAndModify/#findandmodify-arrayfilters

      val colName = s"FindAndModifySpec${System identityHashCode this}-5"
      val collection = db(colName)

      collection.insert.many(Seq(
        BSONDocument("_id" -> 1, "grades" -> Seq(95, 92, 90)),
        BSONDocument("_id" -> 2, "grades" -> Seq(98, 100, 102)),
        BSONDocument("_id" -> 3, "grades" -> Seq(95, 110, 100)))).
        map(_ => {}) must beTypedEqualTo({}).await(0, timeout) and {
          collection.findAndModify(
            selector = BSONDocument("grades" -> BSONDocument(f"$$gte" -> 100)),
            modifier = collection.updateModifier(
              update = BSONDocument(f"$$set" -> BSONDocument(
                f"grades.$$[element]" -> 100)),
              fetchNewObject = true,
              upsert = false),
            sort = None,
            fields = None,
            bypassDocumentValidation = false,
            writeConcern = WriteConcern.Journaled,
            maxTime = None,
            collation = None,
            arrayFilters = Seq(
              BSONDocument("element" -> BSONDocument(f"$$gte" -> 100)))).
            map(_.value) must beSome(BSONDocument(
              "_id" -> 2,
              "grades" -> Seq(98, 100, 100 /* was 102*/ ))).await(0, timeout)
        }
    } tag "gt_mongo32"

    f"fail with invalid $$inc clause" in {
      val colName = s"FindAndModifySpec${System identityHashCode this}-5"
      val collection = db(colName)

      val query = BSONDocument("FindAndModifySpecFail" -> BSONDocument(
        f"$$exists" -> false))

      val future = collection.insert(ordered = true).one(jack2).flatMap { _ =>
        collection.findAndUpdate(
          selector = query,
          update = BSONDocument(f"$$inc" -> "age"),
          fetchNewObject = false,
          upsert = false,
          sort = Option.empty,
          fields = Option.empty,
          bypassDocumentValidation = false,
          writeConcern = WriteConcern.Default,
          maxTime = Option.empty,
          collation = Option.empty,
          arrayFilters = Seq.empty)
      }

      future.map(_ => Option.empty[Int]).recover {
        case e: CommandError =>
          //e.printStackTrace
          e.code
      } must beSome( /*code = */ 9).await(1, timeout)
    }
  }
}
