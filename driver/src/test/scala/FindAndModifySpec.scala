import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.WriteConcern
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.commands.CommandException
import reactivemongo.api.tests.{ builder, decoder, pack, reader, writer }

import org.specs2.concurrent.ExecutionEnv

final class FindAndModifySpec(implicit ee: ExecutionEnv)
    extends org.specs2.mutable.Specification {

  "FindAndModify".title

  import reactivemongo.api.TestCompat._
  import tests.Common, Common._

  "Raw findAndModify" should {
    case class Person(
        firstName: String,
        lastName: String,
        age: Int)

    implicit val PersonReader = reader[Person] { doc =>
      Person(
        decoder.string(doc, "firstName").getOrElse(""),
        decoder.string(doc, "lastName").getOrElse(""),
        decoder.int(doc, "age").getOrElse(0)
      )
    }

    implicit val PersonWriter = writer[Person] { person =>
      import builder.{ elementProducer => e }

      builder.document(
        Seq(
          e("firstName", builder.string(person.firstName)),
          e("lastName", builder.string(person.lastName)),
          e("age", builder.int(person.age))
        )
      )
    }

    def writeDocument[T](value: T)(implicit w: pack.Writer[T]) =
      pack.serialize(value, w)

    val jack1 = Person("Jack", "London", 27)
    val jack2 = jack1.copy(age = /* updated to */ 40)

    "upsert a doc and fetch it" >> {
      def upsertAndFetch(
          c: DefaultCollection,
          p: Person,
          age: Int,
          timeout: FiniteDuration
        )(check: c.FindAndModifyResult => org.specs2.matcher.MatchResult[Any]
        ) = {

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
          arrayFilters = Seq.empty
        ).aka("result") must (beLike[c.FindAndModifyResult] {
          case result =>
            // println(s"FindAndModify#0: $age -> ${result.lastError}")

            result.lastError.map(_.n) must beSome(1) and check(result) and {
              result.result[Person] must beSome[Person](after)
            } and {
              // println(s"FindAndModify#1{$age}: ${c.name}")

              /*c.find(BSONDocument.empty).cursor[BSONDocument]().
                  collect[List]().flatMap { docs =>
                    println(s"FindAndModify#2: docs = ${docs.map(BSONDocument.pretty)} / $after")
                    c.count(Some(writeDocument(after)))
                 }*/
              c.count(Some(writeDocument(after)))
                .aka("count after") must beTypedEqualTo(1L).await(1, timeout)
            }
        }).await(1, timeout)
      }

      "with the default connection" in {
        val after = jack1.copy(age = 37)

        eventually(2, timeout) {
          val colName =
            s"fam-${System identityHashCode this}-1-${System.currentTimeMillis()}"
          val collection = db(colName)

          collection
            .count(Some(writeDocument(jack1)))
            .aka("count before") must beTypedEqualTo(0L).await(1, timeout) and {
            upsertAndFetch(collection, jack1, after.age, timeout) {
              _.lastError.exists(_.upserted.isDefined) must beTrue
            }
          }
        }
      }

      "with the slow connection" in {
        val before = jack1
        val after = jack2

        eventually(2, timeout) {
          val colName =
            s"fam-${System identityHashCode this}-2-${System.currentTimeMillis()}"
          val slowColl = slowDb(colName)

          slowColl.update
            .one(before, before, upsert = true)
            .map(_.n) must beTypedEqualTo(1).awaitFor(slowTimeout) and {
            db(colName).count(None) must beTypedEqualTo(1L).awaitFor(
              slowTimeout
            )

          } and {
            upsertAndFetch(slowColl, before, after.age, slowTimeout) {
              _.lastError.exists(_.upserted.isDefined) must beFalse
            }
          }
        }
      }
    }

    section("ge_mongo4")

    "modify a document and fetch its previous value" in {
      eventually(2, timeout) {
        val colName =
          s"fam${System identityHashCode this}-3-${System.currentTimeMillis()}"
        val collection = db(colName)

        val incrementAge = {
          import collection.AggregationFramework.{ PipelineOperator, Set }

          List[PipelineOperator](
            Set(
              BSONDocument(
                "age" -> BSONDocument(f"$$add" -> BSONArray(f"$$age", 1))
              )
            )
          )
        }

        def findAndUpd =
          collection.findAndUpdateWithPipeline(jack2, incrementAge)

        collection.update
          .one(jack2, jack2, upsert = true)
          .map(_.n) must beTypedEqualTo(1).await(1, timeout) and {
          collection.count(
            Some(
              BSONDocument(
                "firstName" -> jack2.firstName,
                "lastName" -> jack2.lastName
              )
            )
          ) must beTypedEqualTo(1L).await(0, timeout)
        } and {
          findAndUpd must (beLike[collection.FindAndModifyResult] {
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
          collection
            .find(jack2.copy(age = jack2.age + 1))
            .one[Person]
            .aka("incremented") must beSome[Person].await(1, timeout)
        }
      }
    }

    if (replSetOn) {
      val james = Person("James", "Joyce", 27)

      "support transaction & rollback" in eventually(2, timeout) {
        val selector = BSONDocument(
          "firstName" -> james.firstName,
          "lastName" -> james.lastName
        )

        val colName =
          s"fam${System identityHashCode selector}-4-${System.currentTimeMillis()}"
        val coll = db.collection(colName)

        (for {
          _ <- coll.create(failsIfExists = false)
          ds <- db.startSession()
          dt <- ds.startTransaction(None)
          _ <- dt.collectionNames.filter(_ contains colName) // check created

          _ <- dt
            .collection(colName)
            .findAndUpdate(selector, james, upsert = true)

          _ <- dt
            .collection(colName)
            .findAndUpdate(
              selector ++ BSONDocument("age" -> 27),
              BSONDocument(f"$$set" -> BSONDocument("age" -> 35))
            )
        } yield dt) must beLike[reactivemongo.api.DB] {
          case dt =>
            val c = dt.collection(colName)

            (for {
              p1 <- c.find(selector).requireOne[Person].map(_.age)

              _ <- dt.abortTransaction()

              p2 <- coll.find(selector).one[Person].map(_.fold(-1)(_.age))
            } yield p1 -> p2).andThen {
              case _ => db.killSession()
            } must beLike[(Int, Int)] {
              case (35, -1 /*not found after rlb*/ ) => ok
            }.await(1, timeout)
        }.await(1, slowTimeout)
      }
    }

    section("ge_mongo4")

    "support arrayFilters" in {
      // See https://docs.mongodb.com/manual/reference/method/db.collection.findAndModify/#findandmodify-arrayfilters

      eventually(2, timeout) {
        val colName =
          s"FindAndModify${System identityHashCode this}-5-${System.currentTimeMillis()}"
        val collection = db(colName)

        collection.insert
          .many(
            Seq(
              BSONDocument("_id" -> 1, "grades" -> Seq(95, 92, 90)),
              BSONDocument("_id" -> 2, "grades" -> Seq(98, 100, 102)),
              BSONDocument("_id" -> 3, "grades" -> Seq(95, 110, 100))
            )
          )
          .map(_.n) must beTypedEqualTo(3).await(0, timeout) and {
          collection
            .findAndModify(
              selector =
                BSONDocument("grades" -> BSONDocument(f"$$gte" -> 100)),
              modifier = collection.updateModifier(
                update = BSONDocument(
                  f"$$set" -> BSONDocument(f"grades.$$[element]" -> 100)
                ),
                fetchNewObject = true,
                upsert = false
              ),
              sort = None,
              fields = None,
              bypassDocumentValidation = false,
              writeConcern = WriteConcern.Journaled,
              maxTime = None,
              collation = None,
              arrayFilters =
                Seq(BSONDocument("element" -> BSONDocument(f"$$gte" -> 100)))
            )
            .map(_.value) must beSome(
            BSONDocument(
              "_id" -> 2,
              "grades" -> Seq(98, 100, 100 /* was 102*/ )
            )
          ).await(0, timeout)
        }
      }
    } tag "gt_mongo32"

    f"fail with invalid $$inc clause" in eventually(2, timeout) {
      val colName =
        s"fam${System identityHashCode this}-6-${System.currentTimeMillis()}"
      val collection = db(colName)

      val query = BSONDocument(
        "FindAndModifySpecFail" -> BSONDocument(f"$$exists" -> false)
      )

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
          arrayFilters = Seq.empty
        )
      }

      future.map(_ => Option.empty[Int]).recover {
        case CommandException.Code(c) =>
          // e.printStackTrace
          Some(c)
      } must beSome( /*code = */ 9).await(1, (timeout * 2) + (timeout / 2))
    }
  }
}
