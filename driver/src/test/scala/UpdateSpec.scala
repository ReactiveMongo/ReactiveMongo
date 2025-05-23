import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.ReadPreference
import reactivemongo.api.bson.{ BSONDocument, BSONString }
import reactivemongo.api.commands.WriteResult
import reactivemongo.api.tests.{ builder, decoder, pack, reader, writer }

import _root_.tests.Common

trait UpdateSpec extends UpdateFixtures { collectionSpec: CollectionSpec =>
  import reactivemongo.api.TestCompat._
  import Common._

  private lazy val updCol1 = db(s"update1${System.identityHashCode(db)}")

  private lazy val slowUpdCol1 = slowDb(
    s"slowup1${System.identityHashCode(db)}"
  )
  private lazy val updCol2 = db(s"update2${System.identityHashCode(slowDb)}")

  private lazy val slowUpdCol2 = slowDb(
    s"slowup2${System.identityHashCode(slowDb)}"
  )

  def updateSpecs = {
    // with fixtures ...
    val jack3 = Person3("Jack", "London", 27, BigDecimal("12.345"))
    val jane3 = Person3("Jane", "London", 18, BigDecimal("3.45"))

    {
      def spec[T](
          c: DefaultCollection,
          timeout: FiniteDuration,
          f: => T
        )(upd: T => T
        )(implicit
          w: pack.Writer[T],
          r: pack.Reader[T]
        ) = {
        val person = f

        c.update.one(
          q = person,
          u = BSONDocument(f"$$set" -> BSONDocument("age" -> 33)),
          upsert = true
        ) must beLike[c.UpdateWriteResult]({
          case result =>
            result.upserted.toList must beLike[List[c.Upserted]] {
              case c.Upserted(0, id: BSONObjectID) :: Nil =>
                eventually(2, timeout) {
                  c.find(
                    selector = BSONDocument("_id" -> id),
                    projection = Option.empty[BSONDocument]
                  ).one[T] must beSome(upd(person)).await(1, timeout)
                }
            }
        }).await(1, timeout)
      }

      section("gt_mongo32")
      "upsert with MongoDB 3.4+" >> {
        "a person with the default connection" in {
          spec(updCol1, timeout, jack3)(_.copy(age = 33))
        }

        "a person with the slow connection and Secondary preference" in {
          val coll =
            slowUpdCol1.withReadPreference(ReadPreference.secondaryPreferred)

          coll.readPreference must_=== ReadPreference.secondaryPreferred and {
            spec(coll, slowTimeout, jane3)(_.copy(age = 33))
          }
        }
      }
      section("gt_mongo32")
    }

    "upsert a document" >> {
      def spec(c: DefaultCollection, timeout: FiniteDuration) = {
        val doc = BSONDocument("_id" -> "foo", "bar" -> 2)

        c.update
          .one(q = BSONDocument.empty, u = doc, upsert = true)
          .map(_.upserted.toList) must beLike[List[c.Upserted]] {
          case c.Upserted(0, BSONString("foo")) :: Nil =>
            c.find(
              selector = BSONDocument("_id" -> "foo"),
              projection = Option.empty[BSONDocument]
            ).one[BSONDocument]
              .aka("found") must beSome(doc).await(1, timeout)
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

    "update many documents" >> {
      "with the default connection" in {
        val coll = db(s"update3${System.identityHashCode(db)}")
        val update = coll.update.maxBulkSize(2)

        // Fixtures
        val doc1 = BSONDocument("_id" -> 1)
        val doc2 = BSONDocument("_id" -> 2)
        val doc3 = BSONDocument("_id" -> 3)

        def resultSpec(res: Future[coll.MultiBulkWriteResult]) =
          res must beLike[coll.MultiBulkWriteResult] {
            case result =>
              result.n must_=== 3 and {
                result.nModified must_=== 0 // upserted
              } and {
                coll
                  .find(BSONDocument.empty)
                  .cursor[BSONDocument]()
                  .collect[Set]() must beTypedEqualTo(Set(doc1, doc2, doc3))
                  .await(2, timeout)
              }
          }.awaitFor(timeout)

        update.maxBulkSize must_=== 2 and (eventually(2, timeout) {
          Future
            .sequence(
              Seq(
                update.element(doc1, doc1, upsert = true),
                update.element(doc2, doc2, upsert = true),
                update.element(doc3, doc3, upsert = true)
              )
            )
            .aka("elements") must beLike[Seq[coll.UpdateElement]] {
            case elements @ (up1 +: _) =>
              elements.size must_=== 3 and {
                resultSpec(update.many(elements))
              } and {
                // Clean collection
                coll.delete
                  .one(BSONDocument.empty)
                  .map(_.n) must beTypedEqualTo(3).awaitFor(timeout)
              } and {
                coll.count() must beTypedEqualTo(0L).await(1, timeout)
              } and {
                resultSpec(update.many(up1, elements.drop(1)))
              }
          }.awaitFor(timeout / 2L * 4L)
        })
      }
    }

    {
      def spec[T](
          c: DefaultCollection,
          timeout: FiniteDuration,
          f: => T
        )(upd: T => T
        )(implicit
          w: c.pack.Writer[T],
          r: c.pack.Reader[T]
        ) = {
        val person = f
        val update = c.update

        import c.AggregationFramework._

        val res: Future[c.UpdateWriteResult] = update.one[T](
          q = person,
          u = List(Set(BSONDocument("age" -> 66))),
          upsert = false,
          multi = false,
          collation = None,
          arrayFilters = Seq.empty
        )

        res must beLike[c.UpdateWriteResult] {
          case result =>
            result.nModified must_=== 1 and {
              c.find(
                selector = BSONDocument("age" -> 66),
                projection = Option.empty[BSONDocument]
              ).one[T] must beSome(upd(person)).await(1, timeout)
            }
        }.await(1, timeout)
      }

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

          val colName = s"UpdateSpec${System.identityHashCode(this)}-5"
          val collection = db(colName)

          collection.insert
            .many(
              Seq(
                BSONDocument("_id" -> 1, "grades" -> Seq(95, 92, 90)),
                BSONDocument("_id" -> 2, "grades" -> Seq(98, 100, 102)),
                BSONDocument("_id" -> 3, "grades" -> Seq(95, 110, 100))
              )
            )
            .map(_ => {}) must beTypedEqualTo({}).await(0, timeout) and {
            collection.update
              .one(
                q = BSONDocument("grades" -> BSONDocument(f"$$gte" -> 100)),
                u = BSONDocument(
                  f"$$set" -> BSONDocument(f"grades.$$[element]" -> 100)
                ),
                upsert = false,
                multi = true,
                collation = None,
                arrayFilters =
                  Seq(BSONDocument("element" -> BSONDocument(f"$$gte" -> 100)))
              )
              .map(_.n) must beTypedEqualTo(2).await(0, timeout)
          }
        }
      }
      section("gt_mongo32")
    }
  }
}

sealed trait UpdateFixtures { _self: UpdateSpec with CollectionSpec =>

  case class Person3(
      firstName: String,
      lastName: String,
      age: Int,
      score: BigDecimal) // Mongo +3.4

  object Person3 {

    implicit lazy val personReader: pack.Reader[Person3] =
      reader[Person3] { (doc: pack.Document) =>
        Person3(
          decoder.string(doc, "firstName").getOrElse(""),
          decoder.string(doc, "lastName").getOrElse(""),
          decoder.int(doc, "age").getOrElse(0),
          decoder.double(doc, "score").fold(BigDecimal(-1L))(BigDecimal(_))
        )
      }

    implicit lazy val personWriter: pack.Writer[Person3] = {
      import builder.{ elementProducer => e, string }

      writer[Person3] { (person: Person3) =>
        builder.document(
          Seq(
            e("firstName", string(person.firstName)),
            e("lastName", string(person.lastName)),
            e("age", builder.int(person.age)),
            e("score", builder.double(person.score.doubleValue))
          )
        )
      }
    }
  }
}
