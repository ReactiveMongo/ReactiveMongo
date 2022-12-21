import scala.concurrent._
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api._
import reactivemongo.api.TestCompat._
import reactivemongo.api.bson.{ BSONDocument, BSONInteger, BSONString }
import reactivemongo.api.tests.{ builder, decoder }

import org.specs2.concurrent.ExecutionEnv

import _root_.tests.Common

final class CommonUseCases(implicit ee: ExecutionEnv)
    extends org.specs2.mutable.Specification
    with org.specs2.specification.AfterAll {

  "Common use cases".title

  sequential
  stopOnFail

  // ---

  import Common.{ timeout, slowTimeout }
  import builder.regex

  lazy val (db, slowDb) = Common.databases(
    s"reactivemongo-usecases-${System identityHashCode this}",
    Common.connection,
    Common.slowConnection,
    retries = 1
  )

  val colName = s"commonusecases${System identityHashCode this}"
  lazy val collection = db(colName)
  lazy val slowColl = slowDb(colName)

  def afterAll() = { db.drop(); () }

  // ---

  "ReactiveMongo" should {
    "create a collection" in {
      collection.create() must beTypedEqualTo({}).await(1, timeout)
    }

    "insert some documents" in eventually(2, timeout / 2L) {
      val docs =
        (18 to 60).map(i => BSONDocument("age" -> i, "name" -> s"Jack${i}"))

      collection.delete.one(BSONDocument.empty).flatMap { _ =>
        collection.count(Option.empty[BSONDocument])
      } must beTypedEqualTo(0L).awaitFor(timeout) and {
        (for {
          _ /*result*/ <- collection.insert(ordered = true).many(docs)
          count <- collection.count(
            Some(
              BSONDocument(
                "age" -> BSONDocument(f"$$gte" -> 18, f"$$lte" -> 60)
              )
            )
          )
        } yield count) must beTypedEqualTo(43L).awaitFor(timeout)
      }
    }

    "find them" in {
      // batchSize (>1) allows us to test cursors ;)
      val it = collection
        .find(BSONDocument())
        .batchSize(2)
        .sort(BSONDocument("age" -> 1))
        .cursor[BSONDocument]()

      // import reactivemongo.core.protocol.{ Response, Reply }
      // import reactivemongo.api.tests.{ makeRequest => req, nextResponse }

      it.collect[List](Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]())
        .map(
          _.map { doc => decoder.int(doc, "age").mkString }.mkString("")
        ) must beTypedEqualTo((18 to 60).mkString("")).await(1, timeout * 2)

    }

    "find by regexp" in {
      collection
        .find(BSONDocument("name" -> regex("ack2", "")))
        .cursor[BSONDocument]()
        .collect[List](Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]())
        .map(_.size) must beTypedEqualTo(10).awaitFor(timeout)
    }

    "find by regexp with flag" in {
      val query =
        BSONDocument(
          f"$$or" -> BSONArray(
            BSONDocument("name" -> regex("^jack2", "i")),
            BSONDocument("name" -> regex("^jack3", "i"))
          )
        )

      collection
        .find(query)
        .cursor[BSONDocument]()
        .collect[List](Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]())
        .map(_.size) aka "size" must beTypedEqualTo(20).await(1, timeout)
    }

    "find them with a projection" >> {
      val pjn = BSONDocument("name" -> 1, "age" -> 1, "something" -> 1)
      val expected = (18 to 60).mkString("")

      def findSpec(c: DefaultCollection, t: FiniteDuration) = {
        def it = c
          .find(BSONDocument.empty, Some(pjn))
          .batchSize(2)
          .cursor[BSONDocument]()

        // import reactivemongo.core.protocol.{ Response, Reply }
        // import reactivemongo.api.tests.{ makeRequest => req, nextResponse }

        it.collect[List](Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]())
          .map {
            _.map(doc => decoder.int(doc, "age").mkString).mkString("")
          } must beTypedEqualTo(expected).await(1, t)
      }

      "with the default connection" in {
        findSpec(collection, timeout)
      }

      "with the slow connection" in eventually(2, timeout) {
        val t = slowTimeout + (timeout / 2L)

        findSpec(slowColl, Common.ifX509(t * 5)(t * 3))
      }
    }

    "insert a document containing merged objects, fetch and check it" in {
      val array = BSONArray(
        BSONDocument(
          "entry" -> BSONInteger(1),
          "type" -> BSONString("telephone"),
          "professional" -> BSONBoolean(true),
          "value" -> BSONString("+331234567890")
        )
      )

      val array2 = BSONArray(
        BSONDocument(
          "entry" -> BSONInteger(2),
          "type" -> BSONString("mail"),
          "professional" -> BSONBoolean(true),
          "value" -> BSONString("joe@plop.com")
        )
      )

      val doc = BSONDocument(
        "name" -> BSONString("Joe"),
        "contacts" -> (array ++ array2)
      )

      collection.insert.one(doc).flatMap { _ =>
        collection.find(BSONDocument("name" -> "Joe")).one[BSONDocument]
      } must beSome[BSONDocument].which { fetched =>
        val contactsString = decoder
          .children(fetched, "contacts")
          .map { c =>
            decoder.string(c, "type").mkString + ":" +
              decoder.string(c, "value").mkString
          }
          .mkString(",")

        contactsString must_=== "telephone:+331234567890,mail:joe@plop.com"
      }.awaitFor(timeout)
    }

    "insert a weird doc" in {
      val doc = BSONDocument(
        "coucou" -> BSONString("coucou"),
        "plop" -> BSONInteger(1),
        "plop" -> BSONInteger(2)
      )

      collection.insert.one(doc).map(_ => {}) must beTypedEqualTo({})
        .await(1, timeout)
    }

    "find this weird doc" in {
      collection
        .find(BSONDocument("coucou" -> BSONString("coucou")))
        .one[BSONDocument] must beSome.await(1, timeout)
    }

    "fail with this error" in {
      val query = BSONDocument(
        f"$$and" ->
          BSONDocument("name" -> BSONString("toto"))
      )

      Await
        .result(collection.find(query).one[BSONDocument], timeout)
        .aka("findOne") must throwA[Exception]
    }
  }
}
