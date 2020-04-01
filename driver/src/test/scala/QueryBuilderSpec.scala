package reactivemongo

import reactivemongo.api.{
  Collation,
  Collection,
  FailoverStrategy,
  PackSupport,
  ReadConcern,
  ReadPreference
}
import reactivemongo.api.collections.{ HintFactory, QueryBuilderFactory }

import reactivemongo.api.bson.BSONDocument

import reactivemongo.api.tests, tests.Pack

final class QueryBuilderSpec extends org.specs2.mutable.Specification { specs =>
  "Query builder" title

  section("unit")

  import Factory.QueryBuilder
  lazy val b = new QueryBuilder(Factory.dummyCollection)

  "Option" should {
    "be set as" >> {
      "sort" in {
        b.sort must beNone and {
          b.sort(BSONDocument("foo" -> 1)).
            sort must beSome(BSONDocument("foo" -> 1))
        }
      }

      "projection" in {
        b.projection must beNone and {
          b.projection(BSONDocument("bar" -> 1)).
            projection must beSome(BSONDocument("bar" -> 1))
        }
      }

      "hint" in {
        b.hint must beNone and {
          val h = Factory.hint(BSONDocument("foo" -> 1))

          b.hint(h).hint must beSome(h)
        }
      }

      "explain" in {
        b.explain must beFalse and {
          b.explain(true).explain must beTrue
        }
      }

      "snapshot" in {
        b.snapshot must beFalse and {
          b.snapshot(true).snapshot must beTrue
        }
      }

      "comment" in {
        b.comment must beNone and {
          b.comment("Foo").comment must beSome("Foo")
        }
      }

      "maxTimeMs" in {
        b.maxTimeMs must beNone and {
          b.maxTimeMs(1234L).maxTimeMs must beSome(1234L)
        }
      }

      "readConcern" in {
        b.readConcern must_=== ReadConcern.Local and {
          b.readConcern(ReadConcern.Majority).
            readConcern must_=== ReadConcern.Majority
        }
      }

      "singleBatch" in {
        b.singleBatch must beFalse and {
          b.singleBatch(true).singleBatch must beTrue
        }
      }

      "maxScan" in {
        b.maxScan must beNone and {
          b.maxScan(1.23D).maxScan must beSome(1.23D)
        }
      }

      "returnKey" in {
        b.returnKey must beFalse and {
          b.returnKey(true).returnKey must beTrue
        }
      }

      "showRecordId" in {
        b.showRecordId must beFalse and {
          b.showRecordId(true).showRecordId must beTrue
        }
      }

      "max" in {
        b.max must beNone and {
          b.max(BSONDocument("bar" -> 1)).
            max must beSome(BSONDocument("bar" -> 1))
        }
      }

      "min" in {
        b.min must beNone and {
          b.min(BSONDocument("bar" -> 1)).
            min must beSome(BSONDocument("bar" -> 1))
        }
      }

      "collation" in {
        val colla = new Collation(
          locale = "en-US",
          caseLevel = None,
          caseFirst = None,
          strength = Some(Collation.PrimaryStrength),
          numericOrdering = None,
          alternate = None,
          maxVariable = None,
          backwards = None)

        b.collation must beNone and {
          b.collation(colla).collation must beSome(colla)
        }
      }

      "batchSize" in {
        b.batchSize must_=== 0 and {
          b.batchSize(123).batchSize must_=== 123
        }
      }

      "skip" in {
        b.skip must_=== 0 and {
          b.skip(456).skip must_=== 456
        }
      }
    }
  }

  section("unit")

  "Merge" should {
    lazy val coll = _root_.tests.Common.db(
      s"querybuilder${System identityHashCode this}")

    "write minimal query" in {
      val builder = coll.find(
        BSONDocument("username" -> "John Doe"),
        Option.empty[BSONDocument])

      val expected = BSONDocument(
        "find" -> coll.name,
        "skip" -> 0,
        "tailable" -> false,
        "awaitData" -> false,
        "oplogReplay" -> false,
        "noCursorTimeout" -> false,
        "allowPartialResults" -> false,
        "singleBatch" -> false,
        "returnKey" -> false,
        "showRecordId" -> false,
        "filter" -> BSONDocument("username" -> "John Doe"),
        "limit" -> 10, // maxDocs
        "readConcern" -> BSONDocument("level" -> "local"),
        f"$$readPreference" -> BSONDocument("mode" -> "primary"))

      builder.merge(ReadPreference.Primary, 10) must_=== expected
    }

    "write with more options" >> {
      lazy val builder1 = coll.find(
        BSONDocument("username" -> "John Doe"), Option.empty[BSONDocument]).
        sort(BSONDocument("age" -> 1)).
        hint(coll.hint(BSONDocument("foo" -> 1)))

      lazy val expected1 = BSONDocument(
        "find" -> coll.name,
        "skip" -> 0,
        "tailable" -> false,
        "awaitData" -> false,
        "oplogReplay" -> false,
        "noCursorTimeout" -> false,
        "allowPartialResults" -> false,
        "singleBatch" -> false,
        "returnKey" -> false,
        "showRecordId" -> false,
        "filter" -> BSONDocument("username" -> "John Doe"),
        "limit" -> 11,
        "sort" -> BSONDocument("age" -> 1),
        "hint" -> BSONDocument("foo" -> 1),
        "readConcern" -> BSONDocument("level" -> "local"),
        f"$$readPreference" -> BSONDocument("mode" -> "primary"))

      "with query builder #1" in {
        builder1.merge(ReadPreference.Primary, 11) must_=== expected1
      }

      "with query builder #2" in {
        val c = "get john doe users sorted by age"
        val builder2 = builder1.comment(c)

        val expected = expected1 ++ ("comment" -> c, "limit" -> 12)

        builder2.merge(ReadPreference.Primary, 12) must_=== expected
      }
    }
  }

  // ---

  object Factory extends PackSupport[Pack]
    with HintFactory[Pack] with QueryBuilderFactory[Pack] {

    val pack = reactivemongo.api.tests.pack

    val dummyCollection = new Collection {
      def db: reactivemongo.api.DB = ???
      def name = "tests"

      private[reactivemongo] def failoverStrategy: FailoverStrategy =
        FailoverStrategy.default
    }
  }
}
