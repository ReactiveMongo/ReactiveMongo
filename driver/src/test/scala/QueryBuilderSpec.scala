import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.api.{
  Collation,
  FailoverStrategy,
  QueryOpts,
  ReadPreference
}
import reactivemongo.api.collections.GenericQueryBuilder

import reactivemongo.api.bson.BSONDocument

import reactivemongo.api.tests

final class QueryBuilderSpec extends org.specs2.mutable.Specification { specs =>
  "Query builder" title

  section("unit")

  lazy val b = new TestQueryBuilder()

  "Boolean flag" should {
    "be set for" >> {
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

      "singleBatch" in {
        b.singleBatch must beFalse and {
          b.singleBatch(true).singleBatch must beTrue
        }
      }
    }
  }

  "Collation" should {
    "be set" in {
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
  }

  "Document option" should {
    "be set for" >> {
      "max" in {
        b.max must beNone and {
          val doc = BSONDocument("max" -> 1)
          b.max(doc).max must beSome(doc)
        }
      }

      "min" in {
        b.min must beNone and {
          val doc = BSONDocument("min" -> 2)
          b.min(doc).min must beSome(doc)
        }
      }
    }
  }

  "Numeric option" should {
    "be set for" >> {
      "maxScan" in {
        b.maxScan must beNone and {
          b.maxScan(1.23D).maxScan must beSome(1.23D)
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

      tests.merge(
        builder, ReadPreference.Primary, 10) must_=== expected
    }

    "write with more options" >> {
      lazy val builder1: GenericQueryBuilder[pack.type] = coll.find(
        BSONDocument("username" -> "John Doe"), Option.empty[BSONDocument]).
        sort(BSONDocument("age" -> 1))

      val expected1 = BSONDocument(
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
        "readConcern" -> BSONDocument("level" -> "local"),
        f"$$readPreference" -> BSONDocument("mode" -> "primary"))

      "with query builder #1" in {
        tests.merge(
          builder1, ReadPreference.Primary, 11) must_=== expected1
      }

      "with query builder #2" in {
        val c = "get john doe users sorted by age"
        val builder2 = builder1.comment(c)

        val expected = expected1 ++ ("comment" -> c, "limit" -> 12)

        tests.merge(
          builder2, ReadPreference.Primary, 12) must_=== expected
      }
    }
  }

  // ---

  val pack = reactivemongo.api.tests.pack

  protected final class TestQueryBuilder(
    val failoverStrategy: FailoverStrategy = FailoverStrategy.default,
    val queryOption: Option[pack.Document] = None,
    val sortOption: Option[pack.Document] = None,
    val projectionOption: Option[pack.Document] = None,
    val hintOption: Option[pack.Document] = None,
    val explainFlag: Boolean = false,
    val snapshotFlag: Boolean = false,
    val commentString: Option[String] = None,
    val options: tests.QueryOpts = tests.QueryOpts(),
    val maxTimeMsOption: Option[Long] = None,
    val version: MongoWireVersion = MongoWireVersion.V34) extends GenericQueryBuilder[pack.type] {
    type Self = TestQueryBuilder
    val pack: specs.pack.type = specs.pack

    def copy(
      queryOption: Option[pack.Document] = queryOption,
      sortOption: Option[pack.Document] = sortOption,
      projectionOption: Option[pack.Document] = projectionOption,
      hintOption: Option[pack.Document] = hintOption,
      explainFlag: Boolean = explainFlag,
      snapshotFlag: Boolean = snapshotFlag,
      commentString: Option[String] = commentString,
      options: tests.QueryOpts = options,
      failoverStrategy: FailoverStrategy = failoverStrategy,
      maxTimeMsOption: Option[Long] = maxTimeMsOption): TestQueryBuilder =
      new TestQueryBuilder(failoverStrategy, queryOption, sortOption,
        projectionOption, hintOption, explainFlag, snapshotFlag, commentString,
        options, maxTimeMsOption)

  }
}
