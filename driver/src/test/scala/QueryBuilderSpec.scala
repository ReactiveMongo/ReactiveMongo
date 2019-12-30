import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.api.{ Collation, FailoverStrategy, QueryOpts }
import reactivemongo.api.collections.GenericQueryBuilder

import reactivemongo.api.bson.BSONDocument

final class QueryBuilderSpec extends org.specs2.mutable.Specification { specs =>
  "Query builder" title

  section("unit")

  lazy val b = new TestQueryBuilder()

  "Boolean flag" should {
    "be set for" >> {
      "maxScan" in {
        b.maxScan must beFalse and {
          b.maxScan(true).maxScan must beTrue
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

  section("unit")

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
    val options: QueryOpts = QueryOpts(),
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
      options: QueryOpts = options,
      @deprecatedName(Symbol("failover")) failoverStrategy: FailoverStrategy = failoverStrategy,
      maxTimeMsOption: Option[Long] = maxTimeMsOption): TestQueryBuilder =
      new TestQueryBuilder(failoverStrategy, queryOption, sortOption,
        projectionOption, hintOption, explainFlag, snapshotFlag, commentString,
        options, maxTimeMsOption)

  }
}
