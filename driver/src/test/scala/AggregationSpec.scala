import scala.collection.immutable.{ ListSet, Set }

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.bson.{
  BSONArray,
  BSONDocument,
  BSONDocumentHandler,
  BSONDocumentReader,
  BSONInteger,
  BSONLong,
  BSONNull,
  BSONString,
  BSONTimestamp,
  Macros,
  array,
  document
}
import reactivemongo.api.{
  Cursor,
  CursorOps,
  CursorProducer,
  ReadConcern,
  WrappedCursor,
  WrappedCursorOps
}
import reactivemongo.api.indexes._, IndexType._

import reactivemongo.api.bson.collection.BSONCollection

import reactivemongo.api.tests.{ builder, pack }

import org.specs2.concurrent.ExecutionEnv
import org.specs2.specification.core.Fragments

final class AggregationSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification
  with org.specs2.specification.AfterAll {

  "Aggregation framework" title

  sequential
  stopOnFail

  // ---

  import tests.Common, Common.{ timeout, slowTimeout }

  @volatile private var integration = false
  lazy val (db, slowDb) = {
    integration = true
    Common.databases(s"reactivemongo-agg-${System identityHashCode this}", Common.connection, Common.slowConnection)
  }

  def afterAll(): Unit = if (integration) { db.drop(); () }

  val zipColName = s"zipcodes${System identityHashCode this}"
  lazy val coll: BSONCollection = {
    val c: BSONCollection = db(zipColName)
    scala.concurrent.Await.result(c.create().flatMap(_ =>
      c.indexesManager.ensure(index(
        List("city" -> Text, "state" -> Text))).map(_ => c)), timeout * 2)
  }
  lazy val slowZipColl: BSONCollection = slowDb(zipColName)

  implicit val locationHandler: BSONDocumentHandler[Location] = Macros.handler[Location]
  implicit val zipCodeHandler: BSONDocumentHandler[ZipCode] = Macros.handler[ZipCode]

  private val jpCodes = List(
    ZipCode("JP 13", "TOKYO", "JP", 13185502L,
      Location(35.683333, 139.683333)),
    ZipCode("AO", "AOGASHIMA", "JP", 200L, Location(32.457, 139.767)))

  private val zipCodes = List(
    ZipCode("10280", "NEW YORK", "NY", 19746227L,
      Location(-74.016323, 40.710537)),
    ZipCode("72000", "LE MANS", "FR", 148169L,
      Location(48.0077, 0.1984))) ++ jpCodes

  // ---

  "Zip codes collection" should {
    "expose the index stats" in {
      import coll.AggregationFramework.IndexStatsResult
      //import coll.AggregationResultImplicits.BSONIndexStatsReader

      val idxAgg = coll.aggregateWith[IndexStatsResult]() { framework =>
        import framework._

        List(IndexStats, Sort(Ascending("name")))
      }.collect[List](
        Int.MaxValue, Cursor.FailOnError[List[IndexStatsResult]]()).
        map(_.find(_.name != "_id_"))

      idxAgg must beLike[Option[IndexStatsResult]] {
        case Some(IndexStatsResult("_id_", k2, _, _)) =>
          sys.error(s"--> ${BSONDocument pretty k2}")

        case Some(IndexStatsResult("city_text_state_text", k2, _, _)) =>
          k2.string("_fts") must beSome("text") and {
            k2.int("_ftsx") must beSome(1)
          }
      }.await(1, timeout)
    }

    "be inserted" in {
      def insert(data: List[ZipCode]): Future[Unit] = data.headOption match {
        case Some(zip) => coll.insert.one(zip).flatMap(_ => insert(data.tail))
        case _         => Future.successful({})
      }

      insert(zipCodes) must beTypedEqualTo({}).await(1, timeout) and {
        coll.count() aka "c#1" must beTypedEqualTo(4L).await(1, slowTimeout)
      } and {
        slowZipColl.count() aka "c#2" must beTypedEqualTo(4L).
          await(1, slowTimeout)
      }
    }

    "return states with populations above 10000000" >> {
      // http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set/#return-states-with-populations-above-10-million
      val expected = List(
        document("_id" -> "JP", "totalPop" -> 13185702L),
        document("_id" -> "NY", "totalPop" -> 19746227L))

      def withRes[T](c: BSONCollection)(f: Future[List[BSONDocument]] => T): T = {
        f(c.aggregateWith[BSONDocument]() { framework =>
          import framework.{ Ascending, Group, Match, Sort, SumField }

          List(
            Group(BSONString(f"$$state"))(
              "totalPop" -> SumField("population")),
            Match(document("totalPop" -> document(f"$$gte" -> 10000000L))),
            Sort(Ascending("_id")))
        }.collect[List](Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]()))
      }

      "with the default connection" in withRes(coll) {
        _ aka "results" must beTypedEqualTo(expected).await(1, timeout)
      }

      "with the slow connection" in withRes(slowZipColl) {
        _ aka "results" must beTypedEqualTo(expected).await(1, slowTimeout)
      }

      "using a view" in {
        import coll.AggregationFramework
        import AggregationFramework.{ Ascending, Group, Match, Sort, SumField }

        val viewName = s"pop10m${System identityHashCode this}"

        def result = for {
          _ <- coll.createView(
            name = viewName,
            operator = Group(BSONString(f"$$state"))(
              "totalPop" -> SumField("population")),
            pipeline = Seq(
              Match(document("totalPop" -> document(f"$$gte" -> 10000000L))),
              Sort(Ascending("_id"))))
          view: BSONCollection = db(viewName)
          res <- view.find(
            BSONDocument.empty).cursor[BSONDocument]().collect[List](
              expected.size + 2, Cursor.FailOnError[List[BSONDocument]]())

        } yield res

        result must beTypedEqualTo(expected).await(1, timeout)
      } tag "gt_mongo32"

      "with expected count" in {
        import coll.AggregationFramework
        import AggregationFramework.{ Group, SumAll }

        val result = coll.aggregatorContext[BSONDocument](
          List(Group(BSONString(f"$$state"))("count" -> SumAll))).
          prepared.cursor.collect[Set](
            Int.MaxValue, Cursor.FailOnError[Set[BSONDocument]]())

        result must beTypedEqualTo(Set(
          document("_id" -> "JP", "count" -> 2),
          document("_id" -> "FR", "count" -> 1),
          document("_id" -> "NY", "count" -> 1))).await(1, timeout)
      }
    }

    "explain simple result" in {
      val result = coll.aggregateWith[BSONDocument](explain = true) {
        framework =>
          import framework.{ Group, Match, SumField }

          List(
            Group(BSONString(f"$$state"))(
              "totalPop" -> SumField("population")),
            Match(document("totalPop" -> document(f"$$gte" -> 10000000L))))
      }.headOption

      result aka "results" must beLike[Option[BSONDocument]] {
        case Some(explainResult) =>
          explainResult.getAsOpt[BSONArray]("stages") must beSome
      }.await(1, timeout)
    }

    "return average city population by state" >> {
      // See http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set/#return-average-city-population-by-state
      val expected = List(
        document("_id" -> "FR", "avgCityPop" -> 148169D),
        document("_id" -> "JP", "avgCityPop" -> 6592851D),
        document("_id" -> "NY", "avgCityPop" -> 19746227D))

      def withCtx[T](c: BSONCollection)(f: (c.AggregationFramework.Group, List[c.PipelineOperator]) => T): T = {
        import c.AggregationFramework
        import AggregationFramework.{ Ascending, Group, Sort, SumField }

        val firstOp = Group(document(
          "state" -> f"$$state", "city" -> f"$$city"))(
          "pop" -> SumField("population"))

        val pipeline = List(
          Group(BSONString(f"$$_id.state"))(
            "avgCityPop" -> AggregationFramework.AvgField("pop")),
          Sort(Ascending("_id")))

        f(firstOp, pipeline)
      }

      "successfully as a single batch" in {
        withCtx(coll) { (firstOp, pipeline) =>
          coll.aggregateWith[BSONDocument]() { _ =>
            firstOp +: pipeline
          }.collect[List]() must beTypedEqualTo(expected).await(1, timeout)
        }
      }

      "with cursor" >> {
        def collect(c: BSONCollection, upTo: Int = Int.MaxValue) =
          withCtx(c) { (firstOp, pipeline) =>
            c.aggregateWith[BSONDocument](batchSize = Some(1)) { _ =>
              firstOp +: pipeline
            }.collect[List](upTo)
          }

        "without limit (maxDocs)" in {
          collect(coll) must beTypedEqualTo(expected).await(1, timeout)
        }

        "with limit (maxDocs)" in {
          collect(coll, 2) must beTypedEqualTo(expected take 2).
            await(1, timeout)
        }

        "with metadata sort" in {
          coll.aggregateWith[ZipCode]() { framework =>
            import framework.{
              Descending,
              Match,
              MetadataSort,
              Sort,
              TextScore
            }

            val firstOp = Match(BSONDocument(
              f"$$text" -> BSONDocument(f"$$search" -> "JP")))
            val pipeline = List(Sort(
              MetadataSort("score", TextScore), Descending("city")))

            firstOp +: pipeline
          }.collect[List](4, Cursor.FailOnError[List[ZipCode]]()).
            aka("aggregated") must beTypedEqualTo(jpCodes).await(1, timeout)

        }
      }

      "with produced cursor" >> {
        "without limit (maxDocs)" in withCtx(coll) { (firstOp, pipeline) =>
          val cursor = coll.aggregatorContext[BSONDocument](
            (firstOp +: pipeline), batchSize = Some(1)).prepared.cursor

          cursor.collect[List](
            Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]()) must beTypedEqualTo(expected).await(1, timeout)
        }

        "of expected type" in withCtx(coll) { (firstOp, pipeline) =>
          // Custom cursor support
          trait FooCursor[T] extends Cursor[T] with CursorOps[T] {
            def foo: String
          }

          class DefaultFooCursor[T](val wrappee: Cursor.WithOps[T])
            extends FooCursor[T] with WrappedCursor[T]
            with WrappedCursorOps[T] {

            @inline def opsWrappee = wrappee

            val foo = "Bar"
          }

          implicit def fooProducer[T] = new CursorProducer[T] {
            type ProducedCursor = FooCursor[T]
            def produce(base: Cursor.WithOps[T]) = new DefaultFooCursor(base)
          }

          // Aggregation itself
          val aggregator = coll.aggregatorContext[BSONDocument](
            (firstOp +: pipeline), batchSize = Some(1)).prepared[FooCursor]

          aggregator.cursor.isInstanceOf[FooCursor[BSONDocument]] must beTrue
        }
      }
    }

    "return largest and smallest cities by state" in {
      // See http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set/#return-largest-and-smallest-cities-by-state
      import coll.AggregationFramework
      import AggregationFramework.{
        FirstField,
        Group,
        LastField,
        Limit,
        Project,
        Sort,
        Ascending,
        Skip,
        SumField
      }

      val expected = List(
        document(
          "biggestCity" -> document(
            "name" -> "LE MANS", "population" -> 148169L),
          "smallestCity" -> document(
            "name" -> "LE MANS", "population" -> 148169L),
          "state" -> "FR"),
        document(
          "biggestCity" -> document(
            "name" -> "TOKYO", "population" -> 13185502L),
          "smallestCity" -> document(
            "name" -> "AOGASHIMA", "population" -> 200L),
          "state" -> "JP"),
        document(
          "biggestCity" -> document(
            "name" -> "NEW YORK", "population" -> 19746227L),
          "smallestCity" -> document(
            "name" -> "NEW YORK", "population" -> 19746227L),
          "state" -> "NY"))

      val primGroup = Group(document(
        "state" -> f"$$state", "city" -> f"$$city"))(
        "pop" -> SumField("population"))

      val groupPipeline = List(
        Sort( // enforce order before Group using {First,Last}Field
          Ascending("state"), Ascending("pop")),
        Group(BSONString(f"$$_id.state"))(
          "biggestCity" -> LastField("_id.city"),
          "biggestPop" -> LastField("pop"),
          "smallestCity" -> FirstField("_id.city"),
          "smallestPop" -> FirstField("pop")),
        Project(document("_id" -> 0, "state" -> f"$$_id",
          "biggestCity" -> document(
            "name" -> f"$$biggestCity", "population" -> f"$$biggestPop"),
          "smallestCity" -> document(
            "name" -> f"$$smallestCity", "population" -> f"$$smallestPop"))),
        Sort(Ascending("state")))

      coll.aggregateWith[BSONDocument]() { _ =>
        primGroup +: groupPipeline
      }.collect[List](Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]()) must beTypedEqualTo(expected).await(1, timeout) and {
        coll.aggregateWith[BSONDocument]() { _ =>
          primGroup +: (groupPipeline :+ Limit(2))
        }.collect[List](Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]()) must beTypedEqualTo(expected take 2).await(1, timeout)
      } and {
        coll.aggregateWith[BSONDocument]() { _ =>
          primGroup +: (groupPipeline :+ Skip(2))
        }.collect[List](Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]()) must beTypedEqualTo(expected drop 2).await(1, timeout)
      }
    }

    "return distinct states" >> {
      def distinctSpec(c: BSONCollection, timeout: FiniteDuration) =
        c.distinct[String, ListSet](
          key = "state",
          selector = None,
          readConcern = ReadConcern.Local,
          collation = None) must beTypedEqualTo(
          ListSet("NY", "FR", "JP")).await(1, timeout)

      "with the default connection" in {
        distinctSpec(coll, timeout)
      }

      "with the slow connection" in {
        distinctSpec(slowZipColl, slowTimeout)
      }
    }
  }

  "Inventory #1" should {
    lazy val orders: BSONCollection =
      db.collection(s"agg-orders-1-${System identityHashCode this}")

    lazy val inventory: BSONCollection = db.collection(
      s"agg-inv-1-${System identityHashCode orders}")

    "be provided with order fixtures" in {
      (for {
        // orders
        _ <- orders.insert.many(Seq(
          BSONDocument(
            "_id" -> 1, "item" -> "abc", "price" -> 12, "quantity" -> 2),
          BSONDocument(
            "_id" -> 2, "item" -> "jkl", "price" -> 20, "quantity" -> 1),
          BSONDocument("_id" -> 3)))

        // inventory
        _ <- inventory.insert.many(Seq(
          BSONDocument("_id" -> 1, "sku" -> "abc",
            "description" -> "product 1", "instock" -> 120),
          BSONDocument("_id" -> 2, "sku" -> "def",
            "description" -> "product 2", "instock" -> 80),
          BSONDocument("_id" -> 3, "sku" -> "ijk",
            "description" -> "product 3", "instock" -> 60),
          BSONDocument("_id" -> 4, "sku" -> "jkl",
            "description" -> "product 4", "instock" -> 70),
          BSONDocument("_id" -> 5, "sku" -> Option.empty[String],
            "description" -> "Incomplete"),
          BSONDocument("_id" -> 6)))

      } yield ()) must beTypedEqualTo({}).await(0, timeout)
    }

    "perform a simple lookup so the joined documents are returned" in {
      // See https://docs.mongodb.com/master/reference/operator/aggregation/lookup/#examples

      implicit val productHandler: BSONDocumentHandler[Product] = Macros.handler[Product]
      implicit val invReportHandler: BSONDocumentHandler[InventoryReport] = Macros.handler[InventoryReport]

      def expected = List(
        InventoryReport(1, Some("abc"), Some(12), Some(2),
          List(Product(1, Some("abc"), Some("product 1"), Some(120)))),
        InventoryReport(2, Some("jkl"), Some(20), Some(1),
          List(Product(4, Some("jkl"), Some("product 4"), Some(70)))),
        InventoryReport(3, docs = List(
          Product(5, None, Some("Incomplete")), Product(6))))

      import orders.AggregationFramework.Lookup

      orders.aggregatorContext[InventoryReport](
        List(Lookup(inventory.name, "item", "sku", "docs"))).
        prepared.cursor.collect[List](
          Int.MaxValue, Cursor.FailOnError[List[InventoryReport]]()).
          aka("result") must beTypedEqualTo(expected).await(0, timeout)

    }

    "perform a graph lookup so the joined documents are returned" in {
      // See https://docs.mongodb.com/manual/reference/operator/aggregation/graphLookup/#examples

      implicit val productHandler: BSONDocumentHandler[Product] = Macros.handler[Product]
      implicit val invReportHandler: BSONDocumentHandler[InventoryReport] = Macros.handler[InventoryReport]

      def expected = List(
        InventoryReport(1, Some("abc"), Some(12), Some(2),
          List(Product(1, Some("abc"), Some("product 1"), Some(120)))),
        InventoryReport(2, Some("jkl"), Some(20), Some(1),
          List(Product(4, Some("jkl"), Some("product 4"), Some(70)))),
        InventoryReport(3, docs = List.empty))

      import orders.AggregationFramework.GraphLookup

      orders.aggregatorContext[InventoryReport](
        List(GraphLookup(
          inventory.name, BSONString(f"$$item"), "item", "sku", "docs"))).
        prepared.cursor.collect[List](
          Int.MaxValue, Cursor.FailOnError[List[InventoryReport]]()).
          aka("result") must beTypedEqualTo(expected).await(0, timeout)

    } tag "gt_mongo32"
  }

  "Inventory #2" should {
    lazy val orders: BSONCollection =
      db.collection(s"agg-order-2-${System identityHashCode this}")

    lazy val inventory: BSONCollection = db.collection(
      s"agg-inv-2-${System identityHashCode orders}")

    "be provided the fixtures" in {
      (for {
        // orders
        _ <- orders.insert.one(BSONDocument(
          "_id" -> 1, "item" -> "MON1003", "price" -> 350, "quantity" -> 2,
          "specs" -> BSONArray("27 inch", "Retina display", "1920x1080"),
          "type" -> "Monitor"))

        // inventory
        _ <- inventory.insert.many(Seq(
          BSONDocument("_id" -> 1, "sku" -> "MON1003",
            "type" -> "Monitor", "instock" -> 120, "size" -> "27 inch",
            "resolution" -> "1920x1080"),
          BSONDocument("_id" -> 2, "sku" -> "MON1012",
            "type" -> "Monitor", "instock" -> 85, "size" -> "23 inch",
            "resolution" -> "1920x1080"),
          BSONDocument("_id" -> 3, "sku" -> "MON1031",
            "type" -> "Monitor", "instock" -> 60, "size" -> "23 inch",
            "displayType" -> "LED")))

      } yield ()) must beTypedEqualTo({}).await(0, timeout)
    }

    "so the joined documents are returned" in {
      import orders.AggregationFramework.{ Lookup, Match }

      def expected = document(
        "_id" -> 1,
        "item" -> "MON1003",
        "price" -> 350,
        "quantity" -> 2,
        "specs" -> "27 inch",
        "type" -> "Monitor",
        "docs" -> BSONArray(document(
          "_id" -> BSONInteger(1),
          "sku" -> "MON1003",
          "type" -> "Monitor",
          "instock" -> BSONInteger(120),
          "size" -> "27 inch",
          "resolution" -> "1920x1080")))

      val afterUnwind = List(
        Lookup(inventory.name, "specs", "size", "docs"),
        Match(document("docs" -> document(f"$$ne" -> BSONArray()))))

      orders.aggregateWith[BSONDocument]() { framework =>
        import framework._

        UnwindField("specs") +: afterUnwind
      }.headOption must beSome(expected).await(0, timeout) and {
        orders.aggregateWith[BSONDocument]() { framework =>
          import framework._

          Unwind("specs", None, Some(true)) +: afterUnwind
        }.headOption must beSome(expected).await(0, timeout)
      }
    }
  }

  f"Aggregation result for '$$out'" should {
    // https://docs.mongodb.com/master/reference/operator/aggregation/out/#example

    lazy val books: BSONCollection = db.collection(
      s"books-1-${System identityHashCode this}")

    "with valid fixtures" in {
      val fixtures = Seq(
        BSONDocument(
          "_id" -> 8751, "title" -> "The Banquet",
          "author" -> "Dante", "copies" -> 2),
        BSONDocument(
          "_id" -> 8752, "title" -> "Divine Comedy",
          "author" -> "Dante", "copies" -> 1),
        BSONDocument(
          "_id" -> 8645, "title" -> "Eclogues",
          "author" -> "Dante", "copies" -> 2),
        BSONDocument(
          "_id" -> 7000, "title" -> "The Odyssey",
          "author" -> "Homer", "copies" -> 10),
        BSONDocument(
          "_id" -> 7020, "title" -> "Iliad",
          "author" -> "Homer", "copies" -> 10))

      books.insert.many(fixtures).map(_ => {}).
        aka("fixtures") must beTypedEqualTo({}).await(0, timeout)
    }

    "be outputed to collection" in {
      val outColl = s"authors-1-${System identityHashCode this}"

      type Author = (String, List[String])
      implicit val authorReader: BSONDocumentReader[(String, List[String])] = BSONDocumentReader[Author] { doc =>
        (for {
          id <- doc.getAsTry[String]("_id")
          books <- doc.getAsTry[List[String]]("books")
        } yield id -> books).get
      }

      books.aggregateWith[Author]() { framework =>
        import framework._

        List(
          Sort(Ascending("title")),
          Group(BSONString(f"$$author"))("books" -> PushField("title")),
          Out(outColl))
      }.collect[List](Int.MaxValue, Cursor.FailOnError[List[Author]]()).map(_ => {}) must beEqualTo({}).await(0, timeout) and {
        db.collection[BSONCollection](outColl).find(BSONDocument.empty).
          sort(BSONDocument("_id" -> -1)).cursor[Author]().
          collect[List](3, Cursor.FailOnError[List[Author]]()) must beTypedEqualTo(
            List(
              "Homer" -> List("Iliad", "The Odyssey"),
              "Dante" -> List(
                "Divine Comedy",
                "Eclogues", "The Banquet"))).await(0, timeout)
      }
    }
  }

  "Aggregation result for '$stdDevPop'" should {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevPop/#examples

    lazy val contest: BSONCollection =
      db.collection(s"contest-1-${System identityHashCode this}")

    "with valid fixtures" in {
      /*
       { "_id" : 1, "name" : "dave123", "quiz" : 1, "score" : 85 }
       { "_id" : 2, "name" : "dave2", "quiz" : 1, "score" : 90 }
       { "_id" : 3, "name" : "ahn", "quiz" : 1, "score" : 71 }
       { "_id" : 4, "name" : "li", "quiz" : 2, "score" : 96 }
       { "_id" : 5, "name" : "annT", "quiz" : 2, "score" : 77 }
       { "_id" : 6, "name" : "ty", "quiz" : 2, "score" : 82 }
       */
      val fixtures = Seq(
        BSONDocument(
          "_id" -> 1,
          "name" -> "dave123", "quiz" -> 1, "score" -> 85),
        BSONDocument(
          "_id" -> 2,
          "name" -> "dave2", "quiz" -> 1, "score" -> 90),
        BSONDocument(
          "_id" -> 3,
          "name" -> "ahn", "quiz" -> 1, "score" -> 71),
        BSONDocument(
          "_id" -> 4,
          "name" -> "li", "quiz" -> 2, "score" -> 96),
        BSONDocument(
          "_id" -> 5,
          "name" -> "annT", "quiz" -> 2, "score" -> 77),
        BSONDocument(
          "_id" -> 6,
          "name" -> "ty", "quiz" -> 2, "score" -> 82))

      Future.sequence(fixtures.map { doc =>
        contest.insert.one(doc)
      }).map(_ => {}) aka "fixtures" must beTypedEqualTo({}).await(0, timeout)
    }

    "return the standard deviation of each quiz" in {
      implicit val reader: BSONDocumentReader[QuizStdDev] =
        Macros.reader[QuizStdDev]

      /*
       db.contest.aggregate([
         { $group: { _id: "$quiz", stdDev: { $stdDevPop: "$score" } } }
       ])
      */
      contest.aggregateWith[QuizStdDev]() {
        framework =>
          import framework._

          List(
            Group(BSONString(f"$$quiz"))(
              "stdDev" -> StdDevPopField("score")),
            Sort(Ascending("_id")))
      }.collect[List](Int.MaxValue, Cursor.FailOnError[List[QuizStdDev]]()).aka(f"$$stdDevPop results") must beTypedEqualTo(List(
        QuizStdDev(1, 8.04155872120988D), QuizStdDev(2, 8.04155872120988D))).await(0, timeout)

      /*
       { "_id" : 1, "stdDev" : 8.04155872120988 }
       { "_id" : 2, "stdDev" : 8.04155872120988 }
       */
    }

    "return a sum as hash per quiz" in {
      import contest.AggregationFramework.{ Group, Sum }

      contest.aggregatorContext[BSONDocument](
        List(Group(BSONString(f"$$quiz"))(
          "hash" -> Sum(document(
            f"$$multiply" -> array(f"$$_id", f"$$score")))))).prepared.cursor
        .collect[Set](Int.MaxValue, Cursor.FailOnError[Set[BSONDocument]]()) must beTypedEqualTo(Set(
          document("_id" -> 2, "hash" -> 1261),
          document("_id" -> 1, "hash" -> 478))).await(1, timeout)
    }

    "return the maximum score per quiz" in {
      import contest.AggregationFramework.{ Group, MaxField }

      contest.aggregatorContext[BSONDocument](
        List(Group(BSONString(f"$$quiz"))(
          "maxScore" -> MaxField("score")))).prepared.cursor
        .collect[Set](Int.MaxValue, Cursor.FailOnError[Set[BSONDocument]]()) must beTypedEqualTo(Set(
          document("_id" -> 2, "maxScore" -> 96),
          document("_id" -> 1, "maxScore" -> 90))).await(1, timeout)
    }

    "return a max as hash per quiz" in {
      import contest.AggregationFramework.{ Group, Max }

      contest.aggregatorContext[BSONDocument](
        List(Group(BSONString(f"$$quiz"))(
          "maxScore" -> Max(document(
            f"$$multiply" -> array(f"$$_id", f"$$score")))))).prepared.cursor
        .collect[Set](Int.MaxValue, Cursor.FailOnError[Set[BSONDocument]]()) must beTypedEqualTo(Set(
          document("_id" -> 2, "maxScore" -> 492),
          document("_id" -> 1, "maxScore" -> 213))).await(1, timeout)
    }

    "return the minimum score per quiz" in {
      import contest.AggregationFramework.{ Group, MinField }

      contest.aggregatorContext[BSONDocument](
        List(Group(BSONString(f"$$quiz"))(
          "minScore" -> MinField("score")))).prepared.cursor
        .collect[Set](Int.MaxValue, Cursor.FailOnError[Set[BSONDocument]]()) must beTypedEqualTo(Set(
          document("_id" -> 2, "minScore" -> 77),
          document("_id" -> 1, "minScore" -> 71))).await(1, timeout)
    }

    "return a min as hash per quiz" in {
      import contest.AggregationFramework.{ Group, Min }

      contest.aggregatorContext[BSONDocument](
        List(Group(BSONString(f"$$quiz"))(
          "minScore" -> Min(document(
            f"$$multiply" -> array(f"$$_id", f"$$score")))))).prepared.cursor
        .collect[Set](Int.MaxValue, Cursor.FailOnError[Set[BSONDocument]]()) must beTypedEqualTo(Set(
          document("_id" -> 2, "minScore" -> 384),
          document("_id" -> 1, "minScore" -> 85))).await(1, timeout)
    }

    "push name and score per quiz group" in {
      val expected = Set(
        QuizScores(2, Set(
          Score("ty", 82), Score("annT", 77), Score("li", 96))),
        QuizScores(1, Set(
          Score("dave123", 85), Score("dave2", 90), Score("ahn", 71))))

      import contest.AggregationFramework.{ Group, Push }

      contest.aggregatorContext[QuizScores](
        List(Group(BSONString(f"$$quiz"))(
          "scores" -> Push(
            document("name" -> f"$$name", "score" -> f"$$score"))))).
        prepared.cursor
        .collect[Set](Int.MaxValue, Cursor.FailOnError[Set[QuizScores]]()) must beTypedEqualTo(expected).await(1, timeout)
    }

    "add name and score to a set per quiz group" in {
      val expected = Set(
        QuizScores(2, Set(
          Score("ty", 82), Score("annT", 77), Score("li", 96))),
        QuizScores(1, Set(
          Score("dave123", 85), Score("dave2", 90), Score("ahn", 71))))

      import contest.AggregationFramework.{ Group, AddToSet }

      contest.aggregatorContext[QuizScores](
        List(Group(BSONString(f"$$quiz"))(
          "scores" -> AddToSet(document(
            "name" -> f"$$name", "score" -> f"$$score"))))).prepared.cursor
        .collect[Set](Int.MaxValue, Cursor.FailOnError[Set[QuizScores]]()) must beTypedEqualTo(expected).await(1, timeout)
    }
  }

  "Aggregation result '$stdDevSamp'" should {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevSamp/#example

    lazy val contest: BSONCollection =
      db.collection(s"contest-2-${System identityHashCode this}")

    "with valid fixtures" in {
      /*
       {_id: 0, username: "user0", age: 20}
       {_id: 1, username: "user1", age: 42}
       {_id: 2, username: "user2", age: 28}
       */
      val fixtures = Seq(
        BSONDocument("_id" -> 0, "username" -> "user0", "age" -> 20),
        BSONDocument("_id" -> 1, "username" -> "user1", "age" -> 42),
        BSONDocument("_id" -> 2, "username" -> "user2", "age" -> 28))

      contest.insert.many(fixtures).map(_ => {}).
        aka("fixtures") must beTypedEqualTo({}).await(0, timeout)
    }

    "return the standard deviation of user ages" in {
      // { "_id" : null, "ageStdDev" : 11.135528725660043 }
      val expected = BSONDocument("_id" -> BSONNull, "ageStdDev" -> 11.135528725660043D)

      /*
       db.users.aggregate([
         { $sample: { size: 100 } },
         { $group: { _id: null, ageStdDev: { $stdDevSamp: "$age" } } }
       ])
      */

      contest.aggregateWith[BSONDocument]() { framework =>
        import framework._

        List(Sample(100), Group(BSONNull)(
          "ageStdDev" -> StdDevSamp(BSONString(f"$$age"))))
      }.headOption must beSome(expected).await(0, timeout)
    }
  }

  "Geo-indexed documents" should {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/geoNear/#example

    lazy val places: BSONCollection =
      db(s"places${System identityHashCode this}")

    "must be inserted" in {
      import reactivemongo.api.indexes._, IndexType._

      places.create().flatMap { _ =>
        places.indexesManager.ensure(index(List("loc" -> Geo2DSpherical)))
      }.map(_ => {}) must beEqualTo({}).await(1, timeout) and {
        /*
       {
         "type": "public",
         "loc": {
           "type": "Point", "coordinates": [-73.97, 40.77]
         },
         "name": "Central Park",
         "category": "Parks"
       },
       {
         "type": "public",
         "loc": {
           "type": "Point", "coordinates": [-73.88, 40.78]
         },
         "name": "La Guardia Airport",
         "category": "Airport"
       }
       */

        places.insert.many(Seq(
          document(
            "type" -> "public",
            "loc" -> document(
              "type" -> "Point", "coordinates" -> array(-73.97, 40.77)),
            "name" -> "Central Park",
            "category" -> "Parks"),
          document(
            "type" -> "public",
            "loc" -> document(
              "type" -> "Point", "coordinates" -> array(-73.88, 40.78)),
            "name" -> "La Guardia Airport",
            "category" -> "Airport"))).
          map(_ => {}) must beTypedEqualTo({}).await(0, timeout)
      }
    }

    "and aggregated using $geoNear" in {
      /*
       db.places.aggregate([{
         $geoNear: {
           near: { type: "Point", coordinates: [ -73.9667, 40.78 ] },
           distanceField: "dist.calculated",
           minDistance: 1000,
           maxDistance: 5000,
           query: { type: "public" },
           includeLocs: "dist.location",
           num: 5,
           spherical: true
         }
       }])
       */

      implicit val pointReader: BSONDocumentReader[GeoPoint] = Macros.reader[GeoPoint]
      implicit val distanceReader: BSONDocumentReader[GeoDistance] = BSONDocumentReader[GeoDistance] { doc =>
        (for {
          calc <- doc.double("calculated").map(_.toInt)
          loc <- doc.getAsOpt[GeoPoint]("loc")
        } yield GeoDistance(calc, loc)).get
      }
      implicit val placeReader: BSONDocumentReader[GeoPlace] = Macros.reader[GeoPlace]

      import places.AggregationFramework.{ GeoNear, Limit }

      places.aggregatorContext[GeoPlace](
        List(GeoNear(
          document(
            "type" -> "Point",
            "coordinates" -> array(-73.9667, 40.78)),
          distanceField = Some("dist.calculated"),
          minDistance = Some(1000),
          maxDistance = Some(5000),
          query = Some(document("type" -> "public")),
          includeLocs = Some("dist.loc"),
          spherical = true), Limit(5))).prepared.cursor
        .collect[List](Int.MaxValue, Cursor.FailOnError[List[GeoPlace]]()) aka "places" must beTypedEqualTo(List(
          GeoPlace(
            loc = GeoPoint(List(-73.97D, 40.77D)),
            name = "Central Park",
            category = "Parks",
            dist = GeoDistance(
              calculated = 1147,
              loc = GeoPoint(List(-73.97D, 40.77D)))))).await(0, timeout)

      // { "type" : "public", "loc" : { "type" : "Point", "coordinates" : [ -73.97, 40.77 ] }, "name" : "Central Park", "category" : "Parks", "dist" : { "calculated" : 1147.4220523120696, "loc" : { "type" : "Point", "coordinates" : [ -73.97, 40.77 ] } } }
    }
  }

  "Forecasts" should {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/redact/
    lazy val forecasts: BSONCollection =
      db(s"forecasts${System identityHashCode this}")

    "be inserted" in {
      /*
{
  _id: 1,
  title: "123 Department Report",
  tags: [ "G", "STLW" ],
  year: 2014,
  subsections: [
    {
      subtitle: "Section 1: Overview",
      tags: [ "SI", "G" ],
      content:  "Section 1: This is the content of section 1."
    },
    {
      subtitle: "Section 2: Analysis",
      tags: [ "STLW" ],
      content: "Section 2: This is the content of section 2."
    },
    {
      subtitle: "Section 3: Budgeting",
      tags: [ "TK" ],
      content: {
        text: "Section 3: This is the content of section3.",
        tags: [ "HCS" ]
      }
    }
  ]
}
 */

      forecasts.insert.one(BSONDocument(
        "_id" -> 1,
        "title" -> "123 Department Report",
        "tags" -> BSONArray("G", "STLW"),
        "year" -> 2014,
        "subsections" -> BSONArray(
          BSONDocument(
            "subtitle" -> "Section 1: Overview",
            "tags" -> BSONArray("SI", "G"),
            "content" -> "Section 1: This is the content of section 1."),
          BSONDocument(
            "subtitle" -> "Section 2: Analysis",
            "tags" -> BSONArray("STLW"),
            "content" -> "Section 2: This is the content of section 2."),
          BSONDocument(
            "subtitle" -> "Section 3: Budgeting",
            "tags" -> BSONArray("TK"),
            "content" -> BSONDocument(
              "text" -> "Section 3: This is the content of section3.",
              "tags" -> BSONArray("HCS")))))).
        map(_ => {}) must beTypedEqualTo({}).await(0, timeout)
    }

    "be redacted" in {
      implicit val subsectionReader: BSONDocumentHandler[Subsection] =
        Macros.handler[Subsection]

      implicit val reader: BSONDocumentHandler[Redaction] =
        Macros.handler[Redaction]

      /*
var userAccess = [ "STLW", "G" ];
db.forecasts.aggregate(
   [
     { $match: { year: 2014 } },
     { $redact: {
        $cond: {
           if: { $gt: [ { $size: { $setIntersection: [ "$tags", userAccess ] } }, 0 ] },
           then: "$$DESCEND",
           else: "$$PRUNE"
         }
       }
     }
   ]
);
 */
      val result = forecasts.aggregateWith[Redaction]() { framework =>
        import framework.{ Match, Redact }

        List(
          Match(document("year" -> 2014)),
          Redact(document(f"$$cond" -> document(
            "if" -> document(
              f"$$gt" -> array(document(
                f"$$size" -> document(f"$$setIntersection" -> array(
                  f"$$tags", array("STLW", "G")))), 0)),
            "then" -> f"$$$$DESCEND",
            "else" -> f"$$$$PRUNE"))))

      }.headOption

      val expected = Redaction(
        title = "123 Department Report",
        tags = List("G", "STLW"),
        year = 2014,
        subsections = List(
          Subsection(
            subtitle = "Section 1: Overview",
            tags = List("SI", "G"),
            content = "Section 1: This is the content of section 1."),
          Subsection(
            subtitle = "Section 2: Analysis",
            tags = List("STLW"),
            content = "Section 2: This is the content of section 2.")))
      /*
{
  "_id" : 1,
  "title" : "123 Department Report",
  "tags" : [ "G", "STLW" ],
  "year" : 2014,
  "subsections" : [
    {
      "subtitle" : "Section 1: Overview",
      "tags" : [ "SI", "G" ],
      "content" : "Section 1: This is the content of section 1."
    },
    {
      "subtitle" : "Section 2: Analysis",
      "tags" : [ "STLW" ],
      "content" : "Section 2: This is the content of section 2."
    }
  ]
}
 */

      result must beSome(expected).await(0, timeout)
    }
  }

  "Customer accounts" should {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/redact/
    lazy val customers: BSONCollection =
      db(s"customers${System identityHashCode this}")

    "be inserted" in {
      /*
{
  _id: 1,
  level: 1,
  acct_id: "xyz123",
  cc: {
    level: 5,
    type: "yy",
    num: 000000000000,
    exp_date: ISODate("2015-11-01T00:00:00.000Z"),
    billing_addr: {
      level: 5,
      addr1: "123 ABC Street",
      city: "Some City"
    },
    shipping_addr: [
      {
        level: 3,
        addr1: "987 XYZ Ave",
        city: "Some City"
      },
      {
        level: 3,
        addr1: "PO Box 0123",
        city: "Some City"
      }
    ]
  },
  status: "A"
}
*/
      customers.insert.one(document(
        "_id" -> 1,
        "level" -> 1,
        "acct_id" -> "xyz123",
        "cc" -> document(
          "level" -> 5,
          "type" -> "yy",
          "num" -> "000000000000",
          "exp_date" -> "2015-11-01T00:00:00.000Z",
          "billing_addr" -> document(
            "level" -> 5,
            "addr1" -> "123 ABC Street",
            "city" -> "Some City"),
          "shipping_addr" -> array(
            document(
              "level" -> 3,
              "addr1" -> "987 XYZ Ave",
              "city" -> "Some City"),
            document(
              "level" -> 3,
              "addr1" -> "PO Box 0123",
              "city" -> "Some City"))),
        "status" -> "A")).
        map(_ => {}) must beTypedEqualTo({}).await(0, timeout)
    }

    "be redacted" in {
      /*
db.accounts.aggregate([
    { $match: { status: "A" } },
    {
      $redact: {
        $cond: {
          if: { $eq: [ "$level", 5 ] },
          then: "$$PRUNE",
          else: "$$DESCEND"
        }
      }
    }
  ])
 */
      val result = customers.aggregateWith[BSONDocument]() { framework =>
        import framework.{ Match, Redact }

        List(
          Match(document("status" -> "A")),
          Redact(document(
            f"$$cond" -> document(
              "if" -> document(f"$$eq" -> array(f"$$level", 5)),
              "then" -> f"$$$$PRUNE",
              "else" -> f"$$$$DESCEND"))))

      }.headOption

      result must beSome(document(
        "_id" -> 1,
        "level" -> 1,
        "acct_id" -> "xyz123",
        "status" -> "A")).await(0, timeout)
    }
  }

  section("gt_mongo32")
  "Produce" should {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot/#replaceroot-with-an-embedded-document
    lazy val fruits: BSONCollection =
      db(s"fruits${System identityHashCode this}")

    "be inserted" in {
      /*
      {
         "_id" : 1,
         "fruit" : [ "apples", "oranges" ],
         "in_stock" : { "oranges" : 20, "apples" : 60 },
         "on_order" : { "oranges" : 35, "apples" : 75 }
      }
       */
      fruits.insert.one(document(
        "_id" -> 1,
        "fruit" -> array("apples", "oranges"),
        "in_stock" -> document(
          "oranges" -> 20,
          "apples" -> 60),
        "on_order" -> document(
          "oranges" -> 35,
          "apples" -> 75))).
        map(_ => {}) must beTypedEqualTo({}).await(0, timeout)
    }

    f"and reshaped using $$replaceRoot" in {
      val result = fruits.aggregateWith[BSONDocument]() { framework =>
        import framework.{ Match, ReplaceRootField }

        List(Match(document("_id" -> 1)), ReplaceRootField("in_stock"))
      }.headOption

      result must beSome(document("oranges" -> 20, "apples" -> 60)).
        await(0, timeout)
    }
  }

  "Contacts" should {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot/#replaceroot-with-a-newly-created-document
    lazy val contacts: BSONCollection =
      db(s"contacts${System identityHashCode this}")

    "be inserted" in {
      /*
      { "_id" : 1, "first_name" : "Gary", "last_name" : "Sheffield", "city" : "New York" }
       */
      contacts.insert.one(document(
        "_id" -> 1,
        "first_name" -> "Gary",
        "last_name" -> "Sheffield",
        "city" -> "New York")).
        map(_ => {}) must beTypedEqualTo({}).await(0, timeout)
    }

    "and reshaped using $replaceRoot" in {
      val result = contacts.aggregateWith[BSONDocument]() { framework =>
        import framework.{ Match, ReplaceRoot }

        List(Match(document("_id" -> 1)), ReplaceRoot(document(
          "full_name" -> document(
            f"$$concat" -> array(f"$$first_name", " ", f"$$last_name")))))

      }.headOption

      result must beSome(document(
        "full_name" -> "Gary Sheffield")).await(0, timeout)
    }

    "and counted" in {
      implicit def countReader = BSONDocumentReader[Int] {
        _.getAsTry[Int]("foo").get
      }

      contacts.aggregateWith[Int]() { framework =>
        import framework.{ Count, Match }

        List(Match(document("first_name" -> "Gary")), Count("foo"))
      }.head must beTypedEqualTo(1).await(0, timeout)
    }
  }

  "Students" should {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/addFields/
    lazy val students: BSONCollection =
      db(s"students${System identityHashCode this}")

    "be inserted" in {
      students.insert.many(Seq(
        document(
          "_id" -> 1,
          "student" -> "Maya",
          "homework" -> BSONArray(10, 5, 10),
          "quiz" -> BSONArray(10, 8),
          "extraCredit" -> 0),
        document(
          "_id" -> 2,
          "student" -> "Ryan",
          "homework" -> BSONArray(5, 6, 5),
          "quiz" -> BSONArray(8, 8),
          "extraCredit" -> 8))).
        map(_ => {}) must beTypedEqualTo({}).await(0, timeout)
    }

    f"be aggregated with $$sum on array fields" in {
      val expectedResults = Set(
        document(
          "_id" -> 1,
          "student" -> "Maya",
          "homework" -> BSONArray(10, 5, 10),
          "quiz" -> BSONArray(10, 8),
          "extraCredit" -> 0,
          "totalHomework" -> 25,
          "totalQuiz" -> 18,
          "totalScore" -> 43),
        document(
          "_id" -> 2,
          "student" -> "Ryan",
          "homework" -> BSONArray(5, 6, 5),
          "quiz" -> BSONArray(8, 8),
          "extraCredit" -> 8,
          "totalHomework" -> 16,
          "totalQuiz" -> 16,
          "totalScore" -> 40))

      students.aggregateWith[BSONDocument]() { framework =>
        import framework.AddFields

        List(
          AddFields(document(
            "totalHomework" -> document(f"$$sum" -> f"$$homework"),
            "totalQuiz" -> document(f"$$sum" -> f"$$quiz"))),
          AddFields(document(
            "totalScore" -> document(f"$$add" -> array(
              f"$$totalHomework", f"$$totalQuiz", f"$$extraCredit")))))

      }.collect[Set](Int.MaxValue, Cursor.FailOnError[Set[BSONDocument]]()).
        aka("aggregated") must beTypedEqualTo(expectedResults).await(0, timeout)
    }
  }

  "Users" should {
    implicit val userHandler: BSONDocumentHandler[User] = Macros.handler[User]

    // See https://docs.mongodb.com/manual/reference/operator/aggregation/slice/#example
    lazy val users: BSONCollection =
      db(s"users${System identityHashCode userHandler}")

    "be inserted" in {
      users.insert.many(Seq(
        User(_id = 1, name = "dave123",
          favorites = Seq("chocolate", "cake", "butter", "apples")),
        User(_id = 2, name = "li", favorites = Seq("apples", "pudding", "pie")),
        User(_id = 3, name = "ahn",
          favorites = Seq("pears", "pecans", "chocolate", "cherries")),
        User(_id = 4, name = "ty", favorites = Seq("ice cream")))).
        map(_ => {}) must beTypedEqualTo({}).await(0, timeout)

    }

    f"be aggregated using $$slice and $$count" in {
      users.aggregateWith[User]() { framework =>
        import framework.{ Project, Slice }

        List(Project(BSONDocument(
          "name" -> 1,
          "favorites" -> Slice(
            array = BSONString(f"$$favorites"),
            n = BSONInteger(3)))))

      }.collect[Seq](4, Cursor.FailOnError[Seq[User]]()).
        aka("top favorites") must beTypedEqualTo(Seq(
          User(_id = 1, name = "dave123",
            favorites = Seq("chocolate", "cake", "butter")),
          User(_id = 2, name = "li", favorites = Seq("apples", "pudding", "pie")),
          User(_id = 3, name = "ahn",
            favorites = Seq("pears", "pecans", "chocolate")),
          User(_id = 4, name = "ty", favorites = Seq("ice cream")))).
        await(0, timeout)
    }
  }
  section("gt_mongo32")

  section("unit")
  "Stage" should {
    import reactivemongo.api.tests.{ AggFramework => AggregationFramework }

    val makePipe = reactivemongo.api.tests.makePipe(AggregationFramework)(_)

    f"be $$addFields" in {
      import AggregationFramework.AddFields

      val specs = BSONDocument("foo" -> 1, "bar" -> 2L)

      makePipe(AddFields(specs)) must_=== BSONDocument(
        f"$$addFields" -> BSONDocument(specs))
    }

    f"be $$facet" in {
      // See https://docs.mongodb.com/manual/reference/operator/aggregation/facet/

      import AggregationFramework.{ Count, Facet, Out, UnwindField }

      makePipe(Facet(
        Seq(
          "foo" -> List(UnwindField("bar"), Count("c")),
          "lorem" -> List(Out("ipsum"))))) must_=== BSONDocument(
        f"$$facet" -> BSONDocument(
          "foo" -> BSONArray(
            BSONDocument(f"$$unwind" -> f"$$bar"),
            BSONDocument(f"$$count" -> "c")),
          "lorem" -> BSONArray(
            BSONDocument(f"$$out" -> "ipsum"))))
    }

    f"be $$bucket" in {
      import AggregationFramework.{ Bucket, SumField }

      makePipe(Bucket(
        groupBy = BSONString(f"$$foo"),
        boundaries = Seq(BSONInteger(10), BSONLong(20), BSONInteger(30)),
        default = "literal")("totalPop" -> SumField("population"))) must_=== BSONDocument(
        f"$$bucket" -> BSONDocument(
          "groupBy" -> f"$$foo",
          "default" -> "literal",
          "output" -> BSONDocument(
            "totalPop" -> BSONDocument(f"$$sum" -> f"$$population")),
          "boundaries" -> BSONArray(
            BSONInteger(10), BSONLong(20), BSONInteger(30))))

    }

    f"be $$bucketAuto" in {
      // https://docs.mongodb.com/manual/reference/operator/aggregation/bucketAuto/

      import AggregationFramework.BucketAuto

      makePipe(BucketAuto(BSONString(f"$$price"), 3, None)()) must_=== BSONDocument(f"$$bucketAuto" -> BSONDocument(
        "groupBy" -> f"$$price",
        "buckets" -> 3,
        "output" -> BSONDocument.empty))
    }

    f"be $$changeStream" >> {
      import reactivemongo.api.ChangeStreams.{ FullDocumentStrategy => St }
      import AggregationFramework.ChangeStream

      "when empty" in {
        makePipe(ChangeStream()) must_=== BSONDocument(
          f"$$changeStream" -> BSONDocument.empty)
      }

      "with fullDocument strategy" >> {
        Fragments.foreach[St](Seq(St.Default, St.UpdateLookup)) { st =>
          st.name in {
            makePipe(ChangeStream(
              fullDocumentStrategy = Some(st))) must_=== BSONDocument(
              f"$$changeStream" -> BSONDocument("fullDocument" -> st.name))
          }
        }
      }

      "with offset" >> {
        "that startAfter" in {
          makePipe(ChangeStream(offset = Some(ChangeStream.
            StartAfter(BSONString("offset"))))) must_=== BSONDocument(
            f"$$changeStream" -> BSONDocument("startAfter" -> "offset"))

        }

        "that startAtOperationTime" in {
          makePipe(ChangeStream(
            offset = Some(ChangeStream.StartAt(1234L)))) must_=== BSONDocument(
            f"$$changeStream" -> BSONDocument(
              "startAtOperationTime" -> BSONTimestamp(1234L)))

        }

        "that resumeAfter" in {
          makePipe(ChangeStream(offset = Some(ChangeStream.
            ResumeAfter(BSONString("offset"))))) must_=== BSONDocument(
            f"$$changeStream" -> BSONDocument("resumeAfter" -> "offset"))

        }
      }

      "with both fullDocument strategy and offset" in {
        makePipe(ChangeStream(
          offset = Some(ChangeStream.StartAt(1234L)),
          fullDocumentStrategy = Some(St.Default))) must_=== BSONDocument(
          f"$$changeStream" -> BSONDocument(
            "startAtOperationTime" -> BSONTimestamp(1234L),
            "fullDocument" -> "default"))

      }
    }

    f"be $$collStats" in {
      import AggregationFramework.CollStats

      makePipe(CollStats(
        latencyStatsHistograms = true,
        storageStatsScale = Some(1.23D),
        count = true)) must_=== BSONDocument(
        f"$$collStats" -> BSONDocument(
          "latencyStats" -> BSONDocument("histograms" -> true),
          "storageStats" -> BSONDocument("scale" -> 1.23D),
          "count" -> BSONDocument.empty))
    }

    f"be $$currentOp" in {
      import AggregationFramework.CurrentOp

      makePipe(CurrentOp()) must_=== BSONDocument(
        f"$$currentOp" -> BSONDocument(
          "allUsers" -> false,
          "idleConnections" -> false,
          "idleCursors" -> false,
          "idleSessions" -> true,
          "localOps" -> false))
    }

    f"be $$graphLookup" in {
      import AggregationFramework.GraphLookup

      makePipe(GraphLookup(
        from = "foo",
        startWith = BSONInteger(1),
        connectFromField = "bar",
        connectToField = "lorem",
        as = "ipsum",
        maxDepth = Some(2),
        depthField = Some("depth"),
        restrictSearchWithMatch = Some(BSONDocument.empty))) must_=== BSONDocument(f"$$graphLookup" -> BSONDocument(
        "from" -> "foo",
        "startWith" -> 1,
        "connectFromField" -> "bar",
        "connectToField" -> "lorem",
        "as" -> "ipsum",
        "maxDepth" -> 2,
        "depthField" -> "depth",
        "restrictSearchWithMatch" -> BSONDocument.empty))

    }

    f"be $$lookup with pipeline" in {
      import AggregationFramework.{ LookupPipeline, Match, Project }

      val let = BSONDocument(
        "order_item" -> f"$$item",
        "order_qty" -> f"$$ordered")

      val m = Match(BSONDocument(
        f"$$expr" -> BSONDocument(f"$$and" -> Seq(
          BSONDocument(f"$$eq" -> Seq(f"$$stock_item", f"$$order_item")),
          BSONDocument(f"$$gte" -> Seq(f"$$instock", f"$$order_qty"))))))

      val project = Project(BSONDocument("stock_item" -> 0, "_id" -> 0))

      makePipe(LookupPipeline(
        from = "warehouse",
        let = let,
        pipeline = List(m, project),
        as = "stockdata")) must_=== BSONDocument(f"$$lookup" -> BSONDocument(
        "from" -> "warehouse",
        "let" -> let,
        "pipeline" -> BSONArray(m, project),
        "as" -> "stockdata"))
    }

    f"be $$listLocalSessions" in {
      import AggregationFramework.ListLocalSessions

      makePipe(ListLocalSessions(
        BSONDocument("allUsers" -> true))) must_=== BSONDocument(
        f"$$listLocalSessions" -> BSONDocument("allUsers" -> true))
    }

    f"be $$listSessions" in {
      import AggregationFramework.ListSessions

      makePipe(ListSessions(
        BSONDocument("allUsers" -> true))) must_=== BSONDocument(
        f"$$listSessions" -> BSONDocument("allUsers" -> true))
    }

    f"be $$merge" >> {
      import AggregationFramework.Merge

      "with explicit target DB" in {
        makePipe(Merge(
          intoDb = "foo",
          intoCollection = "bar",
          on = Seq("lorem", "ipsum"),
          whenMatched = Some("replace"),
          let = Some(BSONDocument("var" -> "v")),
          whenNotMatched = None)) must_=== BSONDocument(
          f"$$merge" -> BSONDocument(
            "into" -> BSONDocument(
              "db" -> "foo",
              "coll" -> "bar"),
            "on" -> Seq("lorem", "ipsum"),
            "whenMatched" -> "replace",
            "let" -> BSONDocument("var" -> "v")))

      }

      "without current DB as target" in {
        makePipe(Merge(
          intoCollection = "bar",
          on = Seq("lorem", "ipsum"),
          whenMatched = Some("replace"),
          let = Some(BSONDocument("var" -> "v")),
          whenNotMatched = None)) must_=== BSONDocument(
          f"$$merge" -> BSONDocument(
            "into" -> "bar",
            "on" -> Seq("lorem", "ipsum"),
            "whenMatched" -> "replace",
            "let" -> BSONDocument("var" -> "v")))

      }
    }

    f"be $$planCacheStats" in {
      makePipe(AggregationFramework.PlanCacheStats) must_=== BSONDocument(
        f"$$planCacheStats" -> BSONDocument.empty)

    }

    f"be $$replaceWith" in {
      import AggregationFramework.ReplaceWith

      makePipe(ReplaceWith(BSONDocument("foo" -> 1))) must_=== BSONDocument(
        f"$$replaceWith" -> BSONDocument("foo" -> 1))
    }

    f"be $$set" in {
      import AggregationFramework.Set

      makePipe(Set(BSONDocument("foo" -> 1))) must_=== BSONDocument(
        f"$$set" -> BSONDocument("foo" -> 1))
    }

    f"be $$sortByCount" in {
      import AggregationFramework.{ SortByCount, SortByFieldCount }

      val expected = BSONDocument(f"$$sortByCount" -> f"$$foo")

      makePipe(SortByCount(BSONString(f"$$foo"))) must_=== expected and {
        makePipe(SortByFieldCount("foo")) must_=== expected
      }
    }

    f"be $$unset" in {
      import AggregationFramework.Unset

      makePipe(Unset("foo", List("bar", "lorem"))) must_=== BSONDocument(
        f"$$unset" -> List("foo", "bar", "lorem"))

    }

    f"be $$sample" in {
      import AggregationFramework.Sample

      makePipe(Sample(2)) must_=== BSONDocument(
        f"$$sample" -> BSONDocument("size" -> 2))
    }
  }

  "Group accumulator" >> {
    import reactivemongo.api.tests.AggFramework, AggFramework._

    Fragments.foreach(Seq[(GroupFunction, BSONDocument)](
      AvgField("foo") -> BSONDocument(f"$$avg" -> f"$$foo"),
      Avg(BSONString(f"$$bar")) -> BSONDocument(f"$$avg" -> f"$$bar"),
      FirstField("foo") -> BSONDocument(f"$$first" -> f"$$foo"),
      First(BSONString(f"$$bar")) -> BSONDocument(f"$$first" -> f"$$bar"),
      LastField("foo") -> BSONDocument(f"$$last" -> f"$$foo"),
      Last(BSONString(f"$$bar")) -> BSONDocument(f"$$last" -> f"$$bar"),
      MaxField("foo") -> BSONDocument(f"$$max" -> f"$$foo"),
      Max(BSONString(f"$$bar")) -> BSONDocument(f"$$max" -> f"$$bar"),
      MinField("foo") -> BSONDocument(f"$$min" -> f"$$foo"),
      Min(BSONString(f"$$bar")) -> BSONDocument(f"$$min" -> f"$$bar"),
      MergeObjects(
        BSONDocument("lorem" -> 1)) -> BSONDocument(
          f"$$mergeObjects" -> BSONDocument("lorem" -> 1)),
      PushField("foo") -> BSONDocument(f"$$push" -> f"$$foo"),
      Push(BSONString(f"$$bar")) -> BSONDocument(f"$$push" -> f"$$bar"),
      AddFieldToSet("foo") -> BSONDocument(f"$$addToSet" -> f"$$foo"),
      AddToSet(BSONString(f"$$bar")) -> BSONDocument(f"$$addToSet" -> f"$$bar"),
      StdDevPopField("foo") -> BSONDocument(f"$$stdDevPop" -> f"$$foo"),
      StdDevPop(BSONString(f"$$bar")) -> BSONDocument(
        f"$$stdDevPop" -> f"$$bar"),
      StdDevSampField("foo") -> BSONDocument(f"$$stdDevSamp" -> f"$$foo"),
      StdDevSamp(BSONString(f"$$bar")) -> BSONDocument(
        f"$$stdDevSamp" -> f"$$bar"),
      SumField("foo") -> BSONDocument(f"$$sum" -> f"$$foo"),
      Sum(BSONString(f"$$bar")) -> BSONDocument(f"$$sum" -> f"$$bar"))) {
      case (gfun, expected) => gfun.getClass.getSimpleName in {
        reactivemongo.api.tests.makeFunction(
          AggFramework)(gfun) must_=== expected
      }
    }
  }

  "Atlas Search" >> {
    import reactivemongo.api.tests.{
      AggFramework,
      scoreDocument,
      makeSearch
    }, AggFramework._, AtlasSearch.{ Score => AtlasScore, _ }

    "serialize score for term search" >> {
      val doc = scoreDocument(AggFramework) _

      Fragments.foreach[(AtlasScore, BSONDocument)](Seq(
        AtlasScore.constant(1.23D) -> BSONDocument("constant" -> 1.23D),
        AtlasScore.boost(45.6D) -> BSONDocument("boost" -> 45.6D))) {
        case (score, expected) => score.toString in {
          doc(score) must_=== expected
        }
      }
    }

    // ---

    val term1 = Term(
      query = "foo",
      path = "title" -> Seq("description", "tags"))

    val text1 = Text(
      query = "foo",
      path = "title" -> Seq("description", "tags"))

    val phrase1 = Phrase(
      query = "foo",
      path = "title" -> Seq("description", "tags"))

    "support operator" >> {
      val doc = makeSearch(AggFramework) _

      "<custom>" in {
        val expected = BSONDocument("foo" -> "bar")
        val op = Operator("custom", expected)

        op.name must_=== "custom" and (doc(op) must_=== expected)
      }

      "term" >> {
        "with minimal options" in {
          term1.name must_=== "term" and {
            doc(term1) must_=== BSONDocument(
              "query" -> "foo",
              "path" -> Seq("title", "description", "tags"))
          }
        }

        "with score" in {
          doc(Term(
            query = "foo" -> Seq("bar"),
            path = "title" -> Nil,
            score = Some(AtlasScore.boost(2.1D)))) must_=== BSONDocument(
            "query" -> Seq("foo", "bar"),
            "path" -> "title",
            "score" -> BSONDocument("boost" -> 2.1D))

        }

        Fragments.foreach[(Term.Modifier, String)](Seq(
          Term.Wildcard -> "wildcard",
          Term.Regex -> "regex",
          Term.Prefix -> "prefix")) {
          case (mod, flag) => s"with simple modifier ${flag}" in {
            doc(Term(
              query = "foo",
              path = "title",
              modifier = Some(mod),
              score = Some(AtlasScore.constant(3.45D)))) must_=== BSONDocument(
              "query" -> "foo",
              "path" -> "title",
              flag -> true,
              "score" -> BSONDocument("constant" -> 3.45D))
          }
        }

        "with fuzzy modifier" in {
          doc(Term(
            query = "foo",
            path = "title",
            modifier = Some(Term.Fuzzy(
              maxEdits = 3,
              prefixLength = 2)))) must_=== BSONDocument(
            "query" -> "foo",
            "path" -> "title",
            "fuzzy" -> BSONDocument(
              "maxEdits" -> 3,
              "prefixLength" -> 2))

        }
      }

      "text" >> {
        "with minimal options" in {
          text1.name must_=== "text" and {
            doc(text1) must_=== BSONDocument(
              "query" -> "foo",
              "path" -> Seq("title", "description", "tags"))
          }
        }

        "with score" in {
          doc(Text(
            query = "foo" -> Seq("bar"),
            path = "title" -> Nil,
            score = Some(AtlasScore.boost(2.1D)))) must_=== BSONDocument(
            "query" -> Seq("foo", "bar"),
            "path" -> "title",
            "score" -> BSONDocument("boost" -> 2.1D))

        }

        "with fuzzy" in {
          doc(Text(
            query = "foo" -> Seq.empty,
            path = "title" -> Nil,
            fuzzy = Some(Text.Fuzzy(
              maxEdits = 3,
              prefixLength = 2,
              maxExpansions = 5)))) must_=== BSONDocument(
            "query" -> "foo",
            "path" -> "title",
            "fuzzy" -> BSONDocument(
              "maxEdits" -> 3,
              "prefixLength" -> 2,
              "maxExpansions" -> 5))

        }
      }

      "phrase" >> {
        "with minimal options" in {
          phrase1.name must_=== "phrase" and {
            doc(phrase1) must_=== BSONDocument(
              "query" -> "foo",
              "path" -> Seq("title", "description", "tags"),
              "slop" -> 0)
          }
        }

        "with score" in {
          doc(Phrase(
            query = "foo" -> Seq("bar"),
            path = "title",
            slop = 5,
            score = Some(AtlasScore.boost(2.1D)))) must_=== BSONDocument(
            "query" -> Seq("foo", "bar"),
            "path" -> "title",
            "slop" -> 5,
            "score" -> BSONDocument("boost" -> 2.1D))

        }
      }

      "compound" >> {
        import Compound.newBuilder

        val builder1 = newBuilder(Compound.must, term1)

        "with minimal options" in {
          val op = builder1.result()

          op.name must_=== "compound" and {
            doc(op) must_=== BSONDocument(
              "must" -> Seq(BSONDocument("term" -> doc(term1))))
          } and {
            val expected = BSONDocument(
              "must" -> Seq(BSONDocument("term" -> doc(term1))),
              "minimumShouldMatch" -> 2)

            doc(builder1.minimumShouldMatch(2).
              result()) must_=== expected and {
              doc(newBuilder(
                Compound.must, term1, 2).result()) must_=== expected
            }
          }
        }

        "with multiple 'must' operators" in {
          val builder = newBuilder(Compound.must, term1, Seq(text1))
          val expected1 = BSONDocument("must" -> Seq(
            BSONDocument("term" -> doc(term1)),
            BSONDocument("text" -> doc(text1))))

          doc(builder.result()) must_=== expected1 and {
            doc(newBuilder(Compound.must, term1 -> Seq(text1)).
              result()) must_=== expected1
          } and {
            val expected2 = expected1 ++ BSONDocument("minimumShouldMatch" -> 3)

            doc(builder.minimumShouldMatch(3).
              result()) must_=== expected2 and {
              doc(newBuilder(
                Compound.must, term1, Seq(text1), 3).
                result()) must_=== expected2
            } and {
              doc(newBuilder(
                Compound.must, term1 -> Seq(text1), 3).
                result()) must_=== expected2
            }
          }
        }

        "with multiple clauses" in {
          doc(builder1.append(Compound.mustNot, phrase1).
            result()) must_=== BSONDocument(
            "must" -> Seq(BSONDocument("term" -> doc(term1))),
            "mustNot" -> Seq(BSONDocument("phrase" -> doc(phrase1)))) and {
              val expected = BSONDocument(
                "must" -> Seq(BSONDocument("term" -> doc(term1))),
                "mustNot" -> Seq(
                  BSONDocument("text" -> doc(text1)),
                  BSONDocument("phrase" -> doc(phrase1))))

              doc(builder1.append(
                Compound.mustNot, text1, Seq(phrase1)).
                result()) must_=== expected and {
                doc(builder1.append(
                  Compound.mustNot, text1 -> Seq(phrase1)).
                  result()) must_=== expected
              }
            }
        }

        "with overriden clause" in {
          doc(builder1.append(Compound.must, phrase1).
            result()) must_=== BSONDocument(
            "must" -> Seq(BSONDocument("phrase" -> doc(phrase1))))
        }
      }

      "exists" in {
        val op = Exists("title" -> Nil)

        op.name must_=== "exists" and {
          doc(op) must_=== BSONDocument(
            "path" -> "title") and {
              doc(Exists("title" -> Seq(
                "description", "tags"))) must_=== BSONDocument(
                "path" -> Seq("title", "description", "tags"))
            }
        }
      }

      "near" >> {
        "with minimal options" in {
          val op = Near(
            query = "foo" -> Seq.empty,
            path = "title" -> Seq("description", "tags"))

          op.name must_=== "near" and {
            doc(op) must_=== BSONDocument(
              "query" -> "foo",
              "path" -> Seq("title", "description", "tags"))
          }
        }

        "with pivot and score" in {
          doc(Near(
            query = "foo" -> Seq("bar"),
            path = "title" -> Nil,
            pivot = Some(0.5D),
            score = Some(AtlasScore.boost(2.1D)))) must_=== BSONDocument(
            "query" -> Seq("foo", "bar"),
            "path" -> "title",
            "pivot" -> 0.5D,
            "score" -> BSONDocument("boost" -> 2.1D))

        }
      }

      "wildcard" >> {
        "with minimal options" in {
          val op = Wildcard(
            query = "foo" -> Seq.empty,
            path = "title" -> Seq(
              "description", "tags"))

          op.name must_=== "wildcard" and {
            doc(op) must_=== BSONDocument(
              "query" -> "foo",
              "path" -> Seq("title", "description", "tags"),
              "allowAnalyzedField" -> false)
          }
        }

        "with pivot and score" in {
          doc(Wildcard(
            query = "foo" -> Seq("bar"),
            path = "title" -> Nil,
            allowAnalyzedField = true,
            score = Some(AtlasScore.boost(2.1D)))) must_=== BSONDocument(
            "query" -> Seq("foo", "bar"),
            "path" -> "title",
            "allowAnalyzedField" -> true,
            "score" -> BSONDocument("boost" -> 2.1D))

        }
      }

      "regex" >> {
        "with minimal options" in {
          val op = Regex(
            query = "foo" -> Seq.empty,
            path = "title" -> Seq("description", "tags"))

          op.name must_=== "regex" and {
            doc(op) must_=== BSONDocument(
              "query" -> "foo",
              "path" -> Seq("title", "description", "tags"),
              "allowAnalyzedField" -> false)
          }
        }

        "with pivot and score" in {
          doc(Regex(
            query = "foo" -> Seq("bar"),
            path = "title",
            allowAnalyzedField = true,
            score = Some(AtlasScore.boost(2.1D)))) must_=== BSONDocument(
            "query" -> Seq("foo", "bar"),
            "path" -> "title",
            "allowAnalyzedField" -> true,
            "score" -> BSONDocument("boost" -> 2.1D))

        }
      }

      "range" >> {
        "with minimal options" in {
          val op = Range(
            path = "foo" -> Seq("bar"),
            start = Range.greaterThan(2),
            end = Range.lessThanOrEqual(7.5D))

          op.name must_=== "range" and {
            doc(op) must_=== BSONDocument(
              "path" -> Seq("foo", "bar"),
              "gt" -> 2,
              "lte" -> 7.5D)
          }
        }

        "with score" in {
          doc(Range(
            path = "foo",
            start = Range.greaterThan(2),
            end = Range.lessThanOrEqual(7.5D),
            score = Some(AtlasScore.boost(2.1D)))) must_=== BSONDocument(
            "path" -> "foo",
            "gt" -> 2,
            "lte" -> 7.5D,
            "score" -> BSONDocument("boost" -> 2.1D))

        }
      }

      "query string" >> {
        "with minimal options" in {
          val op = QueryString(
            defaultPath = "title",
            query = "Rocky AND (IV OR 4 OR Four)")

          op.name must_=== "queryString" and {
            doc(op) must_=== BSONDocument(
              "defaultPath" -> "title",
              "query" -> "Rocky AND (IV OR 4 OR Four)")
          }
        }

        "with score" in {
          doc(QueryString(
            defaultPath = "title",
            query = "Rocky AND (IV OR 4 OR Four)",
            score = Some(AtlasScore.boost(2.1D)))) must_=== BSONDocument(
            "defaultPath" -> "title",
            "query" -> "Rocky AND (IV OR 4 OR Four)",
            "score" -> BSONDocument("boost" -> 2.1D))

        }
      }
    }

    f"be serialized as $$search" >> {
      val doc = makeSearch(AggFramework) _
      val makePipe = reactivemongo.api.tests.makePipe(AggFramework)(_)

      Fragments.foreach(Seq[(Operator, BSONDocument)](
        term1 -> BSONDocument(
          "query" -> "foo",
          "path" -> Seq("title", "description", "tags")),
        text1 -> BSONDocument(
          "query" -> "foo",
          "path" -> Seq("title", "description", "tags")),
        phrase1 -> BSONDocument(
          "query" -> "foo",
          "path" -> Seq("title", "description", "tags"),
          "slop" -> 0))) {
        case (sop, doc) => s"with ${sop.name} operator" in {
          makePipe(AtlasSearch(sop)) must_=== BSONDocument(
            f"$$search" -> BSONDocument(sop.name -> doc))
        }
      }

      "with compound operator" in {
        val builder = Compound.newBuilder(Compound.must, term1).
          append(Compound.mustNot, text1 -> Seq(phrase1))

        makePipe(AtlasSearch(builder.result())) must_=== BSONDocument(
          f"$$search" -> BSONDocument("compound" -> BSONDocument(
            "must" -> Seq(BSONDocument("term" -> doc(term1))),
            "mustNot" -> Seq(
              BSONDocument("text" -> doc(text1)),
              BSONDocument("phrase" -> doc(phrase1))))))

      }
    }
  }

  section("unit")

  // ---

  def index(
    key: Seq[(String, IndexType)],
    name: Option[String] = None,
    unique: Boolean = false,
    background: Boolean = false,
    sparse: Boolean = false,
    version: Option[Int] = None, // let MongoDB decide
    partialFilter: Option[pack.Document] = None,
    options: pack.Document = builder.document(Seq.empty)) = Index(pack)(key, name, unique, background, sparse, None, None, None, None, None, None, None, None, None, None, None, None, None, version, partialFilter, options)

  case class User(
    _id: Int,
    name: String,
    favorites: Seq[String])

  case class Location(lon: Double, lat: Double)

  case class ZipCode(
    _id: String, city: String, state: String,
    population: Long, location: Location)

  case class Product(
    _id: Int, sku: Option[String] = None,
    description: Option[String] = None,
    instock: Option[Int] = None)

  case class InventoryReport(
    _id: Int,
    item: Option[String] = None,
    price: Option[Int] = None,
    quantity: Option[Int] = None,
    docs: List[Product] = Nil)

  case class QuizStdDev(_id: Int, stdDev: Double)

  case class GeoPoint(coordinates: List[Double])
  case class GeoDistance(calculated: Int, loc: GeoPoint)
  case class GeoPlace(
    loc: GeoPoint,
    name: String,
    category: String,
    dist: GeoDistance)

  case class Subsection(
    subtitle: String,
    tags: List[String],
    content: String)
  case class Redaction(
    title: String,
    tags: List[String],
    year: Int,
    subsections: List[Subsection])

  case class Score(name: String, score: Int)
  implicit val scoreReader: BSONDocumentReader[Score] = Macros.reader[Score]

  case class QuizScores(_id: Int, scores: Set[Score])

  implicit val quizScoresReader: BSONDocumentReader[QuizScores] =
    BSONDocumentReader[QuizScores] { doc =>
      (for {
        i <- doc.getAsTry[Int]("_id")
        s <- doc.getAsTry[Set[Score]]("scores")
      } yield QuizScores(i, s)).get
    }
}
