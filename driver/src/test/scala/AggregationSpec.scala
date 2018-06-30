import scala.collection.immutable.{ ListSet, Set }

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import reactivemongo.bson._
import reactivemongo.api.{ Cursor, CursorProducer, WrappedCursor }
import reactivemongo.api.collections.bson.BSONCollection

import org.specs2.concurrent.ExecutionEnv

class AggregationSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification
  with org.specs2.specification.AfterAll {

  "Aggregation framework" title

  sequential

  // ---

  import Common.{ timeout, slowTimeout }

  lazy val (db, slowDb) = Common.databases(s"reactivemongo-agg-${System identityHashCode this}", Common.connection, Common.slowConnection)

  def afterAll: Unit = { db.drop(); () }

  val zipColName = s"zipcodes${System identityHashCode this}"
  lazy val coll: BSONCollection = {
    import reactivemongo.api.indexes._, IndexType._

    val c: BSONCollection = db(zipColName)
    scala.concurrent.Await.result(c.create().flatMap(_ =>
      c.indexesManager.ensure(Index(
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
      import reactivemongo.api.commands.{ bson => bsoncommands }
      import bsoncommands.BSONAggregationFramework.IndexStatsResult
      import bsoncommands.BSONAggregationResultImplicits.BSONIndexStatsReader

      val result = coll.aggregateWith1[IndexStatsResult]() { framework =>
        import framework._

        IndexStats -> List(Sort(Ascending("name")))
      }.headOption

      result must beLike[Option[IndexStatsResult]] {
        case Some(IndexStatsResult("city_text_state_text", k2, _, _)) =>
          k2.getAs[String]("_fts") must beSome("text") and {
            k2.getAs[BSONNumberLike]("_ftsx").map(_.toInt) must beSome(1)
          }
      }.await(0, timeout)
    } tag "not_mongo26"

    "be inserted" in {
      def insert(data: List[ZipCode]): Future[Unit] = data.headOption match {
        case Some(zip) => coll.insert(zip).flatMap(_ => insert(data.tail))
        case _         => Future.successful({})
      }

      insert(zipCodes) aka "insert" must beEqualTo({}).await(1, timeout) and {
        coll.count() aka "c#1" must beEqualTo(4).await(1, slowTimeout)
      } and {
        slowZipColl.count() aka "c#2" must beEqualTo(4).await(1, slowTimeout)
      }
    }

    "return states with populations above 10000000" >> {
      // http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set/#return-states-with-populations-above-10-million
      val expected = List(
        document("_id" -> "JP", "totalPop" -> 13185702L),
        document("_id" -> "NY", "totalPop" -> 19746227L))

      def withRes[T](c: BSONCollection)(f: Future[List[BSONDocument]] => T): T = {
        f(c.aggregateWith1[BSONDocument]() { framework =>
          import framework._

          Group(BSONString(f"$$state"))("totalPop" -> SumField("population")) -> List(
            Match(document("totalPop" -> document(f"$$gte" -> 10000000L))))
        }.collect[List](Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]()))
      }

      "with the default connection" in {
        withRes(coll) {
          _ aka "results" must beEqualTo(expected).await(1, timeout)
        }
      }

      "with the slow connection" in {
        withRes(slowZipColl) {
          _ aka "results" must beEqualTo(expected).await(1, slowTimeout)
        }
      }

      "using a view" in {
        import coll.BatchCommands.AggregationFramework
        import AggregationFramework.{ Group, Match, SumField }

        val viewName = s"pop10m${System identityHashCode this}"

        def result = for {
          _ <- coll.createView(
            name = viewName,
            operator = Group(BSONString(f"$$state"))(
              "totalPop" -> SumField("population")),
            pipeline = Seq(
              Match(document("totalPop" -> document(f"$$gte" -> 10000000L)))))
          view: BSONCollection = db(viewName)
          res <- view.find(
            BSONDocument.empty).cursor[BSONDocument]().collect[List](
              expected.size + 2, Cursor.FailOnError[List[BSONDocument]]())
        } yield res

        result must beEqualTo(expected).await(1, timeout)
      } tag "gt_mongo32"

      "with expected count" in {
        import coll.BatchCommands.AggregationFramework
        import AggregationFramework.{ Group, SumAll }
        val result = coll.aggregatorContext[BSONDocument](Group(BSONString(f"$$state"))("count" -> SumAll)).prepared.cursor
          .collect[Set](Int.MaxValue, Cursor.FailOnError[Set[BSONDocument]]())

        result must beTypedEqualTo(Set(
          document("_id" -> "JP", "count" -> 2),
          document("_id" -> "FR", "count" -> 1),
          document("_id" -> "NY", "count" -> 1))).await(1, timeout)
      }
    }

    "explain simple result" in {
      val result = coll.aggregateWith1[BSONDocument](explain = true) {
        framework =>
          import framework._

          Group(BSONString(f"$$state"))("totalPop" -> SumField("population")) -> List(
            Match(document("totalPop" -> document(f"$$gte" -> 10000000L))))
      }.headOption

      result aka "results" must beLike[Option[BSONDocument]] {
        case Some(explainResult) => explainResult.getAs[BSONArray]("stages") must beSome
      }.await(1, timeout)
    }

    "return average city population by state" >> {
      // See http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set/#return-average-city-population-by-state
      val expected = List(
        document("_id" -> "NY", "avgCityPop" -> 19746227D),
        document("_id" -> "FR", "avgCityPop" -> 148169D),
        document("_id" -> "JP", "avgCityPop" -> 6592851D))

      def withCtx[T](c: BSONCollection)(f: (c.BatchCommands.AggregationFramework.Group, List[c.PipelineOperator]) => T): T = {
        import c.BatchCommands.AggregationFramework
        import AggregationFramework.{ Group, SumField }

        val firstOp = Group(document(
          "state" -> f"$$state", "city" -> f"$$city"))(
          "pop" -> SumField("population"))

        val pipeline = List(
          Group(BSONString(f"$$_id.state"))("avgCityPop" ->
            AggregationFramework.AvgField("pop")))

        f(firstOp, pipeline)
      }

      "successfully as a single batch" in {
        withCtx(coll) { (firstOp, pipeline) =>
          val result = coll.aggregateWith1[BSONDocument]() {
            framework => firstOp -> pipeline
          }.collect[List](Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]())

          result aka "results" must beEqualTo(expected).await(1, timeout)
        }
      }

      "with cursor" >> {
        def collect(c: BSONCollection, upTo: Int = Int.MaxValue) = withCtx(c) { (firstOp, pipeline) =>
          c.aggregateWith1[BSONDocument](batchSize = Some(1)) {
            framework => firstOp -> pipeline
          }.collect[List](upTo, Cursor.FailOnError[List[BSONDocument]]())
        }

        "without limit (maxDocs)" in {
          collect(coll) must beEqualTo(expected).await(1, timeout)
        }

        "with limit (maxDocs)" in {
          collect(coll, 2) must beEqualTo(expected take 2).await(1, timeout)
        }

        "with metadata sort" in {
          coll.aggregateWith1[ZipCode]() { framework =>
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

            firstOp -> pipeline
          }.collect[List](4, Cursor.FailOnError[List[ZipCode]]()).
            aka("aggregated") must beTypedEqualTo(jpCodes).await(1, timeout)

        }
      }

      "with produced cursor" >> {
        "without limit (maxDocs)" in {
          withCtx(coll) { (firstOp, pipeline) =>
            val cursor = coll.aggregatorContext[BSONDocument](
              firstOp, pipeline, batchSize = Some(1)).prepared.cursor

            cursor.collect[List](
              Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]()) must beTypedEqualTo(expected).await(1, timeout)
          }
        }

        "of expected type" in {
          withCtx(coll) { (firstOp, pipeline) =>
            // Custom cursor support
            trait FooCursor[T] extends Cursor[T] { def foo: String }

            class DefaultFooCursor[T](val wrappee: Cursor[T])
              extends FooCursor[T] with WrappedCursor[T] {
              val foo = "Bar"
            }

            implicit def fooProducer[T] = new CursorProducer[T] {
              type ProducedCursor = FooCursor[T]
              def produce(base: Cursor[T]) = new DefaultFooCursor(base)
            }

            // Aggregation itself
            val aggregator = coll.aggregatorContext[BSONDocument](
              firstOp, pipeline, batchSize = Some(1)).prepared[FooCursor]

            aggregator.cursor.isInstanceOf[FooCursor[BSONDocument]].
              aka("cursor") must beEqualTo(true)
          }
        }
      }
    }

    "return largest and smallest cities by state" in {
      // See http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set/#return-largest-and-smallest-cities-by-state
      import coll.BatchCommands.AggregationFramework
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

      val groupPipeline = List(
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

      coll.aggregateWith1[BSONDocument]() {
        framework =>
          import framework._

          Group(document("state" -> f"$$state", "city" -> f"$$city"))(
            "pop" -> SumField("population")) -> groupPipeline
      }.collect[List](Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]()) must beTypedEqualTo(expected).await(1, timeout) and {
        coll.aggregateWith1[BSONDocument]() {
          framework =>
            import framework._

            Group(document("state" -> f"$$state", "city" -> f"$$city"))(
              "pop" -> SumField("population")) -> (groupPipeline :+ Limit(2))
        }.collect[List](Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]()) must beTypedEqualTo(expected take 2).await(1, timeout)
      } and {
        coll.aggregateWith1[BSONDocument]() {
          framework =>
            import framework._

            Group(document("state" -> f"$$state", "city" -> f"$$city"))(
              "pop" -> SumField("population")) -> (groupPipeline :+ Skip(2))
        }.collect[List](Int.MaxValue, Cursor.FailOnError[List[BSONDocument]]()) must beTypedEqualTo(expected drop 2).await(1, timeout)
      }
    }

    "return distinct states" >> {
      def distinctSpec(c: BSONCollection, timeout: FiniteDuration) = c.distinct[String, ListSet]("state").
        aka("states") must beTypedEqualTo(ListSet("NY", "FR", "JP")).
        await(1, timeout)

      "with the default connection" in {
        distinctSpec(coll, timeout)
      }

      "with the slow connection" in {
        distinctSpec(slowZipColl, slowTimeout)
      }
    }

    "return a random sample" in {
      import coll.BatchCommands.AggregationFramework.Sample

      coll.aggregatorContext[ZipCode](Sample(2)).prepared.cursor
        .collect[List](Int.MaxValue, Cursor.FailOnError[List[ZipCode]]())
        .map(_.count(zipCodes.contains)) must beEqualTo(2).await(0, timeout)
    } tag "not_mongo26"
  }

  "Inventory #1" should {
    val orders: BSONCollection = db.collection(s"agg-orders-1-${System identityHashCode this}")
    val inventory: BSONCollection = db.collection(
      s"agg-inv-1-${System identityHashCode orders}")

    "be provided with order fixtures" in {
      (for {
        // orders
        _ <- orders.insert(BSONDocument(
          "_id" -> 1, "item" -> "abc", "price" -> 12, "quantity" -> 2))
        _ <- orders.insert(BSONDocument(
          "_id" -> 2, "item" -> "jkl", "price" -> 20, "quantity" -> 1))
        _ <- orders.insert(BSONDocument("_id" -> 3))

        // inventory
        _ <- inventory.insert(BSONDocument("_id" -> 1, "sku" -> "abc",
          "description" -> "product 1", "instock" -> 120))
        _ <- inventory.insert(BSONDocument("_id" -> 2, "sku" -> "def",
          "description" -> "product 2", "instock" -> 80))
        _ <- inventory.insert(BSONDocument("_id" -> 3, "sku" -> "ijk",
          "description" -> "product 3", "instock" -> 60))
        _ <- inventory.insert(BSONDocument("_id" -> 4, "sku" -> "jkl",
          "description" -> "product 4", "instock" -> 70))
        _ <- inventory.insert(BSONDocument(
          "_id" -> 5,
          "sku" -> Option.empty[String], "description" -> "Incomplete"))
        _ <- inventory.insert(BSONDocument("_id" -> 6))
      } yield ()) must beEqualTo({}).await(0, timeout)
    } tag "not_mongo26"

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

      import coll.BatchCommands.AggregationFramework
      import AggregationFramework.Lookup

      orders.aggregatorContext[InventoryReport](Lookup(inventory.name, "item", "sku", "docs")).prepared.cursor
        .collect[List](Int.MaxValue, Cursor.FailOnError[List[InventoryReport]]()) must beTypedEqualTo(expected).await(0, timeout)
    } tag "not_mongo26"

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

      import coll.BatchCommands.AggregationFramework
      import AggregationFramework.GraphLookup

      orders.aggregatorContext[InventoryReport](GraphLookup(inventory.name, BSONString(f"$$item"), "item", "sku", "docs")).prepared.cursor
        .collect[List](Int.MaxValue, Cursor.FailOnError[List[InventoryReport]]()) must beTypedEqualTo(expected).await(0, timeout)
    } tag "gt_mongo32"

    "automatically group in bucket" in {
      // https://docs.mongodb.com/manual/reference/operator/aggregation/bucketAuto/

      val expected = Set(
        document(
          "_id" -> document(
            "min" -> BSONNull,
            "max" -> 12),
          "count" -> 1),
        document(
          "_id" -> document(
            "min" -> 12,
            "max" -> 20),
          "count" -> 1),
        document(
          "_id" -> document(
            "min" -> 20,
            "max" -> 20),
          "count" -> 1))

      orders.aggregateWith1[BSONDocument]() { framework =>
        import framework.BucketAuto

        BucketAuto(BSONString(f"$$price"), 3, None)() -> List.empty
      }.collect[Set](Int.MaxValue, Cursor.FailOnError[Set[BSONDocument]]()).
        aka("buckets") must beEqualTo(expected).await(0, timeout)
    } tag "gt_mongo32"

    // Sales

    val sales: BSONCollection = db.collection(s"agg-sales-A-${System identityHashCode this}")
    implicit val saleItemHandler: BSONDocumentHandler[SaleItem] = Macros.handler[SaleItem]
    implicit val saleHandler: BSONDocumentHandler[Sale] = Macros.handler[Sale]

    "be provided with sale fixtures" in {
      def fixtures = Seq(
        document("_id" -> 0, "items" -> array(
          document("itemId" -> 43, "quantity" -> 2, "price" -> 10),
          document("itemId" -> 2, "quantity" -> 1, "price" -> 240))),
        document("_id" -> 1, "items" -> array(
          document("itemId" -> 23, "quantity" -> 3, "price" -> 110),
          document("itemId" -> 103, "quantity" -> 4, "price" -> 5),
          document("itemId" -> 38, "quantity" -> 1, "price" -> 300))),
        document("_id" -> 2, "items" -> array(
          document("itemId" -> 4, "quantity" -> 1, "price" -> 23))))

      Future.sequence(fixtures.map { doc => sales.insert(doc) }).
        map(_ => {}) must beEqualTo({}).await(0, timeout)
    } tag "not_mongo26"

    "filter when using a '$project' stage" in {
      // See https://docs.mongodb.com/master/reference/operator/aggregation/filter/#example

      import sales.BatchCommands.AggregationFramework.{
        Ascending,
        Sort
      }

      def expected = List(
        Sale(_id = 0, items = List(SaleItem(2, 1, 240))),
        Sale(_id = 1, items = List(
          SaleItem(23, 3, 110), SaleItem(38, 1, 300))),
        Sale(_id = 2, items = Nil))
      val sort = Sort(Ascending("_id"))

      sales.aggregateWith1[Sale]() {
        framework =>
          import framework._

          Project(document("items" -> Filter(
            input = BSONString(f"$$items"),
            as = "item",
            cond = document(f"$$gte" -> array(f"$$$$item.price", 100))))) -> List(sort)
      }.collect[List](Int.MaxValue, Cursor.FailOnError[List[Sale]]()) must beTypedEqualTo(expected).await(0, timeout)
    } tag "not_mongo26"
  }

  "Inventory #2" should {
    val orders: BSONCollection = db.collection(s"agg-order-2-${System identityHashCode this}")
    val inventory: BSONCollection = db.collection(
      s"agg-inv-2-${System identityHashCode orders}")

    "be provided the fixtures" in {
      (for {
        // orders
        _ <- orders.insert(BSONDocument(
          "_id" -> 1, "item" -> "MON1003", "price" -> 350, "quantity" -> 2,
          "specs" -> BSONArray("27 inch", "Retina display", "1920x1080"),
          "type" -> "Monitor"))

        // inventory
        _ <- inventory.insert(BSONDocument("_id" -> 1, "sku" -> "MON1003",
          "type" -> "Monitor", "instock" -> 120, "size" -> "27 inch",
          "resolution" -> "1920x1080"))
        _ <- inventory.insert(BSONDocument("_id" -> 2, "sku" -> "MON1012",
          "type" -> "Monitor", "instock" -> 85, "size" -> "23 inch",
          "resolution" -> "1920x1080"))
        _ <- inventory.insert(BSONDocument("_id" -> 3, "sku" -> "MON1031",
          "type" -> "Monitor", "instock" -> 60, "size" -> "23 inch",
          "displayType" -> "LED"))
      } yield ()) must beEqualTo({}).await(0, timeout)
    } tag "not_mongo26"

    "so the joined documents are returned" in {
      import orders.BatchCommands.AggregationFramework
      import AggregationFramework.{ Lookup, Match }

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

      orders.aggregateWith1[BSONDocument]() {
        framework =>
          import framework._

          UnwindField("specs") -> afterUnwind
      }.headOption must beSome(expected).await(0, timeout) and {
        orders.aggregateWith1[BSONDocument]() {
          framework =>
            import framework._

            Unwind("specs", None, Some(true)) -> afterUnwind
        }.headOption must beSome(expected).await(0, timeout)
      }
    } tag "not_mongo26"
  }

  f"Aggregation result for '$$out'" should {
    // https://docs.mongodb.com/master/reference/operator/aggregation/out/#example

    val books: BSONCollection = db.collection(s"books-1-${System identityHashCode this}")

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

      // TODO: bulk insert
      Future.sequence(fixtures.map { doc => books.insert(doc) }).map(_ => {}).
        aka("fixtures") must beEqualTo({}).await(0, timeout)
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

      books.aggregateWith1[Author]() {
        framework =>
          import framework._

          Sort(Ascending("title")) -> List(
            Group(BSONString(f"$$author"))("books" -> PushField("title")),
            Out(outColl))
      }.collect[List](Int.MaxValue, Cursor.FailOnError[List[Author]]()).map(_ => {}) must beEqualTo({}).await(0, timeout) and {
        db.collection[BSONCollection](outColl).find(BSONDocument.empty).cursor[Author]().
          collect[List](3, Cursor.FailOnError[List[Author]]()) must beTypedEqualTo(
            List(
              "Homer" -> List("Iliad", "The Odyssey"),
              "Dante" -> List("Divine Comedy", "Eclogues", "The Banquet"))).await(0, timeout)
      }
    }

    "be added to set" in {
      implicit val catReader: BSONDocumentReader[AuthorCatalog] = BSONDocumentReader[AuthorCatalog] { doc =>
        (for {
          id <- doc.getAsTry[String]("_id")
          bs <- doc.getAsTry[Set[String]]("books")
        } yield AuthorCatalog(id, bs)).get
      }

      books.aggregateWith1[AuthorCatalog]() {
        framework =>
          import framework._

          Sort(Ascending("title")) -> List(Group(BSONString(f"$$author"))(
            "books" -> AddFieldToSet("title")))
      }.collect[Set](Int.MaxValue, Cursor.FailOnError[Set[AuthorCatalog]]()) must beTypedEqualTo(Set(
        AuthorCatalog(_id = "Homer", books = Set("The Odyssey", "Iliad")),
        AuthorCatalog(_id = "Dante", books = Set(
          "The Banquet", "Eclogues", "Divine Comedy")))).await(1, timeout)
    }
  }

  "Aggregation result for '$stdDevPop'" should {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevPop/#examples

    val contest: BSONCollection = db.collection(s"contest-1-${System identityHashCode this}")

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

      Future.sequence(fixtures.map { doc => contest.insert(doc) }).map(_ => {}).
        aka("fixtures") must beEqualTo({}).await(0, timeout)
    }

    "return the standard deviation of each quiz" in {
      implicit val reader: BSONDocumentReader[QuizStdDev] = Macros.reader[QuizStdDev]

      /*
       db.contest.aggregate([
         { $group: { _id: "$quiz", stdDev: { $stdDevPop: "$score" } } }
       ])
      */
      contest.aggregateWith1[QuizStdDev]() {
        framework =>
          import framework._

          Group(BSONString(f"$$quiz"))("stdDev" -> StdDevPopField("score")) -> List(
            Sort(Ascending("_id")))
      }.collect[List](Int.MaxValue, Cursor.FailOnError[List[QuizStdDev]]()).aka(f"$$stdDevPop results") must beTypedEqualTo(List(
        QuizStdDev(1, 8.04155872120988D), QuizStdDev(2, 8.04155872120988D))).await(0, timeout)

      /*
       { "_id" : 1, "stdDev" : 8.04155872120988 }
       { "_id" : 2, "stdDev" : 8.04155872120988 }
       */
    } tag "not_mongo26"

    "return a sum as hash per quiz" in {
      import coll.BatchCommands.AggregationFramework
      import AggregationFramework.{ Group, Sum }

      contest.aggregatorContext[BSONDocument](
        Group(BSONString(f"$$quiz"))(
          "hash" -> Sum(document(f"$$multiply" -> array(f"$$_id", f"$$score"))))).prepared.cursor
        .collect[Set](Int.MaxValue, Cursor.FailOnError[Set[BSONDocument]]()) must beTypedEqualTo(Set(
          document("_id" -> 2, "hash" -> 1261),
          document("_id" -> 1, "hash" -> 478))).await(1, timeout)
    }

    "return the maximum score per quiz" in {
      import coll.BatchCommands.AggregationFramework
      import AggregationFramework.{ Group, MaxField }

      contest.aggregatorContext[BSONDocument](
        Group(BSONString(f"$$quiz"))("maxScore" -> MaxField("score"))).prepared.cursor
        .collect[Set](Int.MaxValue, Cursor.FailOnError[Set[BSONDocument]]()) must beTypedEqualTo(Set(
          document("_id" -> 2, "maxScore" -> 96),
          document("_id" -> 1, "maxScore" -> 90))).await(1, timeout)
    }

    "return a max as hash per quiz" in {
      import coll.BatchCommands.AggregationFramework
      import AggregationFramework.{ Group, Max }

      contest.aggregatorContext[BSONDocument](
        Group(BSONString(f"$$quiz"))(
          "maxScore" -> Max(document(
            f"$$multiply" -> array(f"$$_id", f"$$score"))))).prepared.cursor
        .collect[Set](Int.MaxValue, Cursor.FailOnError[Set[BSONDocument]]()) must beTypedEqualTo(Set(
          document("_id" -> 2, "maxScore" -> 492),
          document("_id" -> 1, "maxScore" -> 213))).await(1, timeout)
    }

    "return the minimum score per quiz" in {
      import coll.BatchCommands.AggregationFramework
      import AggregationFramework.{ Group, MinField }

      contest.aggregatorContext[BSONDocument](Group(BSONString(f"$$quiz"))("minScore" -> MinField("score"))).prepared.cursor
        .collect[Set](Int.MaxValue, Cursor.FailOnError[Set[BSONDocument]]()) must beTypedEqualTo(Set(
          document("_id" -> 2, "minScore" -> 77),
          document("_id" -> 1, "minScore" -> 71))).await(1, timeout)
    }

    "return a min as hash per quiz" in {
      import coll.BatchCommands.AggregationFramework
      import AggregationFramework.{ Group, Min }

      contest.aggregatorContext[BSONDocument](
        Group(BSONString(f"$$quiz"))(
          "minScore" -> Min(document(
            f"$$multiply" -> array(f"$$_id", f"$$score"))))).prepared.cursor
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

      import coll.BatchCommands.AggregationFramework
      import AggregationFramework.{ Group, Push }

      contest.aggregatorContext[QuizScores](
        Group(BSONString(f"$$quiz"))(
          "scores" -> Push(document("name" -> f"$$name", "score" -> f"$$score")))).prepared.cursor
        .collect[Set](Int.MaxValue, Cursor.FailOnError[Set[QuizScores]]()) must beTypedEqualTo(expected).await(1, timeout)
    }

    "add name and score to a set per quiz group" in {
      val expected = Set(
        QuizScores(2, Set(
          Score("ty", 82), Score("annT", 77), Score("li", 96))),
        QuizScores(1, Set(
          Score("dave123", 85), Score("dave2", 90), Score("ahn", 71))))

      import coll.BatchCommands.AggregationFramework
      import AggregationFramework.{ Group, AddToSet }

      contest.aggregatorContext[QuizScores](
        Group(BSONString(f"$$quiz"))(
          "scores" -> AddToSet(document("name" -> f"$$name", "score" -> f"$$score")))).prepared.cursor
        .collect[Set](Int.MaxValue, Cursor.FailOnError[Set[QuizScores]]()) must beTypedEqualTo(expected).await(1, timeout)
    }
  }

  "Aggregation result '$stdDevSamp'" should {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevSamp/#example

    val contest: BSONCollection = db.collection(s"contest-2-${System identityHashCode this}")

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

      Future.sequence(fixtures.map { doc => contest.insert(doc) }).map(_ => {}).
        aka("fixtures") must beEqualTo({}).await(0, timeout)
    } tag "not_mongo26"

    "return the standard deviation of user ages" in {
      //TODO: Remove: implicit val reader = Macros.reader[QuizStdDev]

      // { "_id" : null, "ageStdDev" : 11.135528725660043 }
      val expected = BSONDocument("_id" -> BSONNull, "ageStdDev" -> 11.135528725660043D)

      /*
       db.users.aggregate([
         { $sample: { size: 100 } },
         { $group: { _id: null, ageStdDev: { $stdDevSamp: "$age" } } }
       ])
      */

      contest.aggregateWith1[BSONDocument]() {
        framework =>
          import framework._

          Sample(100) -> List(Group(BSONNull)(
            "ageStdDev" -> StdDevSamp(BSONString(f"$$age"))))
      }.headOption must beSome(expected).await(0, timeout)
    } tag "not_mongo26"
  }

  "Geo-indexed documents" should {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/geoNear/#example

    val places: BSONCollection = db(s"places${System identityHashCode this}")

    "must be inserted" in {
      import reactivemongo.api.indexes._, IndexType._

      places.create().flatMap { _ =>
        places.indexesManager.ensure(Index(List("loc" -> Geo2DSpherical)))
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

        Future.sequence(Seq(
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
            "category" -> "Airport")).map { doc => places.insert(doc) }).
          map(_ => {}) must beEqualTo({}).await(0, timeout)
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
          calc <- doc.getAsTry[BSONNumberLike]("calculated").map(_.toInt)
          loc <- doc.getAsTry[GeoPoint]("loc")
        } yield GeoDistance(calc, loc)).get
      }
      implicit val placeReader: BSONDocumentReader[GeoPlace] = Macros.reader[GeoPlace]

      import coll.BatchCommands.AggregationFramework
      import AggregationFramework.GeoNear

      places.aggregatorContext[GeoPlace](
        GeoNear(document(
          "type" -> "Point",
          "coordinates" -> array(-73.9667, 40.78)), distanceField = Some("dist.calculated"),
          minDistance = Some(1000),
          maxDistance = Some(5000),
          query = Some(document("type" -> "public")),
          includeLocs = Some("dist.loc"),
          limit = 5,
          spherical = true)).prepared.cursor
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
    val forecasts: BSONCollection = db(s"forecasts${System identityHashCode this}")

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

      forecasts.insert(BSONDocument(
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
              "tags" -> BSONArray("HCS")))))).map(_ => {}) must beEqualTo({}).await(0, timeout)
    }

    "be redacted" in {
      implicit val subsectionReader: BSONDocumentHandler[Subsection] = Macros.handler[Subsection]
      implicit val reader: BSONDocumentHandler[Redaction] = Macros.handler[Redaction]

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
      val result = forecasts.aggregateWith1[Redaction]() {
        framework =>
          import framework._

          Match(document("year" -> 2014)) -> List(
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
    val customers: BSONCollection = db(s"customers${System identityHashCode this}")

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
      customers.insert(document(
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
        "status" -> "A")).map(_ => {}) must beEqualTo({}).await(0, timeout)
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
      val result = customers.aggregateWith1[BSONDocument]() {
        framework =>
          import framework._

          Match(document("status" -> "A")) -> List(
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
    val fruits: BSONCollection = db(s"fruits${System identityHashCode this}")

    "be inserted" in {
      /*
      {
         "_id" : 1,
         "fruit" : [ "apples", "oranges" ],
         "in_stock" : { "oranges" : 20, "apples" : 60 },
         "on_order" : { "oranges" : 35, "apples" : 75 }
      }
       */
      fruits.insert(document(
        "_id" -> 1,
        "fruit" -> array("apples", "oranges"),
        "in_stock" -> document(
          "oranges" -> 20,
          "apples" -> 60),
        "on_order" -> document(
          "oranges" -> 35,
          "apples" -> 75))).map(_ => {}) must beEqualTo({}).await(0, timeout)
    }

    "and reshaped using $replaceRoot" in {
      val result = fruits.aggregateWith1[BSONDocument]() { framework =>
        import framework._

        Match(document("_id" -> 1)) -> List(ReplaceRootField("in_stock"))
      }.headOption

      result must beSome(document("oranges" -> 20, "apples" -> 60)).
        await(0, timeout)
    }
  }

  "Contacts" should {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot/#replaceroot-with-a-newly-created-document
    val contacts: BSONCollection = db(s"contacts${System identityHashCode this}")

    "be inserted" in {
      /*
      { "_id" : 1, "first_name" : "Gary", "last_name" : "Sheffield", "city" : "New York" }
       */
      contacts.insert(document(
        "_id" -> 1,
        "first_name" -> "Gary",
        "last_name" -> "Sheffield",
        "city" -> "New York")).
        map(_ => {}) must beTypedEqualTo({}).await(0, timeout)
    }

    "and reshaped using $replaceRoot" in {
      val result = contacts.aggregateWith1[BSONDocument]() { framework =>
        import framework._

        Match(document("_id" -> 1)) -> List(
          ReplaceRoot(document(
            "full_name" -> document(
              "$concat" -> array("$first_name", " ", "$last_name")))))
      }.headOption

      result must beSome(document(
        "full_name" -> "Gary Sheffield")).await(0, timeout)
    }
  }

  "Students" should {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/addFields/
    val students: BSONCollection =
      db(s"students${System identityHashCode this}")

    "be inserted" in {
      (for {
        _ <- students.insert(document(
          "_id" -> 1,
          "student" -> "Maya",
          "homework" -> BSONArray(10, 5, 10),
          "quiz" -> BSONArray(10, 8),
          "extraCredit" -> 0))
        _ <- students.insert(document(
          "_id" -> 2,
          "student" -> "Ryan",
          "homework" -> BSONArray(5, 6, 5),
          "quiz" -> BSONArray(8, 8),
          "extraCredit" -> 8))
      } yield ()) must beTypedEqualTo({}).await(0, timeout)
    }

    "be aggregated with $$sum on array fields" in {
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

      students.aggregateWith1[BSONDocument]() { framework =>
        import framework.AddFields

        AddFields(document(
          "totalHomework" -> document(f"$$sum" -> f"$$homework"),
          "totalQuiz" -> document(f"$$sum" -> f"$$quiz"))) -> List(
          AddFields(document(
            "totalScore" -> document(f"$$add" -> array(
              f"$$totalHomework", f"$$totalQuiz", f"$$extraCredit")))))
      }.collect[Set](Int.MaxValue, Cursor.FailOnError[Set[BSONDocument]]()).
        aka("aggregated") must beTypedEqualTo(expectedResults).await(0, timeout)
    }
  }
  section("gt_mongo32")

  // ---

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

  case class SaleItem(itemId: Int, quantity: Int, price: Int)
  case class Sale(_id: Int, items: List[SaleItem])

  case class AuthorCatalog(
    _id: String, // author name
    books: Set[String] // books titles
  )

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
