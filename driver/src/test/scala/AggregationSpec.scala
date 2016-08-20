import scala.collection.immutable.ListSet

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration

import reactivemongo.bson._
import reactivemongo.api.Cursor
import reactivemongo.api.collections.bson.BSONCollection

import org.specs2.concurrent.{ ExecutionEnv => EE }

class AggregationSpec extends org.specs2.mutable.Specification {
  "Aggregation framework" title

  import Common._

  sequential

  val zipColName = s"zipcodes${System identityHashCode this}"
  lazy val coll = {
    import ExecutionContext.Implicits.global
    import reactivemongo.api.indexes._, IndexType._

    val c = db(zipColName)
    scala.concurrent.Await.result(c.indexesManager.ensure(Index(
      List("city" -> Text, "state" -> Text)
    )).map(_ => c), timeout * 2)
  }
  lazy val slowZipColl = slowDb(zipColName)

  implicit val locationHandler = Macros.handler[Location]
  implicit val zipCodeHandler = Macros.handler[ZipCode]

  private val jpCodes = List(
    ZipCode("JP 13", "TOKYO", "JP", 13185502L,
      Location(35.683333, 139.683333)),
    ZipCode("AO", "AOGASHIMA", "JP", 200L, Location(32.457, 139.767))
  )

  private val zipCodes = List(
    ZipCode("10280", "NEW YORK", "NY", 19746227L,
      Location(-74.016323, 40.710537)),
    ZipCode("72000", "LE MANS", "FR", 148169L,
      Location(48.0077, 0.1984))
  ) ++ jpCodes

  // ---

  "Zip codes collection" should {
    "expose the index stats" in { implicit ee: EE =>
      import coll.BatchCommands.AggregationFramework.{
        Ascending,
        IndexStats,
        Sort
      }
      import reactivemongo.api.commands.{ bson => bsoncommands }
      import bsoncommands.BSONAggregationFramework.{
        IndexStatsResult,
        IndexStatAccesses
      }
      import bsoncommands.BSONAggregationResultImplicits.BSONIndexStatsReader

      val expected = IndexStatsResult(
        name = null,
        key = BSONDocument.empty,
        host = "foo",
        accesses = IndexStatAccesses(
          ops = 1L,
          since = new java.util.Date()
        )
      )

      coll.aggregate(IndexStats, List(Sort(Ascending("name")))).
        map(_.head[IndexStatsResult]) must beLike[List[IndexStatsResult]] {
          case IndexStatsResult("_id_", k1, _, _) ::
            IndexStatsResult("city_text_state_text", k2, _, _) :: Nil =>
            k1.getAs[BSONNumberLike]("_id").map(_.toInt) must beSome(1) and {
              k2.getAs[String]("_fts") must beSome("text")
            } and {
              k2.getAs[BSONNumberLike]("_ftsx").map(_.toInt) must beSome(1)
            }
        }.await(0, timeout)
    } tag "not_mongo26"

    "be inserted" in { implicit ee: EE =>
      def insert(data: List[ZipCode]): Future[Unit] = data.headOption match {
        case Some(zip) => coll.insert(zip).flatMap(_ => insert(data.tail))
        case _         => Future.successful({})
      }

      insert(zipCodes) aka "insert" must beEqualTo({}).await(1, timeout) and (
        coll.count() aka "count #1" must beEqualTo(4).await(1, slowTimeout)
      ).
        and(slowZipColl.count() aka "count #2" must beEqualTo(4).
          await(1, slowTimeout))

    }

    "return states with populations above 10000000" >> {
      // http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set/#return-states-with-populations-above-10-million
      val expected = List(
        document("_id" -> "JP", "totalPop" -> 13185702L),
        document("_id" -> "NY", "totalPop" -> 19746227L)
      )

      def withRes[T](c: BSONCollection)(f: Future[List[BSONDocument]] => T)(implicit ec: ExecutionContext) = {
        import c.BatchCommands.AggregationFramework
        import AggregationFramework.{ Group, Match, SumField }

        f(c.aggregate(Group(BSONString("$state"))(
          "totalPop" -> SumField("population")
        ), List(
          Match(document("totalPop" -> document("$gte" -> 10000000L)))
        )).map(_.firstBatch))
      }

      "with the default connection" in { implicit ee: EE =>
        withRes(coll) {
          _ aka "results" must beEqualTo(expected).await(1, timeout)
        }
      }

      "with the slow connection" in { implicit ee: EE =>
        withRes(slowZipColl) {
          _ aka "results" must beEqualTo(expected).await(1, slowTimeout)
        }
      }
    }

    "explain simple result" in { implicit ee: EE =>
      val expected = List(
        document("_id" -> "JP", "totalPop" -> 13185702L),
        document("_id" -> "NY", "totalPop" -> 19746227L)
      )

      import coll.BatchCommands.AggregationFramework
      import AggregationFramework.{ Group, Match, SumField }

      coll.aggregate(Group(BSONString("$state"))(
        "totalPop" -> SumField("population")
      ), List(
        Match(document("totalPop" ->
          document("$gte" -> 10000000L)))
      ), explain = true).map(_.firstBatch).
        aka("results") must beLike[List[BSONDocument]] {
          case explainResult :: Nil =>
            explainResult.getAs[BSONArray]("stages") must beSome
        }.await(1, timeout)
    }

    "return average city population by state" >> {
      // See http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set/#return-average-city-population-by-state
      val expected = List(
        document("_id" -> "NY", "avgCityPop" -> 19746227D),
        document("_id" -> "FR", "avgCityPop" -> 148169D),
        document("_id" -> "JP", "avgCityPop" -> 6592851D)
      )

      def withCtx[T](c: BSONCollection)(f: (c.BatchCommands.AggregationFramework.Group, List[c.PipelineOperator]) => T): T = {
        import c.BatchCommands.AggregationFramework
        import AggregationFramework.{ Group, SumField }

        val firstOp = Group(document("state" -> "$state", "city" -> "$city"))(
          "pop" -> SumField("population")
        )

        val pipeline = List(
          Group(BSONString("$_id.state"))("avgCityPop" ->
            AggregationFramework.Avg("pop"))
        )

        f(firstOp, pipeline)
      }

      "successfully as a single batch" in { implicit ee: EE =>
        withCtx(coll) { (firstOp, pipeline) =>
          coll.aggregate(firstOp, pipeline).map(_.firstBatch).
            aka("results") must beEqualTo(expected).await(1, timeout)
        }
      }

      "with cursor" >> {
        def collect(c: BSONCollection, upTo: Int = Int.MaxValue)(implicit ec: ExecutionContext) = withCtx(c) { (firstOp, pipeline) =>
          c.aggregate1[BSONDocument](firstOp, pipeline,
            c.BatchCommands.AggregationFramework.Cursor(1)).
            flatMap(_.collect[List](
              upTo, Cursor.FailOnError[List[BSONDocument]]()
            ))
        }

        "without limit (maxDocs)" in { implicit ee: EE =>
          collect(coll) aka "cursor result" must beEqualTo(expected).
            await(1, timeout)
        }

        "with limit (maxDocs)" in { implicit ee: EE =>
          collect(coll, 2) aka "cursor result" must beEqualTo(expected take 2).
            await(1, timeout)
        }

        "with metadata sort" in { implicit ee: EE =>
          import coll.BatchCommands.AggregationFramework
          import AggregationFramework.{
            Descending,
            Cursor => AggCursor,
            Match,
            MetadataSort,
            Sort,
            TextScore
          }

          val firstOp = Match(BSONDocument(
            "$text" -> BSONDocument("$search" -> "JP")
          ))

          val pipeline = List(Sort(
            MetadataSort("score", TextScore), Descending("city")
          ))

          coll.aggregate1[ZipCode](firstOp, pipeline, AggCursor(1)).
            flatMap(_.collect[List](
              4, Cursor.FailOnError[List[ZipCode]]()
            )) must beEqualTo(jpCodes).
            await(1, timeout)

        }
      }
    }

    "return largest and smallest cities by state" in { implicit ee: EE =>
      // See http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set/#return-largest-and-smallest-cities-by-state
      import coll.BatchCommands.AggregationFramework
      import AggregationFramework.{
        First,
        Group,
        Last,
        Project,
        Sort,
        Ascending,
        SumField
      }

      val expected = List(
        document(
          "biggestCity" -> document(
            "name" -> "NEW YORK", "population" -> 19746227L
          ),
          "smallestCity" -> document(
            "name" -> "NEW YORK", "population" -> 19746227L
          ),
          "state" -> "NY"
        ),
        document(
          "biggestCity" -> document(
            "name" -> "LE MANS", "population" -> 148169L
          ),
          "smallestCity" -> document(
            "name" -> "LE MANS", "population" -> 148169L
          ),
          "state" -> "FR"
        ), document(
          "biggestCity" -> document(
            "name" -> "TOKYO", "population" -> 13185502L
          ),
          "smallestCity" -> document(
            "name" -> "AOGASHIMA", "population" -> 200L
          ),
          "state" -> "JP"
        )
      )

      coll.aggregate(
        Group(document("state" -> "$state", "city" -> "$city"))(
          "pop" -> SumField("population")
        ),
        List(
          Sort(Ascending("population")),
          Group(BSONString("$_id.state"))(
            "biggestCity" -> Last("_id.city"),
            "biggestPop" -> Last("pop"),
            "smallestCity" -> First("_id.city"),
            "smallestPop" -> First("pop")
          ),
          Project(document("_id" -> 0, "state" -> "$_id",
            "biggestCity" -> document(
              "name" -> "$biggestCity", "population" -> "$biggestPop"
            ),
            "smallestCity" -> document(
              "name" -> "$smallestCity", "population" -> "$smallestPop"
            )))
        )
      ).
        map(_.firstBatch) aka "results" must beEqualTo(expected).
        await(1, timeout)
    }

    "return distinct states" >> {
      def distinctSpec(c: BSONCollection, timeout: FiniteDuration)(implicit ee: EE) = c.distinct[String, ListSet]("state").
        aka("states") must beEqualTo(ListSet("NY", "FR", "JP")).
        await(1, timeout)

      "with the default connection" in { implicit ee: EE =>
        distinctSpec(coll, timeout)
      }

      "with the slow connection" in { implicit ee: EE =>
        distinctSpec(slowZipColl, slowTimeout)
      }
    }

    "return a random sample" in { implicit ee: EE =>
      import coll.BatchCommands.AggregationFramework

      coll.aggregate(AggregationFramework.Sample(2)).map(_.head[ZipCode].
        filter(zipCodes.contains).size) must beEqualTo(2).await(0, timeout)
    } tag "not_mongo26"

    "perform simple lookup" >> {
      // See https://docs.mongodb.com/master/reference/operator/aggregation/lookup/#examples

      val orders = db.collection(s"agg-orders-1-${System identityHashCode this}")
      val inventory = db.collection(
        s"agg-inv-1-${System identityHashCode orders}"
      )

      "when fixtures are successfully inserted" in { implicit ee: EE =>
        (for {
          // orders
          _ <- orders.insert(BSONDocument(
            "_id" -> 1, "item" -> "abc", "price" -> 12, "quantity" -> 2
          ))
          _ <- orders.insert(BSONDocument(
            "_id" -> 2, "item" -> "jkl", "price" -> 20, "quantity" -> 1
          ))
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
            "sku" -> Option.empty[String], "description" -> "Incomplete"
          ))
          _ <- inventory.insert(BSONDocument("_id" -> 6))
        } yield ()) must beEqualTo({}).await(0, timeout)
      } tag "not_mongo26"

      implicit val productHandler = Macros.handler[Product]
      implicit val invReportHandler = Macros.handler[InventoryReport]

      "so the joined documents are returned" in { implicit ee: EE =>
        import orders.BatchCommands.AggregationFramework.Lookup

        def expected = List(
          InventoryReport(1, Some("abc"), Some(12), Some(2),
            List(Product(1, Some("abc"), Some("product 1"), Some(120)))),
          InventoryReport(2, Some("jkl"), Some(20), Some(1),
            List(Product(4, Some("jkl"), Some("product 4"), Some(70)))),
          InventoryReport(3, docs = List(
            Product(5, None, Some("Incomplete")), Product(6)
          ))
        )

        orders.aggregate(Lookup(inventory.name, "item", "sku", "docs")).
          map(_.head[InventoryReport].toList) must beEqualTo(expected).
          await(0, timeout)

      } tag "not_mongo26"
    }

    "filter results" >> {
      // See https://docs.mongodb.com/master/reference/operator/aggregation/filter/#example
      val sales = db.collection(s"agg-sales-A-${System identityHashCode this}")
      implicit val saleItemHandler = Macros.handler[SaleItem]
      implicit val saleHandler = Macros.handler[Sale]

      "when fixtures are successfully inserted" in { implicit ee: EE =>
        def fixtures = Seq(
          document("_id" -> 0, "items" -> array(
            document("itemId" -> 43, "quantity" -> 2, "price" -> 10),
            document("itemId" -> 2, "quantity" -> 1, "price" -> 240)
          )),
          document("_id" -> 1, "items" -> array(
            document("itemId" -> 23, "quantity" -> 3, "price" -> 110),
            document("itemId" -> 103, "quantity" -> 4, "price" -> 5),
            document("itemId" -> 38, "quantity" -> 1, "price" -> 300)
          )),
          document("_id" -> 2, "items" -> array(
            document("itemId" -> 4, "quantity" -> 1, "price" -> 23)
          ))
        )

        Future.sequence(fixtures.map { doc => sales.insert(doc) }).
          map(_ => {}) must beEqualTo({}).await(0, timeout)
      } tag "not_mongo26"

      "when using a '$project' stage" in { implicit ee: EE =>
        import sales.BatchCommands.AggregationFramework.{
          Ascending,
          Project,
          Filter,
          Sort
        }

        def expected = List(
          Sale(_id = 0, items = List(SaleItem(2, 1, 240))),
          Sale(_id = 1, items = List(
            SaleItem(23, 3, 110), SaleItem(38, 1, 300)
          )),
          Sale(_id = 2, items = Nil)
        )
        val sort = Sort(Ascending("_id"))

        sales.aggregate(Project(document("items" -> Filter(
          input = BSONString("$items"),
          as = "item",
          cond = document("$gte" -> array("$$item.price", 100))
        ))), List(sort)).map(_.head[Sale]) must beEqualTo(expected).
          await(0, timeout)
      } tag "not_mongo26"
    }
  }

  "perform lookup with array" >> {
    val orders = db.collection(s"agg-order-2-${System identityHashCode this}")
    val inventory = db.collection(
      s"agg-inv-2-${System identityHashCode orders}"
    )

    "when fixtures are successfully inserted" in { implicit ee: EE =>
      (for {
        // orders
        _ <- orders.insert(BSONDocument(
          "_id" -> 1, "item" -> "MON1003", "price" -> 350, "quantity" -> 2,
          "specs" -> BSONArray("27 inch", "Retina display", "1920x1080"),
          "type" -> "Monitor"
        ))

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

    "so the joined documents are returned" in { implicit ee: EE =>
      import orders.BatchCommands.AggregationFramework
      import AggregationFramework.{ Lookup, Match, Unwind }

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
          "resolution" -> "1920x1080"
        ))
      )

      orders.aggregate(Unwind("specs"), List(
        Lookup(inventory.name, "specs", "size", "docs"),
        Match(document("docs" -> document("$ne" -> BSONArray())))
      )).
        map(_.head[BSONDocument].toList) must beEqualTo(List(expected)).
        await(0, timeout)

    } tag "not_mongo26"
  }

  "Aggregation result for '$out'" >> {
    // https://docs.mongodb.com/master/reference/operator/aggregation/out/#example

    val books = db.collection(s"books-1-${System identityHashCode this}")

    "with valid fixtures" in { implicit ee: EE =>
      val fixtures = Seq(
        BSONDocument(
          "_id" -> 8751, "title" -> "The Banquet",
          "author" -> "Dante", "copies" -> 2
        ),
        BSONDocument(
          "_id" -> 8752, "title" -> "Divine Comedy",
          "author" -> "Dante", "copies" -> 1
        ),
        BSONDocument(
          "_id" -> 8645, "title" -> "Eclogues",
          "author" -> "Dante", "copies" -> 2
        ),
        BSONDocument(
          "_id" -> 7000, "title" -> "The Odyssey",
          "author" -> "Homer", "copies" -> 10
        ),
        BSONDocument(
          "_id" -> 7020, "title" -> "Iliad",
          "author" -> "Homer", "copies" -> 10
        )
      )

      Future.sequence(fixtures.map { doc => books.insert(doc) }).map(_ => {}).
        aka("fixtures") must beEqualTo({}).await(0, timeout)
    }

    "should be outputed to collection" in { implicit ee: EE =>
      import books.BatchCommands.AggregationFramework
      import AggregationFramework.{ Ascending, Group, Push, Out, Sort }

      val outColl = s"authors-1-${System identityHashCode this}"

      type Author = (String, List[String])
      implicit val authorReader = BSONDocumentReader[Author] { doc =>
        (for {
          id <- doc.getAsTry[String]("_id")
          books <- doc.getAsTry[List[String]]("books")
        } yield id -> books).get
      }

      books.aggregate(
        Sort(Ascending("title")),
        List(Group(BSONString("$author"))(
          "books" -> Push("title")
        ), Out(outColl))
      ).map(_ => {}).
        aka("$out aggregation") must beEqualTo({}).await(0, timeout) and {
          db.collection(outColl).find(BSONDocument.empty).cursor[Author]().
            collect[List](3, Cursor.FailOnError[List[Author]]()) must beEqualTo(
              List(
                "Homer" -> List("Iliad", "The Odyssey"),
                "Dante" -> List("Divine Comedy", "Eclogues", "The Banquet")
              )
            ).await(0, timeout)
        }
    }
  }

  "Aggregation result for '$stdDevPop'" >> {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevPop/#examples

    val contest = db.collection(s"contest-1-${System identityHashCode this}")

    "with valid fixtures" in { implicit ee: EE =>
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
          "name" -> "dave123", "quiz" -> 1, "score" -> 85
        ),
        BSONDocument(
          "_id" -> 2,
          "name" -> "dave2", "quiz" -> 1, "score" -> 90
        ),
        BSONDocument(
          "_id" -> 3,
          "name" -> "ahn", "quiz" -> 1, "score" -> 71
        ),
        BSONDocument(
          "_id" -> 4,
          "name" -> "li", "quiz" -> 2, "score" -> 96
        ),
        BSONDocument(
          "_id" -> 5,
          "name" -> "annT", "quiz" -> 2, "score" -> 77
        ),
        BSONDocument(
          "_id" -> 6,
          "name" -> "ty", "quiz" -> 2, "score" -> 82
        )
      )

      Future.sequence(fixtures.map { doc => contest.insert(doc) }).map(_ => {}).
        aka("fixtures") must beEqualTo({}).await(0, timeout)
    } tag "not_mongo26"

    "return the standard deviation of each quiz" in { implicit ee: EE =>
      import contest.BatchCommands.AggregationFramework.{
        Ascending,
        Group,
        Sort,
        StdDevPop
      }

      implicit val reader = Macros.reader[QuizStdDev]

      /*
       db.contest.aggregate([
         { $group: { _id: "$quiz", stdDev: { $stdDevPop: "$score" } } }
       ])
      */
      contest.aggregate(Group(BSONString("$quiz"))(
        "stdDev" -> StdDevPop(BSONString("$score"))
      ), List(Sort(Ascending("_id")))).map(_.head[QuizStdDev]).
        aka("$stdDevPop results") must beEqualTo(List(
          QuizStdDev(1, 8.04155872120988D), QuizStdDev(2, 8.04155872120988D)
        )).await(0, timeout)
      /*
       { "_id" : 1, "stdDev" : 8.04155872120988 }
       { "_id" : 2, "stdDev" : 8.04155872120988 }
       */
    } tag "not_mongo26"
  }

  "Aggregation result '$stdDevSamp'" >> {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/stdDevSamp/#example

    val contest = db.collection(s"contest-2-${System identityHashCode this}")

    "with valid fixtures" in { implicit ee: EE =>
      /*
       {_id: 0, username: "user0", age: 20}
       {_id: 1, username: "user1", age: 42}
       {_id: 2, username: "user2", age: 28}
       */
      val fixtures = Seq(
        BSONDocument("_id" -> 0, "username" -> "user0", "age" -> 20),
        BSONDocument("_id" -> 1, "username" -> "user1", "age" -> 42),
        BSONDocument("_id" -> 2, "username" -> "user2", "age" -> 28)
      )

      Future.sequence(fixtures.map { doc => contest.insert(doc) }).map(_ => {}).
        aka("fixtures") must beEqualTo({}).await(0, timeout)
    } tag "not_mongo26"

    "return the standard deviation of each quiz" in { implicit ee: EE =>
      import contest.BatchCommands.AggregationFramework.{
        Group,
        Sample,
        StdDevSamp
      }

      implicit val reader = Macros.reader[QuizStdDev]

      /*
       db.users.aggregate([
         { $sample: { size: 100 } },
         { $group: { _id: null, ageStdDev: { $stdDevSamp: "$age" } } }
       ])
      */
      contest.aggregate(Sample(100), List(Group(BSONNull)(
        "ageStdDev" -> StdDevSamp(BSONString("$age"))
      ))).map(_.firstBatch) must beEqualTo(List(
        BSONDocument("_id" -> BSONNull, "ageStdDev" -> 11.135528725660043D)
      )).await(0, timeout)
      /* { "_id" : null, "ageStdDev" : 11.135528725660043 } */
    } tag "not_mongo26"
  }

  "Geo-indexed documents" >> {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/geoNear/#example

    val places = db(s"places${System identityHashCode this}")

    {
      import ExecutionContext.Implicits.global
      import reactivemongo.api.indexes._, IndexType._

      // db.place.createIndex({'loc':"2dsphere"})
      scala.concurrent.Await.result(places.indexesManager.ensure(Index(
        List("loc" -> Geo2DSpherical)
      )).map(_ => {}), timeout * 2)
    }

    "must be inserted" in { implicit ee: EE =>
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
            "type" -> "Point", "coordinates" -> array(-73.97, 40.77)
          ),
          "name" -> "Central Park",
          "category" -> "Parks"
        ),
        document(
          "type" -> "public",
          "loc" -> document(
            "type" -> "Point", "coordinates" -> array(-73.88, 40.78)
          ),
          "name" -> "La Guardia Airport",
          "category" -> "Airport"
        )
      ).map { doc => places.insert(doc) }).map(_ => {}) must beEqualTo({}).
        await(0, timeout)
    }

    "and aggregated using $geoNear" in { implicit ee: EE =>
      import places.BatchCommands.AggregationFramework.GeoNear

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

      implicit val pointReader = Macros.reader[GeoPoint]
      implicit val distanceReader = BSONDocumentReader[GeoDistance] { doc =>
        (for {
          calc <- doc.getAsTry[BSONNumberLike]("calculated").map(_.toInt)
          loc <- doc.getAsTry[GeoPoint]("loc")
        } yield GeoDistance(calc, loc)).get
      }
      implicit val placeReader = Macros.reader[GeoPlace]

      places.aggregate(GeoNear(document(
        "type" -> "Point",
        "coordinates" -> array(-73.9667, 40.78)
      ), distanceField = Some("dist.calculated"),
        minDistance = Some(1000),
        maxDistance = Some(5000),
        query = Some(document("type" -> "public")),
        includeLocs = Some("dist.loc"),
        limit = 5,
        spherical = true)).map(_.head[GeoPlace]).
        aka("places") must beEqualTo(List(
          GeoPlace(
            loc = GeoPoint(List(-73.97D, 40.77D)),
            name = "Central Park",
            category = "Parks",
            dist = GeoDistance(
              calculated = 1147,
              loc = GeoPoint(List(-73.97D, 40.77D))
            )
          )
        )).await(0, timeout)

      // { "type" : "public", "loc" : { "type" : "Point", "coordinates" : [ -73.97, 40.77 ] }, "name" : "Central Park", "category" : "Parks", "dist" : { "calculated" : 1147.4220523120696, "loc" : { "type" : "Point", "coordinates" : [ -73.97, 40.77 ] } } }
    }
  }

  "Forecasts" should {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/redact/
    val forecasts = db(s"forecasts{System identityHashCode this}")

    "be inserted" in { implicit ee: EE =>
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
            "content" -> "Section 1: This is the content of section 1."
          ),
          BSONDocument(
            "subtitle" -> "Section 2: Analysis",
            "tags" -> BSONArray("STLW"),
            "content" -> "Section 2: This is the content of section 2."
          ),
          BSONDocument(
            "subtitle" -> "Section 3: Budgeting",
            "tags" -> BSONArray("TK"),
            "content" -> BSONDocument(
              "text" -> "Section 3: This is the content of section3.",
              "tags" -> BSONArray("HCS")
            )
          )
        )
      )).map(_ => {}) must beEqualTo({}).await(0, timeout)
    }

    "be redacted" in { implicit ee: EE =>
      import forecasts.BatchCommands.AggregationFramework.{ Match, Redact }

      implicit val subsectionReader = Macros.handler[Subsection]
      implicit val reader = Macros.handler[Redaction]

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

      val result = forecasts.aggregate(Match(document("year" -> 2014)), List(
        Redact(document("$cond" -> document(
          "if" -> document(
            "$gt" -> array(document(
              "$size" -> document("$setIntersection" -> array(
                "$tags", array("STLW", "G")
              ))
            ), 0)
          ),
          "then" -> "$$DESCEND",
          "else" -> "$$PRUNE"
        )))
      )).map(_.head[Redaction])

      val expected = Redaction(
        title = "123 Department Report",
        tags = List("G", "STLW"),
        year = 2014,
        subsections = List(
          Subsection(
            subtitle = "Section 1: Overview",
            tags = List("SI", "G"),
            content = "Section 1: This is the content of section 1."
          ),
          Subsection(
            subtitle = "Section 2: Analysis",
            tags = List("STLW"),
            content = "Section 2: This is the content of section 2."
          )
        )
      )
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

      result must beEqualTo(List(expected)).await(0, timeout)
    }
  }

  "Customer accounts" should {
    // https://docs.mongodb.com/manual/reference/operator/aggregation/redact/
    val customers = db(s"customers{System identityHashCode this}")

    "be inserted" in { implicit ee: EE =>
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
            "city" -> "Some City"
          ),
          "shipping_addr" -> array(
            document(
              "level" -> 3,
              "addr1" -> "987 XYZ Ave",
              "city" -> "Some City"
            ),
            document(
              "level" -> 3,
              "addr1" -> "PO Box 0123",
              "city" -> "Some City"
            )
          )
        ),
        "status" -> "A"
      )).map(_ => {}) must beEqualTo({}).await(0, timeout)
    }

    "be redacted" in { implicit ee: EE =>
      import customers.BatchCommands.AggregationFramework.{ Match, Redact }

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
      val result = customers.aggregate(Match(document("status" -> "A")), List(
        Redact(document(
          "$cond" -> document(
            "if" -> document("$eq" -> array("$level", 5)),
            "then" -> "$$PRUNE",
            "else" -> "$$DESCEND"
          )
        ))
      )).map(_.head[BSONDocument])

      result must beEqualTo(List(document(
        "_id" -> 1,
        "level" -> 1,
        "acct_id" -> "xyz123",
        "status" -> "A"
      ))).await(0, timeout)
    }
  }

  // ---

  case class Location(lon: Double, lat: Double)

  case class ZipCode(
    _id: String, city: String, state: String,
    population: Long, location: Location
  )

  case class Product(
    _id: Int, sku: Option[String] = None,
    description: Option[String] = None,
    instock: Option[Int] = None
  )

  case class InventoryReport(
    _id: Int,
    item: Option[String] = None,
    price: Option[Int] = None,
    quantity: Option[Int] = None,
    docs: List[Product] = Nil
  )

  case class SaleItem(itemId: Int, quantity: Int, price: Int)
  case class Sale(_id: Int, items: List[SaleItem])

  case class QuizStdDev(_id: Int, stdDev: Double)

  case class GeoPoint(coordinates: List[Double])
  case class GeoDistance(calculated: Int, loc: GeoPoint)
  case class GeoPlace(
    loc: GeoPoint,
    name: String,
    category: String,
    dist: GeoDistance
  )

  case class Subsection(
    subtitle: String,
    tags: List[String],
    content: String
  )
  case class Redaction(
    title: String,
    tags: List[String],
    year: Int,
    subsections: List[Subsection]
  )
}
