import scala.collection.immutable.ListSet

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.bson._

import org.specs2.concurrent.{ ExecutionEnv => EE }

object AggregationSpec extends org.specs2.mutable.Specification {
  "Aggregation framework" title

  import Common._

  sequential

  val coll = db("zipcodes")
  import coll.BatchCommands.AggregationFramework
  import AggregationFramework.{
    Cursor,
    First,
    Group,
    Last,
    Match,
    Project,
    Sort,
    Ascending,
    Sample,
    SumField
  }

  case class Location(lon: Double, lat: Double)

  case class ZipCode(_id: String, city: String, state: String,
                     population: Long, location: Location)

  implicit val locationHandler = Macros.handler[Location]
  implicit val zipCodeHandler = Macros.handler[ZipCode]

  private val zipCodes = List(
    ZipCode("10280", "NEW YORK", "NY", 19746227L,
      Location(-74.016323, 40.710537)),
    ZipCode("72000", "LE MANS", "FR", 148169L, Location(48.0077, 0.1984)),
    ZipCode("JP-13", "TOKYO", "JP", 13185502L,
      Location(35.683333, 139.683333)),
    ZipCode("AO", "AOGASHIMA", "JP", 200L, Location(32.457, 139.767)))

  "Zip codes" should {
    "be inserted" in { implicit ee: EE =>
      def insert(data: List[ZipCode]): Future[Unit] = data.headOption match {
        case Some(zip) => coll.insert(zip).flatMap(_ => insert(data.tail))
        case _         => Future.successful({})
      }

      insert(zipCodes) must beEqualTo({}).await(1, timeout)
    }

    "return states with populations above 10000000" in { implicit ee: EE =>
      // http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set/#return-states-with-populations-above-10-million
      val expected = List(document("_id" -> "JP", "totalPop" -> 13185702L),
        document("_id" -> "NY", "totalPop" -> 19746227L))

      coll.aggregate(Group(BSONString("$state"))(
        "totalPop" -> SumField("population")), List(
        Match(document("totalPop" ->
          document("$gte" -> 10000000L))))).map(_.firstBatch).
        aka("results") must beEqualTo(expected).await(1, timeout)
    }

    "return average city population by state" >> {
      // See http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set/#return-average-city-population-by-state
      val expected = List(document("_id" -> "NY", "avgCityPop" -> 19746227D),
        document("_id" -> "FR", "avgCityPop" -> 148169D),
        document("_id" -> "JP", "avgCityPop" -> 6592851D))

      val firstOp = Group(document("state" -> "$state", "city" -> "$city"))(
        "pop" -> SumField("population"))

      val pipeline = List(
        Group(BSONString("$_id.state"))("avgCityPop" ->
          AggregationFramework.Avg("pop")))

      "successfully as a single batch" in { implicit ee: EE =>
        coll.aggregate(firstOp, pipeline).map(_.firstBatch).
          aka("results") must beEqualTo(expected).await(1, timeout)
      }

      "with cursor" >> {
        def collect(upTo: Int = Int.MaxValue)(implicit ec: ExecutionContext) =
          coll.aggregate1[BSONDocument](firstOp, pipeline, Cursor(1)).
            flatMap(_.collect[List](upTo))

        "without limit (maxDocs)" in { implicit ee: EE =>
          collect() aka "cursor result" must beEqualTo(expected).
            await(1, timeout)
        }

        "with limit (maxDocs)" in { implicit ee: EE =>
          collect(2) aka "cursor result" must beEqualTo(expected take 2).
            await(1, timeout)
        }
      }
    }

    "return largest and smallest cities by state" in { implicit ee: EE =>
      // See http://docs.mongodb.org/manual/tutorial/aggregation-zip-code-data-set/#return-largest-and-smallest-cities-by-state
      val expected = List(document(
        "biggestCity" -> document(
          "name" -> "NEW YORK", "population" -> 19746227L),
        "smallestCity" -> document(
          "name" -> "NEW YORK", "population" -> 19746227L),
        "state" -> "NY"),
        document(
          "biggestCity" -> document(
            "name" -> "LE MANS", "population" -> 148169L),
          "smallestCity" -> document(
            "name" -> "LE MANS", "population" -> 148169L),
          "state" -> "FR"), document(
          "biggestCity" -> document(
            "name" -> "TOKYO", "population" -> 13185502L),
          "smallestCity" -> document(
            "name" -> "AOGASHIMA", "population" -> 200L),
          "state" -> "JP"))

      coll.aggregate(
        Group(document("state" -> "$state", "city" -> "$city"))(
          "pop" -> SumField("population")),
        List(Sort(Ascending("population")),
          Group(BSONString("$_id.state"))(
            "biggestCity" -> Last("_id.city"),
            "biggestPop" -> Last("pop"),
            "smallestCity" -> First("_id.city"),
            "smallestPop" -> First("pop")),
          Project(document("_id" -> 0, "state" -> "$_id",
            "biggestCity" -> document(
              "name" -> "$biggestCity", "population" -> "$biggestPop"),
            "smallestCity" -> document(
              "name" -> "$smallestCity", "population" -> "$smallestPop"))))).
        map(_.firstBatch) aka "results" must beEqualTo(expected).
        await(1, timeout)
    }

    "return distinct states" in { implicit ee: EE =>
      coll.distinct[String, ListSet]("state").
        aka("states") must beEqualTo(ListSet("NY", "FR", "JP")).
        await(1, timeout)
    }

    "return a random sample" in { implicit ee: EE =>
      coll.aggregate(Sample(2)).map(_.head[ZipCode].
        filter(zipCodes.contains).size) must beEqualTo(2).await(1, timeout)
    } tag "not_mongo26"
  }
}
