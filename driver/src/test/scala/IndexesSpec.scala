import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import reactivemongo.bson.{ BSONArray, BSONDocument, BSONInteger }
import reactivemongo.api.indexes.{ Index, IndexType }, IndexType.{
  Hashed,
  Geo2D,
  Geo2DSpherical
}
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.core.errors.DatabaseException

import org.specs2.concurrent.{ ExecutionEnv => EE }

class IndexesSpec extends org.specs2.mutable.Specification {
  "Indexes management" title

  sequential

  import Common._

  lazy val geo = db(s"geo${System identityHashCode this}")
  lazy val slowGeo = slowDb(s"geo${System identityHashCode slowDb}")

  "ReactiveMongo Geo Indexes" should {
    {
      def spec(c: BSONCollection, timeout: FiniteDuration)(implicit ee: EE) = {
        val futs: Seq[Future[Unit]] = for (i <- 1 until 10)
          yield c.insert(BSONDocument(
          "loc" -> BSONArray(i + 2D, i * 2D))).map(_ => {})

        Future.sequence(futs) must not(throwA[Throwable]).await(1, timeout)
      }

      "insert some points with the default connection" in { implicit ee: EE =>
        spec(geo, timeout)
      }

      "insert some points with the default connection" in { implicit ee: EE =>
        spec(slowGeo, slowTimeout)
      }
    }

    {
      def spec(c: BSONCollection, timeout: FiniteDuration)(implicit ee: EE) =
        c.indexesManager.ensure(Index(
          List("loc" -> Geo2D),
          options = BSONDocument("min" -> -95, "max" -> 95, "bits" -> 28))).
          aka("index") must beTrue.await(1, timeout * 2)

      "be created with the default connection" in { implicit ee: EE =>
        spec(geo, timeout)
      }

      "be created with the slow connection" in { implicit ee: EE =>
        spec(slowGeo, slowTimeout)
      }
    }

    "fail to insert some points out of range" in { implicit ee: EE =>
      geo.insert(
        BSONDocument("loc" -> BSONArray(27.88D, 97.21D))).
        map(_ => false).recover {
          case e: DatabaseException =>
            // MongoError['point not in interval of [ -95, 95 )' (code = 13027)]
            e.code.exists(_ == 13027)

          case _ => false
        } must beTrue.await(1, timeout)
    }

    {
      def spec(c: BSONCollection, timeout: FiniteDuration)(implicit ee: EE) = {
        def future = c.indexesManager.list().map {
          _.filter(_.name.get == "loc_2d")
        }.filter(!_.isEmpty).map(_.apply(0))

        future must beLike[Index] {
          case i @ Index(("loc", Geo2D) :: _, _, _, _, _, _, _, _, opts) =>
            opts.getAs[BSONInteger]("min").get.value mustEqual -95 and (
              opts.getAs[BSONInteger]("max").get.value mustEqual 95) and (
                opts.getAs[BSONInteger]("bits").get.value mustEqual 28)
        }.await(1, timeout)
      }

      "retrieve indexes with the default connection" in { implicit ee: EE =>
        spec(geo, timeout)
      }

      "retrieve indexes with the default connection" in { implicit ee: EE =>
        spec(slowGeo, slowTimeout)
      }
    }
  }

  lazy val geo2DSpherical = db("geo2d")

  "ReactiveMongo Geo2D indexes" should {
    "insert some points" in { implicit ee: EE =>
      val futs: Seq[Future[Unit]] = for (i <- 1 until 10)
        yield geo2DSpherical.insert(
        BSONDocument("loc" -> BSONDocument(
          "type" -> "Point",
          "coordinates" -> BSONArray(i + 2D, i * 2D)))).map(_ => {})

      Future.sequence(futs) must not(throwA[Throwable]).await(1, timeout)
    }

    "make index" in { implicit ee: EE =>
      geo2DSpherical.indexesManager.ensure(
        Index(List("loc" -> Geo2DSpherical))) must beTrue.await(1, timeout * 2)
    }

    "retrieve indexes" in { implicit ee: EE =>
      geo2DSpherical.indexesManager.list().map {
        _.filter(_.name.get == "loc_2dsphere")
      }.filter(!_.isEmpty).map(_.apply(0)).map(_.key(0)).
        aka("index") must beEqualTo("loc" -> Geo2DSpherical).await(1, timeout)
    }
  }

  lazy val hashed = db("hashed")

  "Hashed indexes" should {
    "insert some data" in { implicit ee: EE =>
      // With WiredTiger, collection must exist before
      val futs: Seq[Future[Unit]] = for (i <- 1 until 10)
        yield hashed.insert(BSONDocument("field" -> s"data-$i")).map(_ => {})

      Future.sequence(futs) must not(throwA[Throwable]).await(1, timeout)
    }

    "make index" in { implicit ee: EE =>
      hashed.indexesManager.ensure(Index(List("field" -> Hashed))).
        aka("index") must beTrue.await(1, timeout)
    }

    "retrieve indexes" in { implicit ee: EE =>
      val index = hashed.indexesManager.list().map {
        _.filter(_.name.get == "field_hashed")
      }.filter(!_.isEmpty).map(_.apply(0))

      index.map(_.key(0)) must beEqualTo("field" -> Hashed).await(1, timeout)
    }
  }

  "Index manager" should {
    "drop all indexes in db.geo" in { implicit ee: EE =>
      geo.indexesManager.dropAll() must beEqualTo(2 /* _id and loc */ ).
        await(1, timeout)
    }
  }

  "Index" should {
    import reactivemongo.api.indexes._
    lazy val col = db(s"indexed_col_${hashCode}")

    "be first created" in { implicit ee: EE =>
      col.indexesManager.ensure(Index(
        Seq("token" -> IndexType.Ascending), unique = true)).
        aka("index creation") must beTrue.await(1, timeout * 2)
    }

    "not be created if already exists" in { implicit ee: EE =>
      col.indexesManager.ensure(Index(
        Seq("token" -> IndexType.Ascending), unique = true)).
        aka("index creation") must beFalse.await(1, timeout * 2)

    }
  }

  lazy val partial = db(s"partial${System identityHashCode this}")

  "Index with partial filter" should {
    // See https://docs.mongodb.com/manual/core/index-partial/#partial-index-with-unique-constraints

    val fixtures = List(
      BSONDocument("username" -> "david", "age" -> 29),
      BSONDocument("username" -> "amanda", "age" -> 35),
      BSONDocument("username" -> "rajiv", "age" -> 57))

    @inline def fixturesInsert(implicit ee: EE) =
      fixtures.map { partial.insert(_) }

    "have fixtures" in { implicit ee: EE =>
      Future.sequence(fixturesInsert).
        map(_ => {}) must beEqualTo({}).await(0, timeout)

    } tag "not_mongo26"

    "be created" in { implicit ee: EE =>
      partial.indexesManager.create(Index(
        key = Seq("username" -> IndexType.Ascending),
        unique = true,
        partialFilter = Some(BSONDocument(
          "age" -> BSONDocument("$gte" -> 21))))).
        map(_.ok) must beTrue.await(0, timeout)
    } tag "not_mongo26"

    "not have duplicate fixtures" in { implicit ee: EE =>
      Future.fold(fixturesInsert)(false) { (inserted, res) =>
        if (res.ok) true else inserted
      }.recover {
        case err: DatabaseException => !err.code.exists(_ == 11000)
        case _                      => true
      } aka "inserted" must beFalse.await(0, timeout)
    } tag "not_mongo26"

    "allow duplicate if the filter doesn't match" in { implicit ee: EE =>
      def insertAndCount = for {
        a <- partial.count()
        _ <- partial.insert(BSONDocument("username" -> "david", "age" -> 20))
        _ <- partial.insert(BSONDocument("username" -> "amanda"))
        _ <- partial.insert(BSONDocument(
          "username" -> "rajiv", "age" -> Option.empty[Int]))

        b <- partial.count()
      } yield a -> b

      insertAndCount must beEqualTo(3 -> 6).await(0, timeout)
    } tag "not_mongo26"
  }
}
