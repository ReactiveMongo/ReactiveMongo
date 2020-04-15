import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.indexes.{ Index, IndexType }, IndexType.{
  Hashed,
  Geo2D,
  Geo2DSpherical
}
import reactivemongo.api.commands.CommandError

import reactivemongo.api.bson.BSONDocument

import reactivemongo.core.errors.DatabaseException

import reactivemongo.api.tests.{ pack, Pack }
import reactivemongo.api.TestCompat._

import org.specs2.concurrent.ExecutionEnv

final class IndexesSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification
  with org.specs2.specification.AfterAll {

  "Indexes management" title

  sequential

  // ---

  import tests.Common
  import Common.{ timeout, slowTimeout }

  lazy val (db, slowDb) = Common.databases(
    s"reactivemongo-gridfs-${System identityHashCode this}",
    Common.connection, Common.slowConnection)

  def afterAll = { db.drop(); () }

  lazy val geo = db(s"geo${System identityHashCode this}")
  lazy val slowGeo = slowDb(s"geo${System identityHashCode slowDb}")

  // ---

  "Geo Indexes" should {
    {
      def spec(c: DefaultCollection, timeout: FiniteDuration) = {
        c.insert(ordered = true).many((1 until 10).map { i =>
          BSONDocument("loc" -> BSONArray(i + 2D, i * 2D))
        }).map(_ => {}) must beTypedEqualTo({}).await(1, timeout)
      }

      "insert some points with the default connection" in {
        spec(geo, timeout)
      }

      "insert some points with the slow connection" in {
        spec(slowGeo, slowTimeout)
      }
    }

    {
      def spec(c: DefaultCollection, timeout: FiniteDuration) =
        c.indexesManager.ensure(index(
          List("loc" -> Geo2D),
          options = BSONDocument("min" -> -95, "max" -> 95, "bits" -> 28))).
          aka("index") must beTrue.await(1, timeout * 2)

      "be created with the default connection" in {
        spec(geo, timeout)
      }

      "be created with the slow connection" in {
        spec(slowGeo, slowTimeout)
      }
    }

    "fail to insert some points out of range" in {
      geo.insert.one(
        BSONDocument("loc" -> BSONArray(27.88D, 97.21D))).
        map(_ => false).recover {
          case e: DatabaseException =>
            // MongoError['point not in interval of [ -95, 95 )' (code = 13027)]
            e.code.exists(_ == 13027)

          case _ => false
        } must beTrue.await(1, timeout)
    }

    {
      def spec(c: DefaultCollection, timeout: FiniteDuration) = {
        def future = c.indexesManager.list().map {
          _.filter(_.name.get == "loc_2d")
        }.filter(!_.isEmpty).map(_.apply(0))

        future must beLike[Index] {
          case idx @ Index.Key(("loc", Geo2D)) =>
            idx.min must beSome(-95) and {
              idx.max must beSome(95)
            } and {
              idx.bits must beSome(28)
            }

        }.await(1, timeout)
      }

      "retrieve indexes with the default connection" in {
        spec(geo, timeout)
      }

      "retrieve indexes with the slow connection" in {
        spec(slowGeo, slowTimeout)
      }
    }
  }

  lazy val geo2DSpherical = db(s"geo2d_${System identityHashCode this}")

  "Geo2D indexes" should {
    "insert some points" in {
      val batch = for (i <- 1 until 10) yield {
        BSONDocument("loc" -> BSONDocument(
          "type" -> "Point",
          "coordinates" -> BSONArray(i + 2D, i * 2D)))
      }

      geo2DSpherical.insert(ordered = false).many(batch).map(_.n).
        aka("inserted") must beTypedEqualTo(9).await(1, timeout)
    }

    "make index" in {
      geo2DSpherical.indexesManager.ensure(
        index(List("loc" -> Geo2DSpherical))) must beTrue.await(1, timeout * 2)
    }

    "retrieve indexes" in {
      geo2DSpherical.indexesManager.list().map {
        _.filter(_.name.get == "loc_2dsphere")
      }.filter(!_.isEmpty).map(_.apply(0)).map(_.key(0)).
        aka("index") must beEqualTo("loc" -> Geo2DSpherical).await(1, timeout)
    }
  }

  lazy val hashed = db(s"hashed_${System identityHashCode this}")

  "Hashed indexes" should {
    "insert some data" in {
      // With WiredTiger, collection must exist before
      hashed.insert.many((1 until 10).map { i =>
        BSONDocument("field" -> s"data-$i")
      }).map(_ => {}) must beTypedEqualTo({}).await(1, timeout)
    }

    "make index" in {
      hashed.indexesManager.ensure(index(List("field" -> Hashed))).
        aka("index") must beTrue.await(1, timeout)
    }

    "retrieve indexes" in {
      val index = hashed.indexesManager.list().map {
        _.filter(_.name.get == "field_hashed")
      }.filter(!_.isEmpty).map(_.apply(0))

      index.map(_.key(0)) must beEqualTo("field" -> Hashed).await(1, timeout)
    }
  }

  "Index manager" should {
    "drop all indexes in db.geo" in {
      geo.indexesManager.dropAll() must beEqualTo(2 /* _id and loc */ ).
        await(1, timeout)
    }
  }

  "Index" should {
    import reactivemongo.api.indexes._
    lazy val col = db(s"indexed_col_${hashCode}")

    "be first created" in {
      col.create().flatMap { _ =>
        col.indexesManager.ensure(index(
          Seq("token" -> IndexType.Ascending), unique = true))
      } aka "index creation" must beTrue.await(1, timeout * 2)
    }

    "not be created if already exists" in {
      col.indexesManager.ensure(index(
        Seq("token" -> IndexType.Ascending), unique = true)).
        aka("index creation") must beFalse.await(1, timeout * 2)

    }
  }

  "Listing indexes" should {
    "return empty list if collection doesn't exist" in {
      lazy val col = db(s"nonexistent-collection-$hashCode")

      col.indexesManager.list() must beEmpty[List[Index]].await(0, timeout)
    }
  }

  lazy val partial = db(s"partial${System identityHashCode this}")

  "Index with partial filter" should {
    // See https://docs.mongodb.com/manual/core/index-partial/#partial-index-with-unique-constraints

    val fixtures = List(
      BSONDocument("username" -> "david", "age" -> 29),
      BSONDocument("username" -> "amanda", "age" -> 29),
      BSONDocument("username" -> "rajiv", "age" -> 57))

    @inline def fixturesInsert =
      fixtures.map { partial.insert.one(_) }

    "have fixtures" in {
      Future.sequence(fixturesInsert).
        map(_ => {}) must beEqualTo({}).await(0, timeout)

    }

    "fail with already inserted documents" in {
      val idx = index(
        key = Seq("age" -> IndexType.Ascending),
        unique = true)

      val mngr = partial.indexesManager
      def spec[T](test: => Future[T]) =
        test must throwA[DatabaseException].like {
          case CommandError.Code(11000) => ok
        }.awaitFor(timeout)

      spec(mngr.ensure(idx)) and spec(mngr.create(idx))
    }

    "be created" in {
      partial.indexesManager.create(index(
        key = Seq("username" -> IndexType.Ascending),
        unique = true,
        partialFilter = Some(BSONDocument(
          "age" -> BSONDocument(f"$$gte" -> 21))))).
        map(_ => {}) must beTypedEqualTo({}).awaitFor(timeout)
    }

    "not have duplicate fixtures" in {
      @com.github.ghik.silencer.silent("fold")
      def spec = Future.fold(fixturesInsert)(false) { (inserted, _) =>
        inserted
      }.recover {
        case err: DatabaseException => !err.code.exists(_ == 11000)
        case _                      => true
      }

      spec aka "inserted" must beFalse.await(0, timeout)
    }

    "allow duplicate if the filter doesn't match" in {
      def insertAndCount = for {
        a <- partial.count()
        _ <- partial.insert(ordered = true).many(Seq(
          BSONDocument("username" -> "david", "age" -> 20),
          BSONDocument("username" -> "amanda"),
          BSONDocument(
            "username" -> "rajiv", "age" -> Option.empty[Int])))

        b <- partial.count()
      } yield a -> b

      insertAndCount must beTypedEqualTo(3L -> 6L).await(0, timeout)
    }
  }

  "Text index" should {
    lazy val coll = db(s"txtidx${System identityHashCode this}")
    lazy val mngr = coll.indexesManager

    val name = "mySearchIndex"
    val textIndex = index(
      Seq(
        "someFieldA" -> IndexType.Text,
        "someFieldB" -> IndexType.Text),
      name = Some(name))

    "be created" in {
      mngr.create(textIndex).flatMap(_ => mngr.list().map(_.flatMap(_.name))).
        aka("indexes") must contain(atLeast(name)).await(0, timeout)

    }

    "be ensured" in {
      mngr.ensure(textIndex) must beFalse.await(0, timeout)
    }
  }

  // ---

  def index(
    key: Seq[(String, IndexType)],
    name: Option[String] = None,
    unique: Boolean = false,
    background: Boolean = false,
    sparse: Boolean = false,
    version: Option[Int] = None, // let MongoDB decide
    partialFilter: Option[BSONDocument] = None,
    options: BSONDocument = BSONDocument.empty) = Index[Pack](pack)(key, name, unique, background, sparse, None, None, None, None, None, None, None, None, None, None, None, None, None, version, partialFilter, options)

}
