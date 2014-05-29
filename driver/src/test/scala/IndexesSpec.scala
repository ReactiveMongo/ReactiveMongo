import org.specs2.mutable._
import reactivemongo.api.indexes._
import reactivemongo.api.indexes.IndexType.{Hashed, Geo2D, Geo2DSpherical}
import reactivemongo.bson._
import reactivemongo.core.errors.DatabaseException
import scala.concurrent.Future
import scala.concurrent.Await

class IndexesSpec extends Specification with Tags {
  sequential

  import Common._

  val geo = db("geo")

  "ReactiveMongo Geo Indexes" should {
    "insert some points" in {
      val futs = for(i <- 1 until 10)
      yield geo.insert(BSONDocument("loc" -> BSONArray( BSONDouble(i + 2), BSONDouble(i * 2) )))
      val fut = Future.sequence(futs)
      Await.result(fut, timeout)
      success
    }
    "make index" in {
      val created = geo.indexesManager.ensure(
        Index(
          List("loc" -> Geo2D),
          options = BSONDocument(
            "min" -> BSONInteger(-95),
            "max" -> BSONInteger(95),
            "bits" -> BSONInteger(28)
          )
        )
      )
      Await.result(created, timeout) mustEqual true
    }
    "fail to insert some points out of range" in {
      val future = geo.insert(BSONDocument("loc" -> BSONArray( BSONDouble(27.88), BSONDouble(97.21) )))
      try {
        Await.result(future, timeout)
        failure
      } catch {
        case e: DatabaseException =>
          e.code mustEqual Some(13027) // MongoError['point not in interval of [ -95, 95 )' (code = 13027)]
      }
      success
    }
    "retrieve indexes" in {
      val future = geo.indexesManager.list().map {
        _.filter(_.name.get == "loc_2d")
      }.filter(!_.isEmpty).map(_.apply(0))
      val index = Await.result(future, timeout)
      index.key(0)._1 mustEqual "loc"
      index.key(0)._2 mustEqual Geo2D
      index.options.getAs[BSONInteger]("min").get.value mustEqual -95
      index.options.getAs[BSONInteger]("max").get.value mustEqual 95
      index.options.getAs[BSONInteger]("bits").get.value mustEqual 28
    }
  }

  val geo2DSpherical = db("geo2d")

  "ReactiveMongo Geo2D indexes" should {

    "insert some points" in {
      val futs = for(i <- 1 until 10)
      yield geo2DSpherical.insert(BSONDocument("loc" -> BSONDocument(
          "type" -> BSONString("Point"),
          "coordinates" -> BSONArray( BSONDouble(i + 2), BSONDouble(i * 2) )
        )))
      val fut = Future.sequence(futs)
      Await.result(fut, timeout)
      success
    }

    "make index" in {
      val created = geo2DSpherical.indexesManager.ensure(
        Index(
          List("loc" -> Geo2DSpherical)
        )
      )
      Await.result(created, timeout) mustEqual true
    }

    "fail to insert a point out of range" in {
      val future = geo2DSpherical.insert(BSONDocument("loc" -> BSONDocument(
        "type" -> BSONString("Point"),
        "coordinates" -> BSONArray( BSONDouble(-195), BSONDouble(25) )
      )))
      try {
        val result = Await.result(future, timeout)
        println(s"\n\n \tPOOR: $result \n\n")
        failure
      } catch {
        case e: DatabaseException =>
          e.code mustEqual Some(16572) // MongoError['Can't extract geo keys from object, malformed geometry?' (code = 16572)]
        case e =>
          e.printStackTrace()
          throw e
      }
      success
    } tag ("mongo2_4")

    "retrieve indexes" in {
      val future = geo2DSpherical.indexesManager.list().map {
        _.filter(_.name.get == "loc_2dsphere")
      }.filter(!_.isEmpty).map(_.apply(0))
      val index = Await.result(future, timeout)
      index.key(0)._1 mustEqual "loc"
      index.key(0)._2 mustEqual Geo2DSpherical
    }
  }

  val hashed = db("hashed")

  "ReactiveMongo Hashed indexes" should {
    "make index" in {
      val created = hashed.indexesManager.ensure(
        Index(
          List("field" -> Hashed)
        )
      )
      Await.result(created, timeout) mustEqual true
    }

    "retrieve indexes" in {
      val future = hashed.indexesManager.list().map {
        _.filter(_.name.get == "field_hashed")
      }.filter(!_.isEmpty).map(_.apply(0))
      val index = Await.result(future, timeout)
      index.key(0)._1 mustEqual "field"
      index.key(0)._2 mustEqual Hashed
    }
  }
}
