import org.specs2.mutable._
import reactivemongo.api.indexes._
import reactivemongo.api.indexes.IndexType.Geo2D
import reactivemongo.bson._
import DefaultBSONHandlers._
import reactivemongo.core.errors.DBError
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._

class IndexesSpec  extends Specification {
  sequential

  import Common._

  val geo = db("geo")

  "ReactiveMongo Geo Indexes" should {
    "insert some points" in {
      val futs = for(i <- 1 until 10)
      yield geo.insert(BSONDocument("loc" -> BSONArray( BSONDouble(i + 2), BSONDouble(i * 2) )))
      val fut = Future.sequence(futs)
      Await.result(fut, DurationInt(5).seconds)
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
      Await.result(created, DurationInt(5).seconds) mustEqual true
    }
    "fail to insert some points out of range" in {
      val future = geo.insert(BSONDocument("loc" -> BSONArray( BSONDouble(27.88), BSONDouble(97.21) )))
      try {
        Await.result(future, DurationInt(5).seconds)
        failure
      } catch {
        case e: DBError =>
          e.code mustEqual Some(13027) // MongoError['point not in interval of [ -95, 95 )' (code = 13027)]
      }
      success
    }
    "retrieve indexes" in {
      val future = geo.indexesManager.list().map {
        _.filter(_.name.get == "loc_2d")
      }.filter(!_.isEmpty).map(_.apply(0))
      val index = Await.result(future, DurationInt(5).seconds)
      index.key(0)._1 mustEqual "loc"
      index.key(0)._2 mustEqual Geo2D
      index.options.getAs[BSONInteger]("min").get.value mustEqual -95
      index.options.getAs[BSONInteger]("max").get.value mustEqual 95
      index.options.getAs[BSONInteger]("bits").get.value mustEqual 28
    }
  }
}