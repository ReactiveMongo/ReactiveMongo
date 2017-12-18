import scala.concurrent.duration.FiniteDuration

import reactivemongo.bson.BSONDocument

import reactivemongo.api.{ BSONSerializationPack, ReadPreference }
import reactivemongo.api.commands.Command

import reactivemongo.api.collections.bson.BSONCollection

import org.specs2.concurrent.ExecutionEnv

class RawCommandSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification {

  "Raw command" title

  import Common._

  sequential

  val gen = new scala.util.Random(System identityHashCode this)
  def colName = {
    val uid = gen.nextLong()
    s"rawcommandspec$uid"
  }

  lazy val collection = db(colName)

  "Collection" should {
    "be found with the default connection" in {
      collection.create() must beEqualTo({}).await(1, timeout)
    }

    "be found with the slow connection" in {
      slowDb(colName).create() must beEqualTo({}).await(1, slowTimeout)
    }
  }

  "Raw command" should {
    "re-index test collection with command as document" >> {
      def reindexSpec(c: BSONCollection, timeout: FiniteDuration) = {
        val runner = Command.run(BSONSerializationPack, db.failoverStrategy)
        val reIndexDoc = BSONDocument("reIndex" -> collection.name)

        runner.apply(db, runner.rawCommand(reIndexDoc)).
          one[BSONDocument](ReadPreference.primary) must beLike[BSONDocument] {
            case doc => doc.getAs[Double]("ok") must beSome(1)
          }.await(1, timeout)
      }

      "with the default connection" in {
        reindexSpec(collection, timeout)
      }

      "with the slow connection" in {
        reindexSpec(slowDb(collection.name), slowTimeout)
      }
    }
  }
}
