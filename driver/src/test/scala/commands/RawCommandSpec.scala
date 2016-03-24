import reactivemongo.bson.BSONDocument

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands.Command

import org.specs2.concurrent.{ ExecutionEnv => EE }

object RawCommandSpec extends org.specs2.mutable.Specification {
  "Raw command" title

  import Common._

  sequential

  lazy val collection = db("rawcommandspec")

  "Test collection" should {
    "be found" in { implicit ee: EE =>
      collection.create() must beEqualTo({}).await(1, timeout)
    }
  }

  "Raw command" should {
    def reIndexDoc = BSONDocument("reIndex" -> collection.name)

    "re-index test collection with command as document" in { implicit ee: EE =>
      val runner = Command.run(BSONSerializationPack)
      runner.apply(db, runner.rawCommand(reIndexDoc)).
        one[BSONDocument] must beLike[BSONDocument] {
          case doc => doc.getAs[Double]("ok") must beSome(1)
        }.await(1, timeout)
    }
  }
}
