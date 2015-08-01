import scala.concurrent.Await

import reactivemongo.api.commands._

object ReplSetGetStatusSpec extends org.specs2.mutable.Specification {
  "replSetGetStatus" title

  import Common._

  val adminDb = connection("admin")

  "BSON command" should {
    "be successful" in {
      import bson.BSONReplSetGetStatusImplicits._

      // TODO: Setup a successful replica set
      adminDb.runCommand(ReplSetGetStatus) must throwA[CommandError].
        await(timeoutMillis)
    }
  }
}
