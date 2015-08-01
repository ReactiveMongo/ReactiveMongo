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

object ServerStatusSpec extends org.specs2.mutable.Specification {
  "serverStatus" title

  import Common._

  "BSON command" should {
    "be successful" in {
      import bson.BSONServerStatusImplicits._

      db.runCommand(ServerStatus) must beLike[ServerStatusResult]({
        case status @ ServerStatusResult(_, _, MongodProcess, _, _, _, _, _) =>
          println(s"Server status: $status"); ok
      }).await(timeoutMillis)
    }
  }

  "Database operation" should {
    "be successful" in {
      db.serverStatus must beLike[ServerStatusResult]({
        case status @ ServerStatusResult(_, _, MongodProcess, _, _, _, _, _) =>
          println(s"Server status: $status"); ok
      }).await(timeoutMillis)
    }
  }
}
