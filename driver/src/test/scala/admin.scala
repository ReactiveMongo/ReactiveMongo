import reactivemongo.api.commands._

import org.specs2.mutable.Specification

import Common._

object RenameCollectionSpec extends Specification {
  "renameCollection" title

  val adminDb = connection("admin")

  "Collection" should {
    val col = adminDb(s"foo_${System identityHashCode adminDb}")

    "be renamed" in {
      col.create().map(_ => col.rename("renamed")).map(_ => {}).
        aka("renaming") must beEqualTo({}).await(timeoutMillis)
    }
  }
}

object ReplSetGetStatusSpec extends Specification {
  "replSetGetStatus" title

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

object ServerStatusSpec extends Specification {
  "serverStatus" title

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

object ResyncSpec extends Specification {
  "Resync" title

  val db = connection("admin")

  "BSON command" should {
    import bson.BSONResyncImplicits._

    "be successful" in {
      db.runCommand(Resync) must not(throwA[CommandError]).await(timeoutMillis)
    }
  }
}
