import reactivemongo.api.commands._

import org.specs2.mutable.Specification

import Common._

import org.specs2.concurrent.{ ExecutionEnv => EE }

object RenameCollectionSpec extends Specification {
  "renameCollection" title

  "Collection" should {

    "be renamed" in { implicit ee: EE =>
      val adminDb = connection("admin")
      val col = adminDb(s"foo_${System identityHashCode adminDb}")

      col.create().map(_ => col.rename("renamed")).map(_ => {}).
        aka("renaming") must beEqualTo({}).await(1, timeout)
    }
  }
}

object ReplSetGetStatusSpec extends Specification {
  "replSetGetStatus" title

  "BSON command" should {
    "be successful" in { implicit ee: EE =>
      import bson.BSONReplSetGetStatusImplicits._

      // TODO: Setup a successful replica set
      connection("admin").runCommand(
        ReplSetGetStatus) must throwA[CommandError].await(1, timeout)
    }
  }
}

object ServerStatusSpec extends Specification {
  "serverStatus" title

  "BSON command" should {
    "be successful" in { implicit ee: EE =>
      import bson.BSONServerStatusImplicits._

      db.runCommand(ServerStatus) must beLike[ServerStatusResult]({
        case status @ ServerStatusResult(_, _, MongodProcess, _, _, _, _, _) =>
          //println(s"Server status: $status")
          ok
      }).await(1, timeout)
    }
  }

  "Database operation" should {
    "be successful" in { implicit ee: EE =>
      db.serverStatus must beLike[ServerStatusResult]({
        case status @ ServerStatusResult(_, _, MongodProcess, _, _, _, _, _) =>
          println(s"Server status: $status"); ok
      }).await(1, timeout)
    }
  }
}

object ResyncSpec extends Specification {
  "Resync" title

  "BSON command" should {
    import bson.BSONResyncImplicits._

    "be successful" in { implicit ee: EE =>
      connection("admin").runCommand(Resync) must not(
        throwA[CommandError]).await(1, timeout)
    }
  }
}

object ReplSetMaintenanceSpec extends Specification {
  "ReplSetMaintenance" title

  "BSON command" should {
    import bson.BSONReplSetMaintenanceImplicits._

    "fail outside replicaSet" in { implicit ee: EE =>
      connection("admin").runCommand(
        ReplSetMaintenance(true)) must throwA[CommandError].
        await(2, timeout)
    }
  }
}
