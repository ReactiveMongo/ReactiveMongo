import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.MongoConnection
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands._

import org.specs2.mutable.Specification

import Common._

import org.specs2.concurrent.{ ExecutionEnv => EE }

class RenameCollectionSpec extends Specification {
  "renameCollection" title

  "Collection" should {
    def spec(c: MongoConnection, timeout: FiniteDuration)(f: MongoConnection => Future[BSONCollection])(implicit ee: EE) = (for {
      coll <- f(c)
      _ <- coll.create()
      _ <- coll.rename(s"renamed${System identityHashCode c}")
    } yield ()) aka "renaming" must beEqualTo({}).await(1, timeout)

    "be renamed using the default connection" in { implicit ee: EE =>
      spec(connection, timeout) {
        _.database("admin").map(_(s"foo_${System identityHashCode db}"))
      }
    }

    "be renamed using the slow connection" in { implicit ee: EE =>
      spec(slowConnection, slowTimeout) {
        _.database("admin").map(_(s"foo_${System identityHashCode slowDb}"))
      }
    }
  }
}

class ReplSetGetStatusSpec extends Specification {
  "replSetGetStatus" title

  "BSON command" should {
    "be successful" in { implicit ee: EE =>
      import bson.BSONReplSetGetStatusImplicits._

      // TODO: Setup a successful replica set
      connection.database("admin").flatMap(_.runCommand(
        ReplSetGetStatus
      )) must throwA[CommandError].await(1, timeout)
    }
  }
}

class ServerStatusSpec extends Specification {
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
          //println(s"Server status: $status")
          ok
      }).await(1, timeout)
    }
  }
}

class ResyncSpec extends Specification {
  "Resync" title

  "BSON command" should {
    import bson.BSONResyncImplicits._

    "be successful" in { implicit ee: EE =>
      connection.database("admin").flatMap(_.runCommand(Resync)) must not(
        throwA[CommandError]
      ).await(1, timeout)
    }
  }
}

class ReplSetMaintenanceSpec extends Specification {
  "ReplSetMaintenance" title

  "BSON command" should {
    import bson.BSONReplSetMaintenanceImplicits._

    "fail outside replicaSet" in { implicit ee: EE =>
      connection.database("admin").flatMap(_.runCommand(
        ReplSetMaintenance(true)
      )) must throwA[CommandError].
        await(1, timeout)
    }
  }
}
