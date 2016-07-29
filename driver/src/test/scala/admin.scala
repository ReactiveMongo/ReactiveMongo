import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.MongoConnection
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands._

import org.specs2.mutable.Specification

import Common._

import org.specs2.concurrent.{ ExecutionEnv => EE }

class IsMasterSpec extends Specification {
  "isMaster" title

  import bson.BSONIsMasterCommand._

  "BSON command" should {
    "be successful" in { implicit ee: EE =>
      test must beLike[IsMasterResult] {
        case IsMasterResult(true, _, _, _, Some(_), _, _, rs, _) =>
          if (replSetOn) {
            rs must beSome[ReplicaSet].like {
              case ReplicaSet(_, me, primary, _ :: _, _,
                _, false, false, false, false, _) =>
                primary aka "primary" must beSome(me)
            }
          } else {
            rs must beNone
          }
      }.await(0, timeout)
    }
  }

  private def test(implicit ee: EE) = {
    import bson.BSONIsMasterCommandImplicits._
    connection.database("admin").flatMap(_.runCommand(IsMaster))
  }
}

class RenameCollectionSpec extends Specification {
  "renameCollection" title

  "Collection" should {
    def spec(c: MongoConnection, timeout: FiniteDuration)(f: MongoConnection => Future[BSONCollection])(implicit ee: EE) = (for {
      coll <- f(c)
      _ <- coll.create()
      _ <- coll.rename(s"renamed${System identityHashCode c}")
    } yield ()) aka "renaming" must beEqualTo({}).await(0, timeout)

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

  "Database 'admin'" should {
    "rename collection if target doesn't exist" in { implicit ee: EE =>
      (for {
        admin <- connection.database("admin", failoverStrategy)
        c1 = db.collection(s"foo_${System identityHashCode admin}")
        _ <- c1.create()
        name = s"renamed_${System identityHashCode c1}"
        c2 <- admin.renameCollection(db.name, c1.name, name)
      } yield name -> c2.name) must beLike[(String, String)] {
        case (expected, name) => name aka "new name" must_== expected
      }.await(0, timeout)
    }

    "fail to rename collection if target exists" in { implicit ee: EE =>
      val c1 = db.collection(s"foo_${System identityHashCode ee}")

      (for {
        _ <- c1.create()
        name = s"renamed_${System identityHashCode c1}"
        c2 = db.collection(name)
        _ <- c2.create()
      } yield name) must beLike[String] {
        case name => name must not(beEqualTo(c1.name)) and {
          Await.result(for {
            admin <- connection.database("admin", failoverStrategy)
            _ <- admin.renameCollection(db.name, c1.name, name)
          } yield {}, timeout) must throwA[Exception].like {
            case err: CommandError =>
              err.errmsg aka err.toString must beSome[String].which {
                _.indexOf("target namespace exists") != -1
              }
          }
        }
      }.await(0, timeout)
    }
  }
}

class ReplSetGetStatusSpec extends Specification {
  "replSetGetStatus" title

  "BSON command" should {
    "be successful" in { implicit ee: EE =>
      if (replSetOn) {
        test must beLike[ReplSetStatus] {
          case ReplSetStatus(_, _, _, _ :: Nil) => ok
        }.await(0, timeout)
      } else {
        test must throwA[CommandError].await(0, timeout)
      }
    }
  }

  private def test(implicit ee: EE) = {
    import bson.BSONReplSetGetStatusImplicits._
    connection.database("admin").flatMap(_.runCommand(ReplSetGetStatus))
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
      }).await(0, timeout)
    }
  }

  "Database operation" should {
    "be successful" in { implicit ee: EE =>
      db.serverStatus must beLike[ServerStatusResult]({
        case status @ ServerStatusResult(_, _, MongodProcess, _, _, _, _, _) =>
          //println(s"Server status: $status")
          ok
      }).await(0, timeout)
    }
  }
}

class ResyncSpec extends Specification {
  "Resync" title

  "Resync BSON command" should {
    import bson.BSONResyncImplicits._

    if (!replSetOn) {
      "fail outside ReplicaSet (MongoDB 3+)" in { implicit ee: EE =>
        connection.database("admin").flatMap(_.runCommand(Resync)) must not(
          throwA[CommandError]
        ).await(0, timeout)
      } tag "not_mongo26"
    } else {
      "be successful with ReplicaSet (MongoDB 3+)" in { implicit ee: EE =>
        connection.database("admin").flatMap(_.runCommand(Resync)) must (
          throwA[CommandError].like {
            case CommandError.Code(c) => c aka "error code" must_== 95
          }
        ).await(0, timeout)
      } tag "not_mongo26"

      "be successful with ReplicaSet (MongoDB 2)" in { implicit ee: EE =>
        connection.database("admin").flatMap(_.runCommand(Resync)) must (
          throwA[CommandError].like {
            case CommandError.Message(msg) =>
              msg aka "error message" must_== "primaries cannot resync"
          }
        ).await(0, timeout)
      } tag "mongo2"
    }
  }
}

class ReplSetMaintenanceSpec extends Specification {
  "ReplSetMaintenance" title

  "BSON command" should {
    import bson.BSONReplSetMaintenanceImplicits._

    // MongoDB 3
    if (!replSetOn) {
      "fail outside replicaSet (MongoDB 3+)" in { implicit ee: EE =>
        connection.database("admin").flatMap(_.runCommand(
          ReplSetMaintenance(true)
        )) must throwA[CommandError].like {
          case CommandError.Code(code) => code aka "error code" must_== 76
        }.await(0, timeout)
      } tag "not_mongo26"
    } else {
      "fail with replicaSet (MongoDB 3+)" in { implicit ee: EE =>
        connection.database("admin").flatMap(_.runCommand(
          ReplSetMaintenance(true)
        )) must throwA[CommandError].like {
          case CommandError.Code(code) => code aka "error code" must_== 95
        }.await(0, timeout)
      } tag "not_mongo26"
    }

    // MongoDB 2.6
    if (!replSetOn) {
      "fail outside replicaSet (MongoDB 2.6)" in { implicit ee: EE =>
        connection.database("admin").flatMap(_.runCommand(
          ReplSetMaintenance(true)
        )) must throwA[CommandError].await(0, timeout)
      } tag "mongo2"
    } else {
      "fail with replicaSet (MongoDB 2.6)" in { implicit ee: EE =>
        connection.database("admin").flatMap(_.runCommand(
          ReplSetMaintenance(true)
        )) must throwA[CommandError].like {
          case CommandError.Message(msg) =>
            msg aka "message" must_== "primaries can't modify maintenance mode"
        }.await(0, timeout)
      } tag "mongo2"
    }

  }
}
