import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.MongoConnection
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands._

import org.specs2.mutable.Specification

import Common._

import org.specs2.concurrent.ExecutionEnv

class IsMasterSpec(implicit ee: ExecutionEnv) extends Specification {
  "isMaster" title

  import bson.BSONIsMasterCommand._

  "BSON command" should {
    "be successful" in {
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

  private def test = {
    import bson.BSONIsMasterCommandImplicits._
    connection.database("admin").flatMap(
      _.runCommand(IsMaster, Common.failoverStrategy))
  }
}

class RenameCollectionSpec(implicit ee: ExecutionEnv) extends Specification {
  "renameCollection" title

  "Collection" should {
    def spec(c: MongoConnection, timeout: FiniteDuration)(f: MongoConnection => Future[BSONCollection]) = (for {
      coll <- f(c)
      _ <- coll.create()
      _ <- coll.rename(s"renamed${System identityHashCode c}")
    } yield ()) aka "renaming" must beEqualTo({}).await(0, timeout)

    "be renamed using the default connection" in {
      spec(connection, timeout) {
        _.database("admin").map(_(s"foo_${System identityHashCode db}"))
      }
    }

    "be renamed using the slow connection" in {
      spec(slowConnection, slowTimeout) {
        _.database("admin").map(_(s"foo_${System identityHashCode slowDb}"))
      }
    }
  }

  "Database 'admin'" should {
    "rename collection if target doesn't exist" in {
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

    "fail to rename collection if target exists" in {
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

class ReplSetGetStatusSpec(implicit ee: ExecutionEnv) extends Specification {
  "replSetGetStatus" title

  "BSON command" should {
    "be successful" in {
      if (replSetOn) {
        test must beLike[ReplSetStatus] {
          case ReplSetStatus(_, _, _, _ :: Nil) => ok
        }.await(0, timeout)
      } else {
        test must throwA[CommandError].await(0, timeout)
      }
    }
  }

  private def test = {
    import bson.BSONReplSetGetStatusImplicits._
    connection.database("admin").flatMap(
      _.runCommand(ReplSetGetStatus, Common.failoverStrategy))
  }
}

class ServerStatusSpec(implicit ee: ExecutionEnv) extends Specification {
  "serverStatus" title

  "BSON command" should {
    "be successful" in {
      import bson.BSONServerStatusImplicits._

      db.runCommand(
        ServerStatus, Common.failoverStrategy) must beLike[ServerStatusResult]({
        case ServerStatusResult(_, _, MongodProcess,
          _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          //println(s"Server status: $status")
          ok
      }).await(0, timeout)
    }
  }

  "Database operation" should {
    "be successful" in {
      db.serverStatus must beLike[ServerStatusResult]({
        case ServerStatusResult(_, _, MongodProcess,
          _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          //println(s"Server status: $status")
          ok
      }).await(0, timeout)
    }
  }
}

class ResyncSpec(implicit ee: ExecutionEnv) extends Specification {
  "Resync" title

  "Resync BSON command" should {
    import bson.BSONResyncImplicits._

    if (!replSetOn) {
      "fail outside ReplicaSet (MongoDB 3+)" in {
        connection.database("admin").flatMap(
          _.runCommand(Resync, Common.failoverStrategy)) must not(
            throwA[CommandError]).await(0, timeout)
      } tag "not_mongo26"
    } else {
      "be successful with ReplicaSet (MongoDB 3+)" in {
        connection.database("admin").flatMap(
          _.runCommand(Resync, Common.failoverStrategy)) must (
            throwA[CommandError].like {
              case CommandError.Code(c) =>
                (c == 95 || c == 96 /* 3.4*/ ) aka "error code" must beTrue
            }).await(0, timeout)
      } tag "not_mongo26"

      "be successful with ReplicaSet (MongoDB 2)" in {
        connection.database("admin").flatMap(
          _.runCommand(Resync, Common.failoverStrategy)) must (
            throwA[CommandError].like {
              case CommandError.Message(msg) =>
                msg aka "error message" must_== "primaries cannot resync"
            }).await(0, timeout)
      } tag "mongo2"
    }
  }
}

class ReplSetMaintenanceSpec(implicit ee: ExecutionEnv) extends Specification {
  "ReplSetMaintenance" title

  "BSON command" should {
    import bson.BSONReplSetMaintenanceImplicits._

    // MongoDB 3
    if (!replSetOn) {
      "fail outside replicaSet (MongoDB 3+)" in {
        connection.database("admin").flatMap(_.runCommand(
          ReplSetMaintenance(true),
          Common.failoverStrategy)) must throwA[CommandError].like {
          case CommandError.Code(code) => code aka "error code" must_== 76
        }.await(0, timeout)
      } tag "not_mongo26"
    } else {
      "fail with replicaSet (MongoDB 3+)" in {
        connection.database("admin").flatMap(_.runCommand(
          ReplSetMaintenance(true),
          Common.failoverStrategy)) must throwA[CommandError].like {
          case CommandError.Code(code) => code aka "error code" must_== 95
        }.await(0, timeout)
      } tag "not_mongo26"
    }

    // MongoDB 2.6
    if (!replSetOn) {
      "fail outside replicaSet (MongoDB 2.6)" in {
        connection.database("admin").flatMap(_.runCommand(
          ReplSetMaintenance(true),
          Common.failoverStrategy)) must throwA[CommandError].await(0, timeout)
      } tag "mongo2"
    } else {
      "fail with replicaSet (MongoDB 2.6)" in {
        connection.database("admin").flatMap(_.runCommand(
          ReplSetMaintenance(true),
          Common.failoverStrategy)) must throwA[CommandError].like {
          case CommandError.Message(msg) =>
            msg aka "message" must_== "primaries can't modify maintenance mode"
        }.await(0, timeout)
      } tag "mongo2"
    }

  }
}

class PingSpec(implicit ee: ExecutionEnv) extends Specification {
  "Ping Command" title

  "respond 1.0" >> {
    "with the default connection" in {
      connection.database("admin").
        flatMap(_.ping()) must beTrue.await(0, timeout)
    }

    "with the slow connection" in {
      slowConnection.database("admin").
        flatMap(_.ping()) must beTrue.await(0, timeout)
    }
  }
}
