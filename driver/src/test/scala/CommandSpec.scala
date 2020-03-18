import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.{ DB, ReadPreference }
import reactivemongo.api.bson.BSONDocument

import reactivemongo.api.commands.{
  Command,
  CommandError,
  IsMasterCommand,
  MongodProcess,
  ReplSetGetStatus,
  ReplSetMaintenance,
  ReplSetStatus,
  ServerStatus,
  ServerStatusResult
}

import org.specs2.concurrent.ExecutionEnv

import reactivemongo.api.tests.{ commands, decoder, pack }

import reactivemongo.api.TestCompat._

final class CommandSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification {

  "Commands" title

  import tests.Common
  import Common._

  "Raw command" should {
    "re-index test collection with command as document" >> {
      def reindexSpec(db: DB, coll: String, t: FiniteDuration) = {
        val reIndexDoc = BSONDocument("reIndex" -> coll)

        db(coll).create() must beEqualTo({}).await(0, t) and {
          db.runCommand(reIndexDoc, db.failoverStrategy).
            one[BSONDocument](ReadPreference.primary) must beLike[BSONDocument] {
              case doc => decoder.double(doc, "ok") must beSome(1)
            }.await(1, t)
        }
      }

      "with the default connection" in {
        reindexSpec(
          db, s"rawcommandspec${System identityHashCode db}", timeout)
      }

      "with the slow connection" in {
        reindexSpec(
          slowDb, s"rawcommandspec${System identityHashCode slowDb}",
          slowTimeout)
      }
    }

    "check isMaster" in {
      val runner = Command.run(pack, db.failoverStrategy)
      implicit val dr = dateReader

      val isMaster = new IsMasterCommand[pack.type] {}
      import isMaster._

      import scala.language.reflectiveCalls
      implicit val w = commands.isMasterWriter(isMaster).get[IsMaster.type]
      implicit val r = commands.isMasterReader(isMaster).get

      runner(db, IsMaster, ReadPreference.primary).
        map(_ => {}) must beEqualTo({}).await(1, timeout)
    }
  }

  "Admin" should {
    "execute replSetGetStatus" in {
      if (replSetOn) {
        replSetGetStatusTest must beLike[ReplSetStatus] {
          case ReplSetStatus(_, _, _, _ :: Nil) => ok
        }.await(0, timeout)
      } else {
        replSetGetStatusTest must throwA[CommandError].await(0, timeout)
      }
    }

    "expose serverStatus" in {
      import commands.{ serverStatusReader, serverStatusWriter }

      db.runCommand(ServerStatus, Common.failoverStrategy).
        aka("result") must beLike[ServerStatusResult]({
          case ServerStatusResult(_, _, MongodProcess,
            _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            //println(s"Server status: $status")
            ok
        }).await(0, timeout)
    }

    "execute ReplSetMaintenance" in {
      import commands.{
        replSetMaintenanceWriter,
        unitBoxReader
      }

      // MongoDB 3
      if (!replSetOn) {
        "fail outside replicaSet (MongoDB 3+)" in {
          connection.database("admin").flatMap(_.runCommand(
            ReplSetMaintenance(true),
            Common.failoverStrategy)) must throwA[CommandError].like {
            case CommandError.Code(code) => code aka "error code" must_== 76
          }.await(0, timeout)
        }
      } else {
        "fail with replicaSet (MongoDB 3+)" in {
          connection.database("admin").flatMap(_.runCommand(
            ReplSetMaintenance(true),
            Common.failoverStrategy)) must throwA[CommandError].like {
            case CommandError.Code(code) => code aka "error code" must_== 95
          }.await(0, timeout)
        }
      }
    }

    "response to ping with ok/1.0" in {
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

  // ---

  private def replSetGetStatusTest = {
    import commands.{ replSetStatusReader, replSetGetStatusWriter }

    connection.database("admin").flatMap(
      _.runCommand(ReplSetGetStatus, Common.failoverStrategy))
  }
}
