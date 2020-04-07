package reactivemongo

import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.{ DB, ReadPreference }
import reactivemongo.api.bson.BSONDocument

import reactivemongo.core.errors.DatabaseException

import reactivemongo.api.commands.{
  CommandError,
  ReplSetGetStatus,
  ReplSetMaintenance
}

import org.specs2.concurrent.ExecutionEnv

import reactivemongo.api.tests.{ commands, decoder }

final class CommandSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification {

  "Commands" title

  import tests.Common
  import Common._

  "Raw command" should {
    "re-index test collection with command as document" >> {
      def reindexSpec(db: DB, coll: String, t: FiniteDuration) = {
        val reIndexDoc = BSONDocument("reIndex" -> coll)

        db(coll).create() must beTypedEqualTo({}).await(0, t) and {
          db.runCommand(reIndexDoc, db.failoverStrategy).
            one[BSONDocument](ReadPreference.primary) must beLike[BSONDocument] {
              case doc => decoder.double(doc, "ok") must beSome(1)
            }.await(1, t)
        }
      }

      "with the default connection" in eventually(2, timeout) {
        reindexSpec(db, s"commandspec${System identityHashCode db}", timeout)
      }

      "with the slow connection" in eventually(retries = 2, sleep = timeout) {
        reindexSpec(
          slowDb, s"commandspec${System identityHashCode slowDb}",
          slowTimeout)

      }
    }
  }

  "Admin" should {
    "execute replSetGetStatus" in {
      if (replSetOn) {
        replSetGetStatusTest.map(_.members.size) must beTypedEqualTo(1).
          await(0, timeout)
      } else {
        replSetGetStatusTest must throwA[DatabaseException].await(0, timeout)
      }
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
            Common.failoverStrategy)) must throwA[DatabaseException].like {
            case CommandError.Code(code) => code aka "error code" must_== 76
          }.await(0, timeout)
        }
      } else {
        "fail with replicaSet (MongoDB 3+)" in {
          connection.database("admin").flatMap(_.runCommand(
            ReplSetMaintenance(true),
            Common.failoverStrategy)) must throwA[DatabaseException].like {
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
