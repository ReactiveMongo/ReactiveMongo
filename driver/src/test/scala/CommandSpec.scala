import scala.concurrent.duration.FiniteDuration

import reactivemongo.bson.BSONDocument

import reactivemongo.api.{ BSONSerializationPack, DefaultDB, ReadPreference }
import reactivemongo.api.commands._

import org.specs2.concurrent.ExecutionEnv

final class CommandSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification {

  "Commands" title

  import tests.Common
  import Common._

  "Raw command" should {
    "re-index test collection with command as document" >> {
      lazy val runner = Command.run(BSONSerializationPack, db.failoverStrategy)

      def reindexSpec(db: DefaultDB, coll: String, t: FiniteDuration) = {
        val reIndexDoc = BSONDocument("reIndex" -> coll)

        db(coll).create() must beEqualTo({}).await(0, t) and {
          runner.apply(db, runner.rawCommand(reIndexDoc)).one[BSONDocument](
            ReadPreference.primary) must beLike[BSONDocument] {
              case doc =>
                if (doc.getAs[Double]("ok").exists(_ != 1)) {
                  println(s"CommandSpec#reIndex: ${BSONDocument pretty doc}")
                }

                doc.getAs[Double]("ok") must beSome(1)
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
      import reactivemongo.api.commands.bson.BSONIsMasterCommand._
      import reactivemongo.api.commands.bson.BSONIsMasterCommandImplicits._

      val runner = Command.run(BSONSerializationPack, db.failoverStrategy)

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

    "expose serverStatus" >> {
      "using raw command" in {
        import bson.BSONServerStatusImplicits._

        db.runCommand(ServerStatus, Common.failoverStrategy).
          aka("result") must beLike[ServerStatusResult]({
            case ServerStatusResult(_, _, MongodProcess,
              _, _, _, _, _, _, _, _, _, _, _, _, _) =>
              //println(s"Server status: $status")
              ok
          }).await(0, timeout)
      }

      "using operation" in {
        db.serverStatus must beLike[ServerStatusResult]({
          case ServerStatusResult(_, _, MongodProcess,
            _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            //println(s"Server status: $status")
            ok
        }).await(0, timeout)
      }
    }

    "execute ReplSetMaintenance" in {
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
            Common.failoverStrategy)) must throwA[CommandError].
            await(0, timeout)
        } tag "mongo2"
      } else {
        "fail with replicaSet (MongoDB 2.6)" in {
          connection.database("admin").flatMap(_.runCommand(
            ReplSetMaintenance(true),
            Common.failoverStrategy)) must throwA[CommandError].like {
            case CommandError.Message(msg) =>
              msg aka "message" must beTypedEqualTo(
                "primaries can't modify maintenance mode")

          }.await(0, timeout)
        } tag "mongo2"
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
    import bson.BSONReplSetGetStatusImplicits._
    connection.database("admin").flatMap(
      _.runCommand(ReplSetGetStatus, Common.failoverStrategy))
  }
}
