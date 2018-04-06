import scala.concurrent.duration.FiniteDuration

import reactivemongo.bson.{
  BSONDocument,
  BSONDocumentReader,
  BSONDocumentWriter
}

import reactivemongo.api.{ BSONSerializationPack, DefaultDB, ReadPreference }
import reactivemongo.api.commands._

import reactivemongo.api.collections.bson.BSONCollection

import org.specs2.concurrent.ExecutionEnv

class CommandSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification {

  "Commands" title

  import Common._

  "FindAndModify" should {
    sequential

    import reactivemongo.api.commands.bson.BSONFindAndModifyCommand._
    import reactivemongo.api.commands.bson.BSONFindAndModifyImplicits._

    val colName = s"FindAndModifySpec${System identityHashCode this}"
    lazy val collection = db(colName)
    lazy val slowColl = slowDb(colName)

    case class Person(
      firstName: String,
      lastName: String,
      age: Int)

    implicit object PersonReader extends BSONDocumentReader[Person] {
      def read(doc: BSONDocument): Person = Person(
        doc.getAs[String]("firstName").getOrElse(""),
        doc.getAs[String]("lastName").getOrElse(""),
        doc.getAs[Int]("age").getOrElse(0))
    }

    implicit object PersonWriter extends BSONDocumentWriter[Person] {
      def write(person: Person): BSONDocument = BSONDocument(
        "firstName" -> person.firstName,
        "lastName" -> person.lastName,
        "age" -> person.age)
    }

    val jack1 = Person("Jack", "London", 27)
    val jack2 = jack1.copy(age = /* updated to */ 40)

    "upsert a doc and fetch it" >> {
      val upsertOp = Update(
        BSONDocument(
          "$set" -> BSONDocument("age" -> 40)),
        fetchNewObject = true, upsert = true)

      def upsertAndFetch(c: BSONCollection, timeout: FiniteDuration) =
        c.runCommand(FindAndModify(jack1, upsertOp), ReadPreference.primary).
          aka("result") must (beLike[FindAndModifyResult] {
            case result =>
              println(s"FindAndModify.result = $result")
              result.lastError.exists(_.upsertedId.isDefined) must beTrue and (
                result.result[Person] aka "upserted" must beSome[Person](
                  jack2))
          }).await(1, timeout)

      "with the default connection" in {
        upsertAndFetch(collection, timeout)
      }

      "with the slow connection" in {
        upsertAndFetch(slowColl, slowTimeout)
      }
    }

    "modify a document and fetch its previous value" in {
      val incrementAge = BSONDocument(
        "$inc" -> BSONDocument("age" -> 1))

      def future = collection.findAndUpdate(jack2, incrementAge)

      future must (beLike[FindAndModifyResult] {
        case result =>
          result.result[Person] aka "previous value" must beSome(jack2) and {
            collection.find(jack2.copy(age = jack2.age + 1)).one[Person].
              aka("incremented") must beSome[Person].await(1, timeout)
          }
      }).await(1, timeout)
    }

    "fail" in {
      val query = BSONDocument()
      val future = collection.runCommand(
        FindAndModify(query, Update(BSONDocument("$inc" -> "age"))))

      future.map(_ => Option.empty[Int]).recover {
        case e: CommandError =>
          //e.printStackTrace
          e.code
      } must beSome( /*code = */ 9).await(1, timeout)
    }
  }

  "Raw command" should {
    "re-index test collection with command as document" >> {
      val runner = Command.run(BSONSerializationPack, db.failoverStrategy)
      def reindexSpec(db: DefaultDB, coll: String, timeout: FiniteDuration) = {
        val reIndexDoc = BSONDocument("reIndex" -> coll)

        db(coll).create() must beEqualTo({}).await(0, timeout) and {
          runner.apply(db, runner.rawCommand(reIndexDoc)).one[BSONDocument](
            ReadPreference.primary) must beLike[BSONDocument] {
              case doc => doc.getAs[Double]("ok") must beSome(1)
            }.await(1, timeout)
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

    "resync" in {
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
