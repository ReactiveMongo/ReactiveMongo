import scala.concurrent.{ Await, Future }

import reactivemongo.api.{
  DB,
  MongoConnection,
  MongoConnectionOptions,
  ReadPreference,
  ScramSha256Authentication,
  WriteConcern
}
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.BSONCollection
import reactivemongo.api.commands.DBUserRole

import org.specs2.concurrent.ExecutionEnv

final class ClientMetadataSpec(
    implicit
    ee: ExecutionEnv)
    extends org.specs2.mutable.Specification {

  "Client metadata".title

  sequential

  import tests.Common
  import Common.{ timeout, DefaultOptions }

  "Application name" should {
    "be sent on every connection of the pool" >> {
      "without authentication" in {
        spec("probe-app", authenticated = false)
      }

      "with SCRAM authentication" in {
        spec("probe-app-scram", authenticated = true)
      }
    }
  }

  // ---

  private val nbChannels = 3

  private def spec(appName: String, authenticated: Boolean) = {
    val dbName = s"clientmetadataspec${System.nanoTime()}"
    val userName = s"probe-${System.nanoTime()}"
    val password = s"password-${System.nanoTime()}"

    def setUp(): Future[Unit] = for {
      db <- Common.connection.database(dbName)
      _ <- db
        .runCommand(BSONDocument("profile" -> 2), db.failoverStrategy)
        .one[BSONDocument](ReadPreference.primary)
      _ <- {
        if (!authenticated) Future.successful({})
        else {
          db.createUser(
            user = userName,
            pwd = Some(password),
            customData = Option.empty[BSONDocument],
            roles = List(DBUserRole("dbOwner", dbName)),
            digestPassword = true,
            writeConcern = WriteConcern.Default,
            restrictions = List.empty,
            mechanisms = List(ScramSha256Authentication)
          )
        }
      }
    } yield ()

    val options = {
      val opts = DefaultOptions.copy(
        nbChannelsPerNode = nbChannels,
        minIdleChannelsPerNode = nbChannels,
        appName = Some(appName)
      )

      if (!authenticated) opts
      else {
        opts.copy(
          authenticationMechanism = ScramSha256Authentication,
          authenticationDatabase = Some(dbName),
          credentials = Map(
            dbName -> MongoConnectionOptions
              .Credential(userName, Some(password))
          )
        )
      }
    }

    Await.result(setUp(), timeout)

    val connection: MongoConnection = Await.result(
      Common.driver.connect(List(Common.primaryHost), options),
      timeout
    )

    try {
      val coll: BSONCollection =
        Await.result(connection.database(dbName), timeout).collection("probe")

      coll.find(BSONDocument("probe" -> 1)).one[BSONDocument] must beNone
        .awaitFor(timeout) and {
        eventually(10, timeout / 10L) {
          profiledAppNames(dbName) must beSome(appName).awaitFor(timeout)
        }
      } and {
        eventually(10, timeout / 10L) {
          connectionsWithAppName(appName) must beTypedEqualTo(
            nbChannels + 1 /* signaling */
          ).awaitFor(timeout)
        }
      }
    } finally {
      try {
        Await.result(connection.close()(timeout), timeout)
      } catch {
        case scala.util.control.NonFatal(_) =>
      }

      ()
    }
  }

  private def profiledAppNames(dbName: String): Future[Option[String]] =
    Common.connection.database(dbName).flatMap { (db: DB) =>
      db.collection("system.profile")
        .find(BSONDocument("ns" -> s"${dbName}.probe", "op" -> "query"))
        .one[BSONDocument]
        .map {
          _.flatMap(_.getAsOpt[String]("appName"))
        }
    }

  private def connectionsWithAppName(appName: String): Future[Int] =
    Common.connection.database("admin").flatMap { admin =>
      admin
        .runCommand(
          BSONDocument(
            "currentOp" -> 1,
            "$all" -> true,
            "appName" -> appName
          ),
          admin.failoverStrategy
        )
        .one[BSONDocument](ReadPreference.primary)
        .map {
          _.getAsOpt[Seq[BSONDocument]]("inprog").fold(0)(_.size)
        }
    }
}
