import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

import reactivemongo.bson.{ BSONArray, BSONBooleanLike, BSONDocument }

import reactivemongo.core.commands.{
  FailedAuthentication,
  SuccessfulAuthentication
}

import reactivemongo.api.{
  BSONSerializationPack,
  MongoConnectionOptions,
  MongoDriver,
  ScramSha1Authentication
}
import reactivemongo.api.commands.Command

/** A Test Suite For MongoDriver */
object MongoDriverSpec extends org.specs2.mutable.Specification {
  "Driver" title

  sequential

  val hosts = Seq("localhost")

  "Connection pool" should {
    "start and close cleanly with no connections" in {
      val md = MongoDriver()

      md.numConnections must_== 0 and (
        md.close(FiniteDuration(500, "milliseconds")) must not(
          throwA[Throwable]))

    }

    "start and close with one connection open" in {
      val md = MongoDriver()
      val connection = md.connection(hosts)
      md.close(FiniteDuration(5, "seconds"))
      success
    }

    "start and close with multiple connections open" in {
      val md = MongoDriver()
      val connection1 = md.connection(hosts, name = Some("Connection1"))
      val connection2 = md.connection(hosts)
      val connection3 = md.connection(hosts)
      md.close(FiniteDuration(5, "seconds"))
      success
    }
  }

  "CR Authentication" should {
    import Common.{ timeout, timeoutMillis }

    lazy val driver = MongoDriver()
    lazy val connection = driver.connection(
      List("localhost:27017"), options = MongoConnectionOptions(nbChannelsPerNode = 1))

    lazy val db = {
      val _db = connection("specs2-test-reactivemongo-auth")
      Await.ready(_db.drop, timeout)
      _db
    }
    val id = System.identityHashCode(driver)

    "be the default mode" in (ok)

    "create a user" in {
      val runner = Command.run(BSONSerializationPack)
      val createUser = BSONDocument("createUser" -> s"test-$id",
        "pwd" -> s"password-$id", // TODO: create a command
        "customData" -> BSONDocument.empty,
        "roles" -> BSONArray(
          BSONDocument("role" -> "readWrite", "db" -> db.name)))

      runner.apply(db, runner.rawCommand(createUser)).one[BSONDocument].
        aka("creation") must (beLike[BSONDocument] {
          case doc => doc.getAs[BSONBooleanLike]("ok").
            exists(_.toBoolean == true) must beTrue
        }).await(timeoutMillis)
    } tag ("mongo2")

    "not be successful with wrong credentials" in {
      Await.result(connection.authenticate(db.name, "foo", "bar"), timeout).
        aka("authentication") must throwA[FailedAuthentication]

    } tag ("mongo2")

    "be successful with right credentials" in {
      connection.authenticate(db.name, s"test-$id", s"password-$id").
        aka("authentication") must beLike[SuccessfulAuthentication](
          { case _ => ok }).await(timeoutMillis) and (
            db("testcol").insert(BSONDocument("foo" -> "bar")).
            map(_ => {}) must beEqualTo({}).await(timeoutMillis))

    } tag ("mongo2")

    "driver shutdown" in { // mainly to ensure the test driver is closed
      driver.close() must not(throwA[Exception])
    } tag ("mongo2")
  }

  "Authentication SCRAM-SHA1" should {
    import Common.{ timeout, timeoutMillis }

    lazy val driver = MongoDriver()
    lazy val connection = driver.connection(
      List("localhost:27017"), options = MongoConnectionOptions(authMode = ScramSha1Authentication, nbChannelsPerNode = 1))

    lazy val db = {
      val _db = connection("specs2-test-reactivemongo-auth")
      Await.ready(_db.drop, timeout)
      _db
    }
    val id = System.identityHashCode(driver)

    "work only if configured" in (ok)

    "create a user" in {
      val runner = Command.run(BSONSerializationPack)
      val createUser = BSONDocument("createUser" -> s"test-$id",
        "pwd" -> s"password-$id", // TODO: create a command
        "customData" -> BSONDocument.empty,
        "roles" -> BSONArray(
          BSONDocument("role" -> "readWrite", "db" -> db.name)))

      runner.apply(db, runner.rawCommand(createUser)).one[BSONDocument].
        aka("creation") must (beLike[BSONDocument] {
          case doc => doc.getAs[BSONBooleanLike]("ok").
            exists(_.toBoolean == true) must beTrue
        }).await(timeoutMillis)
    } tag ("mongo3")

    "not be successful with wrong credentials" in {
      Await.result(connection.authenticate(db.name, "foo", "bar"), timeout).
        aka("authentication") must throwA[FailedAuthentication]

    } tag ("mongo3")

    "be successful with right credentials" in {
      connection.authenticate(db.name, s"test-$id", s"password-$id").
        aka("authentication") must beLike[SuccessfulAuthentication](
          { case _ => ok }).await(timeoutMillis) and (
            db("testcol").insert(BSONDocument("foo" -> "bar")).
            map(_ => {}) must beEqualTo({}).await(timeoutMillis))

    } tag ("mongo3")

    "driver shutdown" in { // mainly to ensure the test driver is closed
      driver.close() must not(throwA[Exception])
    } tag ("mongo3")
  }
}
