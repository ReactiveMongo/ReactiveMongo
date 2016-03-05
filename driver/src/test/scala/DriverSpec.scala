import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

import reactivemongo.bson.{ BSONArray, BSONBooleanLike, BSONDocument }

import reactivemongo.core.nodeset.Authenticate
import reactivemongo.core.commands.{
  FailedAuthentication,
  SuccessfulAuthentication
}
import reactivemongo.core.actors.Exceptions.NodeSetNotReachable

import reactivemongo.api.{
  BSONSerializationPack,
  FailoverStrategy,
  MongoDriver,
  ScramSha1Authentication
}
import reactivemongo.api.commands.Command

object DriverSpec extends org.specs2.mutable.Specification {
  "Driver" title

  sequential

  import Common.{
    DefaultOptions,
    failoverStrategy,
    primaryHost,
    timeout,
    timeoutMillis
  }
  val hosts = Seq(primaryHost)

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
    lazy val driver = MongoDriver()
    lazy val connection = driver.connection(
      List(primaryHost),
      options = DefaultOptions.copy(nbChannelsPerNode = 1))

    lazy val db = {
      val _db = connection.database(
        "specs2-test-reactivemongo-auth", failoverStrategy)
      Await.result(_db.flatMap(d => d.drop.map(_ => d)), timeout)
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
    } tag ("mongo2", "mongo26")

    "not be successful with wrong credentials" in {
      Await.result(connection.authenticate(db.name, "foo", "bar"), timeout).
        aka("authentication") must throwA[FailedAuthentication]

    } tag ("mongo2", "mongo26")

    "be successful with right credentials" in {
      connection.authenticate(db.name, s"test-$id", s"password-$id").
        aka("authentication") must beLike[SuccessfulAuthentication](
          { case _ => ok }).await(timeoutMillis) and (
            db("testcol").insert(BSONDocument("foo" -> "bar")).
            map(_ => {}) must beEqualTo({}).await(timeoutMillis))

    } tag ("mongo2", "mongo26")

    "driver shutdown" in { // mainly to ensure the test driver is closed
      driver.close() must not(throwA[Exception])
    } tag ("mongo2", "mongo26")

    "fail on DB without authentication" in {
      val auth = Authenticate(Common.db.name, "test", "password")
      val conOpts = DefaultOptions.copy(nbChannelsPerNode = 1)

      def con = Common.driver.connection(
        List(primaryHost),
        options = conOpts,
        authentications = Seq(auth))

      Await.result(con.database(
        Common.db.name, failoverStrategy), timeout).
        aka("database resulution") must throwA[NodeSetNotReachable.type]
    } tag ("mongo2", "mongo26")
  }

  "Authentication SCRAM-SHA1" should {
    import Common.{ DefaultOptions, timeout, timeoutMillis }

    lazy val driver = MongoDriver()
    val conOpts = DefaultOptions.copy(
      authMode = ScramSha1Authentication,
      nbChannelsPerNode = 1)
    lazy val connection = driver.connection(
      List(primaryHost), options = conOpts)

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
    } tag ("mongo3", "not_mongo26")

    "not be successful with wrong credentials" in {
      Await.result(connection.authenticate(db.name, "foo", "bar"), timeout).
        aka("authentication") must throwA[FailedAuthentication]

    } tag ("mongo3", "not_mongo26")

    "be successful on existing connection with right credentials" in {
      connection.authenticate(db.name, s"test-$id", s"password-$id").
        aka("authentication") must beLike[SuccessfulAuthentication](
          { case _ => ok }).await(timeoutMillis) and (
            db("testcol").insert(BSONDocument("foo" -> "bar")).
            map(_ => {}) must beEqualTo({}).await(timeoutMillis))

    } tag ("mongo3", "not_mongo26")

    "be successful with right credentials" in {
      val auth = Authenticate(db.name, s"test-$id", s"password-$id")
      val con = driver.connection(
        List(primaryHost),
        options = conOpts,
        authentications = Seq(auth))

      val before = System.currentTimeMillis()

      (for {
        rdb <- con.database("testcol", Common.failoverStrategy)
        col <- rdb.coll("testcol")
        _ <- col.insert(BSONDocument("foo" -> "bar"))
      } yield ()) must beEqualTo({}).await(timeoutMillis)
    } tag ("mongo3", "not_mongo26")

    "driver shutdown" in { // mainly to ensure the test driver is closed
      driver.close() must not(throwA[Exception])
    } tag ("mongo3", "not_mongo26")

    "fail on DB without authentication" in {
      val auth = Authenticate(Common.db.name, "test", "password")
      val conOpts = DefaultOptions.copy(
        authMode = ScramSha1Authentication,
        nbChannelsPerNode = 1)

      def con = Common.driver.connection(
        List(primaryHost),
        options = conOpts,
        authentications = Seq(auth))

      Await.result(con.database(
        Common.db.name, failoverStrategy), timeout).
        aka("database resolution") must throwA[NodeSetNotReachable.type]
    } tag ("mongo3", "not_mongo26")
  }

  "Database" should {
    "be resolved from connection according the failover strategy" >> {
      "successfully" in {
        val fos = FailoverStrategy(FiniteDuration(50, "ms"), 20, _ * 2)

        Common.connection.database(Common.db.name, fos).
          map(_ => {}) must beEqualTo({}).await(fos.maxTimeout.toMillis.toInt)

      }

      "with failure" in {
        lazy val con = Common.driver.connection(List("unavailable:27017"))
        val ws = scala.collection.mutable.ListBuffer.empty[Int]
        val expected = List(2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42)
        val fos = FailoverStrategy(FiniteDuration(50, "ms"), 20,
          { n => val w = n * 2; ws += w; w })
        val before = System.currentTimeMillis()

        con.database("foo", fos).map(_ => List.empty[Int]).
          recover({ case _ => ws.result() }) must beEqualTo(expected).
          await(timeoutMillis * 2) and (
            (before + fos.maxTimeout.toMillis) must beGreaterThanOrEqualTo(
              System.currentTimeMillis()))
      }
    }

    "fail with MongoDB < 2.6" in {
      import reactivemongo.core.errors.ConnectionException

      Await.result(Common.connection.database(
        Common.db.name, failoverStrategy).map(_ => {}), timeout).
        aka("database resolution") must (
          throwA[ConnectionException]("unsupported MongoDB version")) and (
            Common.connection(Common.db.name).
            aka("database") must throwA[ConnectionException](
              "unsupported MongoDB version"))

    } tag ("mongo2", "mongo24", "not_mongo26")
  }
}
