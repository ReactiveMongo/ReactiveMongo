import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._

import reactivemongo.bson.{ BSONArray, BSONBooleanLike, BSONDocument }

import reactivemongo.core.nodeset.{ Authenticate, ProtocolMetadata }
import reactivemongo.core.commands.{
  FailedAuthentication,
  SuccessfulAuthentication
}
import reactivemongo.core.actors.Exceptions.NodeSetNotReachable

import reactivemongo.api.{
  BSONSerializationPack,
  DefaultDB,
  FailoverStrategy,
  MongoConnection,
  MongoConnectionOptions,
  MongoDriver,
  ReadPreference,
  ScramSha1Authentication
}
import reactivemongo.api.commands.Command

import org.specs2.matcher.MatchResult
import org.specs2.concurrent.{ ExecutionEnv => EE }

class DriverSpec extends org.specs2.mutable.Specification {
  "Driver" title

  sequential

  import Common._

  val hosts = Seq(primaryHost)

  "Connection pool" should {
    "cleanly start and close with no connections #1" in {
      val md = MongoDriver()

      md.numConnections must_== 0 and (
        md.close() must not(throwA[Throwable]))

    }

    "cleanly start and close with no connections #2" in {
      val md = MongoDriver()

      md.numConnections must_== 0 and (
        md.close(timeout) must not(throwA[Throwable]))

    }

    "start and close with one connection open" in {
      val md = MongoDriver()
      val connection = md.connection(hosts)

      md.close(timeout) must not(throwA[Exception])
    }

    "start and close with multiple connections open" in {
      val md = MongoDriver()
      val connection1 = md.connection(hosts, name = Some("Connection1"))
      val connection2 = md.connection(hosts)
      val connection3 = md.connection(hosts)

      md.close(timeout) must not(throwA[Exception])
    }

    "use the failover strategy defined in the options" in { implicit ee: EE =>
      lazy val con = driver.connection(List(primaryHost),
        DefaultOptions.copy(failoverStrategy = FailoverStrategy.remote))

      con.database(commonDb).map(_.failoverStrategy).
        aka("strategy") must beTypedEqualTo(FailoverStrategy.remote).
        await(1, timeout) and {
          con.askClose()(timeout) must not(throwA[Exception]).await(1, timeout)
        }
    }

    "fail within expected timeout interval" in { implicit ee: EE =>
      lazy val con = driver.connection(List("foo:123"),
        DefaultOptions.copy(failoverStrategy = FailoverStrategy.remote))

      val before = System.currentTimeMillis()

      con.database(commonDb).map(_ => -1L).recover {
        case _ => System.currentTimeMillis()
      }.aka("duration") must beLike[Long] {
        case duration =>
          (duration must be_>=(1465655000000L)) and (
            duration must be_<(1468650000000L))
      }.await(1, 1468650000000L.milliseconds) and {
        con.askClose()(timeout) must not(throwA[Exception]).await(1, timeout)
      }
    }
  }

  "CR Authentication" should {
    lazy val drv = MongoDriver()
    lazy val connection = drv.connection(
      List(primaryHost),
      options = DefaultOptions.copy(nbChannelsPerNode = 1))

    val dbName = "specs2-test-cr-auth"
    def db_(implicit ec: ExecutionContext) =
      connection.database(dbName, failoverStrategy)

    val id = System.identityHashCode(drv)

    "be the default mode" in { implicit ee: EE =>
      db_.flatMap(_.drop()).map(_ => {}) must beEqualTo({}).await(1, timeout)
    } tag "mongo2"

    "create a user" in { implicit ee: EE =>
      val runner = Command.run(BSONSerializationPack)
      val createUser = BSONDocument("createUser" -> s"test-$id",
        "pwd" -> s"password-$id",
        "customData" -> BSONDocument.empty,
        "roles" -> BSONArray(
          BSONDocument("role" -> "readWrite", "db" -> dbName)))

      db_.flatMap {
        runner.apply(_, runner.rawCommand(createUser)).one[BSONDocument]
      } aka "creation" must (beLike[BSONDocument] {
        case doc => doc.getAs[BSONBooleanLike]("ok").
          exists(_.toBoolean == true) must beTrue
      }).await(1, timeout * 2)
    } tag "mongo2"

    "not be successful with wrong credentials" in { implicit ee: EE =>
      connection.authenticate(dbName, "foo", "bar").
        aka("authentication") must throwA[FailedAuthentication].
        await(1, timeout)

    } tag "mongo2"

    "be successful with right credentials" in { implicit ee: EE =>
      connection.authenticate(dbName, s"test-$id", s"password-$id").
        aka("authentication") must beLike[SuccessfulAuthentication](
          { case _ => ok }).await(1, timeout) and {
            db_.flatMap {
              _("testcol").insert(BSONDocument("foo" -> "bar")).map(_ => {})
            } must beEqualTo({}).await(1, timeout)
          }
    } tag "mongo2"

    "driver shutdown" in { // mainly to ensure the test driver is closed
      drv.close(timeout) must not(throwA[Exception])
    } tag "mongo2"

    "fail on DB without authentication" in { implicit ee: EE =>
      val auth = Authenticate(Common.commonDb, "test", "password")
      val conOpts = DefaultOptions.copy(nbChannelsPerNode = 1)

      def con = Common.driver.connection(
        List(primaryHost),
        options = conOpts,
        authentications = Seq(auth))

      Await.result(con.database(
        Common.commonDb, failoverStrategy), timeout).
        aka("database resulution") must throwA[NodeSetNotReachable]
    } tag "mongo2"
  }

  "Authentication SCRAM-SHA1" should {
    import Common.{ DefaultOptions, timeout, timeoutMillis }

    lazy val drv = MongoDriver()
    val conOpts = DefaultOptions.copy(
      authMode = ScramSha1Authentication,
      nbChannelsPerNode = 1)
    lazy val connection = drv.connection(
      List(primaryHost), options = conOpts)
    val slowOpts = SlowOptions.copy(
      authMode = ScramSha1Authentication, nbChannelsPerNode = 1)
    lazy val slowConnection = drv.connection(List(slowPrimary), slowOpts)

    val dbName = "specs2-test-scramsha1-auth"
    def db_(implicit ee: ExecutionContext) =
      connection.database(dbName, failoverStrategy)

    val id = System.identityHashCode(drv)

    "work only if configured" in { implicit ee: EE =>
      db_.flatMap(_.drop()).map(_ => {}) must beEqualTo({}).
        await(1, timeout * 2)
    } tag "not_mongo26"

    "create a user" in { implicit ee: EE =>
      val runner = Command.run(BSONSerializationPack)
      val createUser = BSONDocument("createUser" -> s"test-$id",
        "pwd" -> s"password-$id",
        "customData" -> BSONDocument.empty,
        "roles" -> BSONArray(
          BSONDocument("role" -> "readWrite", "db" -> dbName)))

      db_.flatMap {
        runner.apply(_, runner.rawCommand(createUser)).one[BSONDocument]
      } aka "creation" must (beLike[BSONDocument] {
        case doc => doc.getAs[BSONBooleanLike]("ok").
          exists(_.toBoolean == true) must beTrue
      }).await(1, timeout)
    } tag "not_mongo26"

    "not be successful with wrong credentials" >> {
      "with the default connection" in { implicit ee: EE =>
        connection.authenticate(dbName, "foo", "bar").
          aka("authentication") must throwA[FailedAuthentication].
          await(1, timeout)

      } tag "not_mongo26"

      "with the slow connection" in { implicit ee: EE =>
        slowConnection.authenticate(dbName, "foo", "bar").
          aka("authentication") must throwA[FailedAuthentication].
          await(1, slowTimeout)

      } tag "not_mongo26"
    }

    "be successful on existing connection with right credentials" >> {
      "with the default connection" in { implicit ee: EE =>
        connection.authenticate(dbName, s"test-$id", s"password-$id").
          aka("authentication") must beLike[SuccessfulAuthentication](
            { case _ => ok }).await(1, timeout) and {
              db_.flatMap {
                _("testcol").insert(BSONDocument("foo" -> "bar"))
              }.map(_ => {}) must beEqualTo({}).await(1, timeout * 2)
            }

      } tag "not_mongo26"

      "with the slow connection" in { implicit ee: EE =>
        slowConnection.authenticate(dbName, s"test-$id", s"password-$id").
          aka("authentication") must beLike[SuccessfulAuthentication](
            { case _ => ok }).await(1, slowTimeout)

      } tag "not_mongo26"
    }

    "be successful with right credentials" >> {
      val auth = Authenticate(dbName, s"test-$id", s"password-$id")

      "with the default connection" in { implicit ee: EE =>
        val con = drv.connection(
          List(primaryHost), options = conOpts, authentications = Seq(auth))

        con.database(dbName, Common.failoverStrategy).
          aka("authed DB") must beLike[DefaultDB] {
            case rdb => rdb.coll("testcol").flatMap(
              _.insert(BSONDocument("foo" -> "bar"))).map(_ => {}).
              aka("insertion") must beEqualTo({}).await(1, timeout)

          }.await(1, timeout) and {
            con.askClose()(timeout) must not(throwA[Exception]).
              await(1, timeout)
          }
      } tag "not_mongo26"

      "with the slow connection" in { implicit ee: EE =>
        val con = drv.connection(
          List(slowPrimary), options = slowOpts, authentications = Seq(auth))

        con.database(dbName, slowFailover).
          aka("authed DB") must beLike[DefaultDB] { case _ => ok }.
          await(1, slowTimeout) and {
            con.askClose()(slowTimeout) must not(throwA[Exception]).
              await(1, slowTimeout)
          }
      } tag "not_mongo26"
    }

    "driver shutdown" in { // mainly to ensure the test driver is closed
      drv.close(timeout) must not(throwA[Exception])
    } tag "not_mongo26"

    "fail on DB without authentication" >> {
      val auth = Authenticate(Common.commonDb, "test", "password")

      "with the default connection" in { implicit ee: EE =>
        def con = Common.driver.connection(
          List(primaryHost), options = conOpts, authentications = Seq(auth))

        con.database(Common.commonDb, failoverStrategy).
          aka("database resolution") must throwA[NodeSetNotReachable].
          await(1, timeout)

      } tag "not_mongo26"

      "with the slow connection" in { implicit ee: EE =>
        def con = Common.driver.connection(
          List(slowPrimary), options = slowOpts, authentications = Seq(auth))

        con.database(Common.commonDb, slowFailover).
          aka("database resolution") must throwA[NodeSetNotReachable].
          await(1, slowTimeout)

      } tag "not_mongo26"
    }
  }

  "Database" should {
    "be resolved from connection according the failover strategy" >> {
      "successfully" in { implicit ee: EE =>
        val fos = FailoverStrategy(FiniteDuration(50, "ms"), 20, _ * 2)

        Common.connection.database(Common.commonDb, fos).
          map(_ => {}) must beEqualTo({}).await(1, estTimeout(fos))

      }

      "with failure" in { implicit ee: EE =>
        lazy val con = Common.driver.connection(List("unavailable:27017"))
        val ws = scala.collection.mutable.ListBuffer.empty[Int]
        val expected = List(2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42)
        val fos = FailoverStrategy(FiniteDuration(50, "ms"), 20,
          { n => val w = n * 2; ws += w; w })
        val before = System.currentTimeMillis()

        con.database("foo", fos).map(_ => List.empty[Int]).
          recover({ case _ => ws.result() }) must beEqualTo(expected).
          await(1, timeout * 2) and {
            (before + estTimeout(fos).toMillis) must be ~ (
              System.currentTimeMillis() +/- 10000)
          }
      }
    }

    section("mongo2", "mongo24", "not_mongo26")
    "fail with MongoDB < 2.6" in { implicit ee: EE =>

      import reactivemongo.core.errors.ConnectionException

      Common.connection.database(Common.commonDb, failoverStrategy).
        map(_ => {}) aka "database resolution" must (
          throwA[ConnectionException]("unsupported MongoDB version")).
          await(1, timeout) and (Await.result(
            Common.connection.database(Common.commonDb), timeout).
            aka("database") must throwA[ConnectionException](
              "unsupported MongoDB version"))

    }
    section("mongo2", "mongo24", "not_mongo26")
  }
}
