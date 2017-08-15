import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration._

import play.api.libs.iteratee.Iteratee

import reactivemongo.bson._
import reactivemongo.core.protocol.Response
import reactivemongo.api.{
  Cursor,
  FailoverStrategy,
  QueryOpts,
  MongoConnection,
  MongoDriver,
  WrappedCursor
}
import reactivemongo.api.collections.bson.BSONCollection

import org.specs2.concurrent.{ ExecutionEnv => EE }

class CursorSpec extends org.specs2.mutable.Specification
  with CursorSpecEnv with Cursor1Spec with TailableCursorSpec {

  "Cursor" title

  sequential

  "ReactiveMongo" should {
    group1

    @inline def con(drv: MongoDriver) =
      drv.connection(List(primaryHost), DefaultOptions)

    def scol(n: String = collName, failover: FailoverStrategy = failoverStrategy, tout: FiniteDuration = timeout, driver: MongoDriver, specCon: => MongoConnection = con(driver))(implicit ec: ExecutionContext) = Await.result(specCon.database(Common.commonDb, failover).map(_(n)), tout)

    "stop on error" >> {
      lazy val drv = new MongoDriver
      def withSlowDefaultColl[T](f: (ExecutionContext => BSONCollection) => T): T = {
        lazy val con = drv.connection(List(slowPrimary), SlowOptions)

        try {
          f(scol(
            specCon = con,
            failover = slowFailover,
            tout = slowTimeout,
            driver = drv)(_: ExecutionContext))
        } catch {
          case err: Throwable =>
            logger.error(s"[${con.name}] Fails to resolve the slow default collection", err)
            throw err
        }
      }

      def withSlowSpecColl[T](f: ((ExecutionContext, String) => BSONCollection) => T): T = f({ (ec: ExecutionContext, n: String) =>
        lazy val con = drv.connection(List(slowPrimary), SlowOptions)

        try {
          scol(n, slowFailover, slowTimeout, drv, con)(ec)
        } catch {
          case err: Throwable =>
            logger.error(s"[${con.name}] Fails to resolve the slow spec collection", err)

            throw err
        }
      })

      { // .foldResponse
        def foldRespSpec(defaultColl: ExecutionContext => BSONCollection, specCol: (ExecutionContext, String) => BSONCollection, timeout: FiniteDuration) = {
          "if fails while processing with existing documents" in {
            implicit ee: EE =>
              @volatile var count = 0
              def onError(last: Unit, e: Throwable): Unit = {
                debug(s"stopOnError: foldResponses (#1): $count")
                count = count + 1
              }
              val c = defaultColl(ee.ec)
              val cursor = c.find(matchAll("cursorspec11")).cursor()

              cursor.foldResponsesM({}, 128)(
                { (_, _) => Future[Cursor.State[Unit]](sys.error("Foo")) },
                Cursor.FailOnError[Unit](onError)).recover({ case _ => count }).
                aka("folding") must beEqualTo(1).await(1, timeout)

          }

          "if fails while processing w/o documents" in { implicit ee: EE =>
            @volatile var count = 0
            def onError(last: Unit, e: Throwable): Unit = {
              debug(s"stopOnError: foldResponses (#2): $count")
              count = count + 1
            }
            val c = specCol(ee.ec, System.identityHashCode(onError _).toString)
            val cursor = c.find(matchAll("cursorspec12")).cursor()

            cursor.foldResponses({}, 128)(
              { (_, _) => sys.error("Foo"): Cursor.State[Unit] },
              Cursor.FailOnError[Unit](onError)).recover({ case _ => count }).
              aka("folding") must beEqualTo(1).await(1, timeout)
          }

          "if fails with initial value" in { implicit ee: EE =>
            @volatile var count = 0
            def onError(last: Unit, e: Throwable): Unit = {
              debug(s"stopOnError: foldResponses (#3): $count")
              count = count + 1
            }
            val c = specCol(ee.ec, System.identityHashCode(onError _).toString)
            val cursor = c.find(matchAll("cursorspec13")).cursor()

            cursor.foldResponses[Unit](sys.error("Foo"), 128)(
              (_, _) => Cursor.Cont({}), Cursor.FailOnError[Unit](onError)).
              recover { case _ => count } must beEqualTo(0).await(1, timeout)
          }

          "if fails to send request" in { implicit ee: EE =>
            @volatile var count = 0
            def onError(last: Unit, e: Throwable): Unit = {
              debug(s"stopOnError: foldResponses (#4): $count")
              count = count + 1
            }
            val c = defaultColl(ee.ec)
            val cursor = c.find(matchAll("cursorspec14")).cursor()

            // Close connection to make the related cursor erroneous
            c.db.connection.askClose()(timeout).
              map(_ => {}) must beEqualTo({}).await(1, timeout) and {
                cursor.foldResponsesM({}, 128)(
                  (_, _) => Future(Cursor.Cont({})),
                  Cursor.FailOnError[Unit](onError)).
                  recover { case _ => count } must beEqualTo(1).
                  await(1, timeout)
              }
          }
        }

        "when folding responses with the default connection" >> {
          foldRespSpec(
            scol(
              driver = drv, specCon = con(drv))(_: ExecutionContext),
            { (ec, n) => scol(n, driver = drv, specCon = con(drv))(ec) },
            timeout)
        }

        "when folding responses with the slow connection" >> {
          withSlowDefaultColl { slowDefaultColl =>
            withSlowSpecColl { slowSpecColl =>
              foldRespSpec(slowDefaultColl, slowSpecColl, slowTimeout)
            }
          }
        }
      }

      { // .enumerateResponse
        def enumRespSpec(tag: String, defaultColl: ExecutionContext => BSONCollection, specCol: (ExecutionContext, String) => BSONCollection, timeout: FiniteDuration) = {
          "if fails while processing with existing documents" in {
            implicit ee: EE =>
              @volatile var count = 0
              val inc = Iteratee.foreach[Response] { _ =>
                debug(s"stopOnError: enumResp (#1): $count")
                if (count == 1) sys.error("Foo")

                count = count + 1
              }
              val c = defaultColl(ee.ec)
              val cursor = c.find(matchAll(s"$tag-spec15")).options(
                QueryOpts(batchSizeN = 2)).cursor()

              (cursor.enumerateResponses(10, true) |>>> inc).
                recover({ case _ => count }).
                aka("enumerating") must beEqualTo(1).await(1, timeout)
          }

          "if fails while processing w/o documents" in { implicit ee: EE =>
            @volatile var count = 0
            val inc = Iteratee.foreach[Response] { _ =>
              debug(s"stopOnError: enumResp (#2): $count")
              count = count + 1
              sys.error("Foo")
            }

            val c = specCol(ee.ec, System.identityHashCode(inc).toString)
            val cursor = c.find(matchAll(s"$tag-spec16")).options(
              QueryOpts(batchSizeN = 2)).cursor()

            (cursor.enumerateResponses(10, true) |>>> inc).
              recover({ case _ => count }) must beEqualTo(1).await(1, timeout)
          }

          "if fails to send request" in { implicit ee: EE =>
            @volatile var count = 0
            val inc = Iteratee.foreach[Response] { _ =>
              debug(s"stopOnError: enumResp (#3): $count")
              count = count + 1
            }
            val c = defaultColl(ee.ec)
            val cursor = c.find(matchAll(s"$tag-spec17")).cursor()

            // Close connection to make the related cursor erroneous
            c.db.connection.askClose()(timeout).
              map(_ => {}) must beEqualTo({}).await(1, timeout) and {
                (cursor.enumerateResponses(10, true) |>>> inc).
                  recover({ case _ => count }) must beEqualTo(0).
                  await(1, timeout)
              }
          }
        }

        "when enumerating responses with the default connection" >> {
          enumRespSpec("default", scol(
            driver = drv, specCon = con(drv))(_: ExecutionContext),
            { (ec, n) => scol(n, driver = drv, specCon = con(drv))(ec) },
            timeout)
        }

        "when enumerating responses with the slow connection" >> {
          withSlowDefaultColl { slowDefaultColl =>
            withSlowSpecColl { slowSpecColl =>
              enumRespSpec("slow", slowDefaultColl, slowSpecColl, slowTimeout)
            }
          }
        }
      }

      "when folding bulks" >> {
        "if fails while processing with existing documents" in {
          implicit ee: EE =>
            @volatile var count = 0
            def onError(last: Unit, e: Throwable): Unit = {
              debug(s"stopOnError: foldBulks (#1): $count")
              count = count + 1
            }
            val c = scol(driver = drv)
            val cursor = c.find(matchAll("cursorspec18")).options(
              QueryOpts(batchSizeN = 64)).cursor()

            cursor.foldBulksM({}, 128)(
              { (_, _) => Future[Cursor.State[Unit]](sys.error("Foo")) },
              Cursor.FailOnError[Unit](onError)).recover({ case _ => count }).
              aka("folding") must beEqualTo(1).await(1, timeout)

        }

        "if fails while processing w/o documents" in { implicit ee: EE =>
          @volatile var count = 0
          def onError(last: Unit, e: Throwable): Unit = {
            debug(s"stopOnError: foldBulks (#2): $count")
            count = count + 1
          }
          val c = scol(
            System.identityHashCode(onError _).toString,
            driver = drv)
          val cursor = c.find(matchAll("cursorspec19")).options(
            QueryOpts(batchSizeN = 64)).cursor()

          cursor.foldBulks({}, 128)(
            { (_, _) => sys.error("Foo"): Cursor.State[Unit] },
            Cursor.FailOnError[Unit](onError)).recover({ case _ => count }).
            aka("folding") must beEqualTo(1).await(1, timeout)
        }

        "if fails with initial value" in { implicit ee: EE =>
          @volatile var count = 0
          def onError(last: Unit, e: Throwable): Unit = {
            debug(s"stopOnError: foldBulks (#3): $count")
            count = count + 1
          }
          val c = scol(
            System.identityHashCode(onError _).toString, driver = drv)

          val cursor = c.find(matchAll("cursorspec20")).options(
            QueryOpts(batchSizeN = 64)).cursor()

          cursor.foldBulksM[Unit](sys.error("Foo"), 128)(
            (_, _) => Future(Cursor.Cont({})),
            Cursor.FailOnError[Unit](onError)).
            recover({ case _ => count }) must beEqualTo(0).await(1, timeout)

        }

        "if fails to send request" in { implicit ee: EE =>
          @volatile var count = 0
          def onError(last: Unit, e: Throwable): Unit = {
            debug(s"stopOnError: foldBulks (#4): $count")
            count = count + 1
          }
          val c = scol(driver = drv)
          val cursor = c.find(matchAll("cursorspec21")).options(
            QueryOpts(batchSizeN = 64)).cursor()

          // Close connection to make the related cursor erroneous
          c.db.connection.askClose()(timeout).
            map(_ => {}) must beEqualTo({}).await(1, timeout) and {
              cursor.foldBulks({}, 128)(
                { (_, _) => Cursor.Cont({}) },
                Cursor.FailOnError[Unit](onError)).
                recover({ case _ => count }) must beEqualTo(1).await(1, timeout)
            }
        }
      }

      "when enumerating bulks" >> {
        "if fails while processing with existing documents" in {
          implicit ee: EE =>
            @volatile var count = 0
            val inc = Iteratee.foreach[Iterator[BSONDocument]] { _ =>
              debug(s"stopOnError: enumerateBulks (#1): $count")
              if (count == 1) sys.error("Foo")

              count = count + 1
            }
            val c = scol(driver = drv)
            val cursor = c.find(matchAll("cursorspec22")).options(
              QueryOpts(batchSizeN = 2)).cursor()

            (cursor.enumerateBulks(10, true) |>>> inc).
              recover({ case _ => count }) must beEqualTo(1).await(1, timeout)
        }

        "if fails while processing w/o documents" in { implicit ee: EE =>
          @volatile var count = 0
          val inc = Iteratee.foreach[Iterator[BSONDocument]] { _ =>
            debug(s"stopOnError: enumerateBulks (#2): $count")
            count = count + 1
            sys.error("Foo")
          }
          val c = scol(System.identityHashCode(inc).toString, driver = drv)
          val cursor = c.find(matchAll("cursorspec23")).options(
            QueryOpts(batchSizeN = 2)).cursor()

          (cursor.enumerateBulks(10, true) |>>> inc).
            recover({ case _ => count }) must beEqualTo(1).
            await(1, timeout)
        }

        "if fails to send request" in { implicit ee: EE =>
          @volatile var count = 0
          val inc = Iteratee.foreach[Iterator[BSONDocument]] { _ =>
            debug(s"stopOnError: enumerateBulks (#3): $count")
            count = count + 1
          }
          val c = scol(driver = drv)
          val cursor = c.find(matchAll("cursorspec24")).cursor()

          // Close connection to make the related cursor erroneous
          c.db.connection.askClose()(timeout).
            map(_ => {}) must beEqualTo({}).await(1, timeout) and {
              (cursor.enumerateBulks(10, true) |>>> inc).
                recover({ case _ => count }) must beEqualTo(0).
                await(1, timeout)
            }
        }
      }

      { // .foldWhile
        def foldWhileSpec(defaultColl: ExecutionContext => BSONCollection, specCol: (ExecutionContext, String) => BSONCollection, timeout: FiniteDuration) = {
          "if fails while processing with existing documents" in { implicit ee: EE =>
            @volatile var count = 0
            def onError(last: Unit, e: Throwable): Unit = {
              debug(s"stopOnError: foldWhile (#1): $count")
              count = count + 1
            }
            val c = defaultColl(ee.ec)
            val cursor = c.find(matchAll("cursorspec25")).cursor()

            cursor.foldWhileM({}, 128)(
              { (_, _) => Future[Cursor.State[Unit]](sys.error("Foo")) },
              Cursor.FailOnError[Unit](onError)).recover({ case _ => count }).
              aka("folding") must beEqualTo(1).await(1, timeout)

          }

          "if fails with initial value" in { implicit ee: EE =>
            @volatile var count = 0
            def onError(last: Unit, e: Throwable): Unit = {
              debug(s"stopOnError: foldWhile (#2): $count")
              count = count + 1
            }
            val c = defaultColl(ee.ec)
            val cursor = c.find(matchAll("cursorspec26")).cursor()

            cursor.foldWhile[Unit](sys.error("Foo"), 128)(
              (_, _) => Cursor.Cont({}), Cursor.FailOnError[Unit](onError)).
              recover({ case _ => count }) must beEqualTo(0).
              await(1, timeout)
          }

          "if fails to send request" in { implicit ee: EE =>
            @volatile var count = 0
            def onError(last: Unit, e: Throwable): Unit = {
              debug(s"stopOnError: foldWhile (#3): $count")
              count = count + 1
            }
            val c = defaultColl(ee.ec)
            val cursor = c.find(matchAll("cursorspec27")).cursor()

            // Close connection to make the related cursor erroneous
            c.db.connection.askClose()(timeout).
              map(_ => {}) must beEqualTo({}).await(1, timeout) and {
                cursor.foldWhile({}, 128)(
                  (_, _) => Cursor.Cont({}),
                  Cursor.FailOnError[Unit](onError)).
                  recover({ case _ => count }).
                  aka("folding") must beEqualTo(1).await(1, timeout)
              }
          }
        }

        "when folding documents with the default connection" >> {
          foldWhileSpec(
            scol(driver = drv)(_: ExecutionContext),
            { (ec, n) => scol(n, driver = drv)(ec) }, timeout)
        }

        "when enumerating responses with the slow connection" >> {
          withSlowDefaultColl { slowDefaultColl =>
            withSlowSpecColl { slowSpecColl =>
              foldWhileSpec(slowDefaultColl, slowSpecColl, slowTimeout)
            }
          }
        }
      }

      { // .enumerate (deprecated)
        def enumSpec(defaultColl: ExecutionContext => BSONCollection, specCol: (ExecutionContext, String) => BSONCollection, timeout: FiniteDuration) = {
          "if fails while processing with existing documents" in {
            implicit ee: EE =>
              @volatile var count = 0
              val inc = Iteratee.foreach[BSONDocument] { _ =>
                debug(s"stopOnError: enumerate (#1): $count")
                if (count == 5) sys.error("Foo")

                count = count + 1
              }
              val c = defaultColl(ee.ec)
              val cursor = c.find(matchAll("cursorspec28")).cursor()

              (cursor.enumerate(10, true) |>>> inc).
                recover({ case _ => count }) must beEqualTo(5).await(1, timeout)
          }

          "if fails to send request" in { implicit ee: EE =>
            @volatile var count = 0
            val inc = Iteratee.foreach[BSONDocument] { _ =>
              debug(s"stopOnError: enumerate (#2): $count")
              count = count + 1
            }
            val c = defaultColl(ee.ec)
            val cursor = c.find(matchAll("cursorspec29")).cursor()

            // Close connection to make the related cursor erroneous
            c.db.connection.askClose()(timeout).
              map(_ => {}) must beEqualTo({}).await(1, timeout) and {
                (cursor.enumerate(10, true) |>>> inc).
                  recover({ case _ => count }).
                  aka("enumerating") must beEqualTo(0).await(1, timeout)
              }
          }
        }

        "when enumerating documents with the default connection" >> {
          enumSpec(
            scol(driver = drv)(_: ExecutionContext),
            { (ec, n) => scol(n, driver = drv)(ec) }, timeout)

        }

        "when enumerating document with the slow connection" >> {
          withSlowDefaultColl { slowDefaultColl =>
            withSlowSpecColl { slowSpecColl =>
              enumSpec(slowDefaultColl, slowSpecColl, slowTimeout)
            }
          }
        }
      }

      "Driver instance must be closed" in {
        drv.close(timeout) must not(throwA[Exception])
      }
    }

    "continue on error" >> {
      lazy val drv = new MongoDriver
      def withSlowDefaultColl[T](f: (ExecutionContext => BSONCollection) => T): T = {
        lazy val con = drv.connection(List(slowPrimary), SlowOptions)

        try {
          f(scol(
            specCon = con,
            failover = slowFailover,
            tout = slowTimeout,
            driver = drv)(_: ExecutionContext))
        } catch {
          case err: Throwable =>
            logger.error(s"[${con.name}] Fails to resolve the slow default collection", err)
            throw err
        }
      }

      def withSlowSpecColl[T](f: ((ExecutionContext, String) => BSONCollection) => T): T = f({ (ec: ExecutionContext, n: String) =>
        lazy val con = drv.connection(List(slowPrimary), SlowOptions)

        try {
          scol(n, slowFailover, slowTimeout, drv, con)(ec)
        } catch {
          case err: Throwable =>
            logger.error(s"[${con.name}] Fails to resolve the slow spec collection", err)

            throw err
        }
      })

      { // .foldResponses
        def foldRespSpec(defaultColl: ExecutionContext => BSONCollection, specCol: (ExecutionContext, String) => BSONCollection, timeout: FiniteDuration) = {
          def delayedTimeout = FiniteDuration(
            (timeout.toMillis * 1.25D).toLong, MILLISECONDS)

          "if fails while processing with existing documents" in {
            implicit ee: EE =>
              @volatile var count = 0
              def onError(last: Unit, e: Throwable): Unit = {
                debug(s"continueOnError: foldResponses (#1): $count")
                count = count + 1
              }
              val cursor = defaultColl(ee.ec).
                find(matchAll("cursorspec30")).cursor()

              // retry on the initial failure - until the max (128) is reached
              cursor.foldResponsesM({}, 128)(
                { (_, _) => Future[Cursor.State[Unit]](sys.error("Foo")) },
                Cursor.ContOnError[Unit](onError)).map(_ => count) must beEqualTo(128).await(2, delayedTimeout)
          }

          "if fails while processing w/o documents" in { implicit ee: EE =>
            @volatile var count = 0
            def onError(last: Unit, e: Throwable): Unit = {
              debug(s"continueOnError: foldResponses (#2): $count")
              count = count + 1
            }
            val c = specCol(ee.ec, System.identityHashCode(onError _).toString)
            val cursor = c.find(matchAll("cursorspec31")).cursor()

            cursor.foldResponses({}, 64)(
              { (_, _) => sys.error("Foo"): Cursor.State[Unit] },
              Cursor.ContOnError[Unit](onError)).map(_ => count) must beEqualTo(64).await(2, delayedTimeout)
          }

          "if fails with initial value" in { implicit ee: EE =>
            @volatile var count = 0
            def onError(last: Unit, e: Throwable): Unit = {
              debug(s"continueOnError: foldResponses (#3): $count")
              count = count + 1
            }
            val c = specCol(ee.ec, System.identityHashCode(onError _).toString)
            val cursor = c.find(matchAll("cursorspec32")).cursor()

            cursor.foldResponsesM[Unit](sys.error("Foo"), 128)(
              (_, _) => Future(Cursor.Cont({})),
              Cursor.ContOnError[Unit](onError)).
              recover({ case _ => count }) must beEqualTo(0).
              await(2, delayedTimeout)
          }

          "if fails to send request" in { implicit ee: EE =>
            @volatile var count = 0
            def onError(last: Unit, e: Throwable): Unit = {
              debug(s"continueOnError: foldResponses (#4): $count")
              count = count + 1
            }
            val c = defaultColl(ee.ec)
            val cursor = c.find(matchAll("cursorspec33")).cursor()

            // Close connection to make the related cursor erroneous
            c.db.connection.askClose()(timeout).
              map(_ => {}) must beEqualTo({}).await(1, timeout) and {
                cursor.foldResponses({}, 128)(
                  (_, _) => Cursor.Cont({}),
                  Cursor.ContOnError[Unit](onError)).map(_ => count).
                  aka("folding") must beEqualTo(1).await(2, delayedTimeout)
              }
          }
        }

        "when folding responses with the default connection" >> {
          foldRespSpec(
            scol(driver = drv)(_: ExecutionContext),
            { (ec, n) => scol(n, driver = drv)(ec) }, timeout)
        }

        "when folding responses with the slow connection" >> {
          withSlowDefaultColl { slowDefaultColl =>
            withSlowSpecColl { slowSpecColl =>
              foldRespSpec(slowDefaultColl, slowSpecColl, slowTimeout)
            }
          }
        }
      }

      { // .enumerateResponses
        def enumRespSpec(defaultColl: ExecutionContext => BSONCollection, specCol: (ExecutionContext, String) => BSONCollection, timeout: FiniteDuration) = {
          def delayedTimeout = FiniteDuration(
            (timeout.toMillis * 1.25D).toLong, MILLISECONDS)

          "if fails while processing with existing documents" in {
            implicit ee: EE =>
              @volatile var count = 0
              var i = 0
              val inc = Iteratee.foreach[Response] { _ =>
                debug(s"continueOnError: enumerateResponses (#1): $count")
                i = i + 1
                if (i % 2 == 0) sys.error("Foo")
                count = count + 1
              }
              val c = defaultColl(ee.ec)
              val cursor = c.find(matchAll("cursorspec34")).options(QueryOpts(
                batchSizeN = 4)).cursor()

              (cursor.enumerateResponses(128, false) |>>> inc).map(_ => count).
                aka("enumerating") must beEqualTo(16).await(2, delayedTimeout)
          }

          "if fails while processing w/o documents" in { implicit ee: EE =>
            @volatile var count = 0
            val inc = Iteratee.foreach[Response] { _ =>
              debug(s"continueOnError: enumerateResponses (#2): $count")
              count = count + 1
              sys.error("Foo")
            }
            val c = specCol(ee.ec, System.identityHashCode(inc).toString)
            val cursor = c.find(matchAll("cursorspec35")).
              options(QueryOpts(batchSizeN = 2)).cursor()

            (cursor.enumerateResponses(64, false) |>>> inc).map(_ => count).
              aka("enumerating") must beEqualTo(1).
              await(2, delayedTimeout)
          }

          "if fails to send request" in { implicit ee: EE =>
            @volatile var count = 0
            val inc = Iteratee.foreach[Response] { _ =>
              debug(s"continueOnError: enumerateResponses (#3): $count")
              count = count + 1
            }
            val c = defaultColl(ee.ec)
            val cursor = c.find(matchAll("cursorspec36")).cursor()

            // Close connection to make the related cursor erroneous
            c.db.connection.askClose()(timeout).
              map(_ => {}) must beEqualTo({}).await(1, timeout) and {
                (cursor.enumerateResponses(128, false) |>>> inc).
                  recover({ case _ => count }) must beEqualTo(0).
                  await(1, timeout)
              }
          }
        }

        "when enumerating responses with the default connection" >> {
          enumRespSpec(
            scol(driver = drv)(_: ExecutionContext),
            { (ec, n) => scol(n, driver = drv)(ec) }, timeout)

        }

        "when enumerating responses with the default connection" >> {
          withSlowDefaultColl { slowDefaultColl =>
            withSlowSpecColl { slowSpecColl =>
              enumRespSpec(slowDefaultColl, slowSpecColl, slowTimeout)
            }
          }
        }
      }

      "when folding bulks" >> {
        lazy val delayedTimeout = FiniteDuration(
          (timeout.toMillis * 1.25D).toLong, MILLISECONDS)

        "if fails while processing with existing documents" in {
          implicit ee: EE =>
            @volatile var count = 0
            def onError(last: Unit, e: Throwable): Unit = {
              debug(s"continueOnError: foldBulks (#1): $count")
              count = count + 1
            }
            val cursor = scol(driver = drv).find(matchAll("cursorspec37")).
              options(QueryOpts(batchSizeN = 64)).cursor()

            cursor.foldBulks({}, 128)(
              { (_, _) => sys.error("Foo"): Cursor.State[Unit] },
              Cursor.ContOnError[Unit](onError)).map(_ => count).
              aka("folding") must beEqualTo(128).await(2, delayedTimeout)
        }

        "if fails while processing w/o documents" in { implicit ee: EE =>
          @volatile var count = 0
          def onError(last: Unit, e: Throwable): Unit = {
            debug(s"continueOnError: foldBulks (#2): $count")
            count = count + 1
          }
          val c = scol(
            System.identityHashCode(onError _).toString,
            driver = drv)
          val cursor = c.find(matchAll("cursorspec38")).options(
            QueryOpts(batchSizeN = 64)).cursor()

          cursor.foldBulksM({}, 64)(
            { (_, _) => Future[Cursor.State[Unit]](sys.error("Foo")) },
            Cursor.ContOnError[Unit](onError)).map(_ => count).
            aka("folding") must beEqualTo(64).await(2, delayedTimeout)

        }

        "if fails with initial value" in { implicit ee: EE =>
          @volatile var count = 0
          def onError(last: Unit, e: Throwable): Unit = {
            debug(s"continueOnError: foldBulks (#3): $count")
            count = count + 1
          }
          val c = scol(
            System.identityHashCode(onError _).toString,
            driver = drv)
          val cursor = c.find(matchAll("cursorspec39")).options(
            QueryOpts(batchSizeN = 64)).cursor()

          cursor.foldBulks[Unit](sys.error("Foo"), 128)(
            (_, _) => Cursor.Cont({}), Cursor.ContOnError[Unit](onError)).
            recover({ case _ => count }) must beEqualTo(0).
            await(2, delayedTimeout)
        }

        "if fails to send request" in { implicit ee: EE =>
          @volatile var count = 0
          def onError(last: Unit, e: Throwable): Unit = {
            debug(s"continueOnError: foldBulks (#4): $count")
            count = count + 1
          }
          val c = scol(driver = drv)
          val cursor = c.find(matchAll("cursorspec40")).options(
            QueryOpts(batchSizeN = 64)).cursor()

          // Close connection to make the related cursor erroneous
          c.db.connection.askClose()(timeout).
            map(_ => {}) must beEqualTo({}).await(1, timeout) and {
              cursor.foldBulks({}, 128)(
                { (_, _) => Cursor.Cont({}) },
                Cursor.ContOnError[Unit](onError)).
                map(_ => count) must beEqualTo(1).await(2, delayedTimeout)
            }
        }
      }

      "when enumerating bulks" >> {
        lazy val delayedTimeout = FiniteDuration(
          (timeout.toMillis * 1.25D).toLong, MILLISECONDS)

        "if fails while processing with existing documents" in {
          implicit ee: EE =>
            @volatile var count = 0
            var i = 0
            val inc = Iteratee.foreach[Iterator[BSONDocument]] { _ =>
              debug(s"continueOnError: enumerateBulks (#1): $i")
              i = i + 1
              if (i % 2 == 0) sys.error("Foo")
              count = count + 1
            }
            val c = scol(driver = drv, failover = slowFailover)
            val cursor = c.find(matchAll("cursorspec41")).options(QueryOpts(
              batchSizeN = 4)).cursor()

            (cursor.enumerateBulks(128, false) |>>> inc).map(_ => count).
              aka("enumerating") must beEqualTo(16).await(2, delayedTimeout)
        }

        "if fails while processing w/o documents" in { implicit ee: EE =>
          @volatile var count = 0
          val inc = Iteratee.foreach[Iterator[BSONDocument]] { _ =>
            debug(s"continueOnError: enumerateBulks (#2): $count")
            count = count + 1
            sys.error("Foo")
          }
          val c = scol(System.identityHashCode(inc).toString, driver = drv)
          val cursor = c.find(matchAll("cursorspec42")).
            options(QueryOpts(batchSizeN = 2)).cursor()

          (cursor.enumerateBulks(64, false) |>>> inc).map(_ => count).
            aka("enumerating") must beEqualTo(1).await(2, delayedTimeout)
        }

        "if fails to send request" in { implicit ee: EE =>
          @volatile var count = 0
          val inc = Iteratee.foreach[Iterator[BSONDocument]] {
            debug(s"continueOnError: enumerateBulks (#3): $count")
            _ => count = count + 1
          }
          val c = scol(driver = drv)
          val cursor = c.find(matchAll("cursorspec43")).cursor()

          // Close connection to make the related cursor erroneous
          c.db.connection.askClose()(timeout).
            map(_ => {}) must beEqualTo({}).await(1, timeout) and {
              (cursor.enumerateBulks(128, false) |>>> inc).
                recover({ case _ => count }) must beEqualTo(0).await(1, timeout)
            }
        }
      }

      { // .foldWhile
        def foldWhileSpec(defaultColl: ExecutionContext => BSONCollection, specCol: (ExecutionContext, String) => BSONCollection, timeout: FiniteDuration) = {
          def delayedTimeout = FiniteDuration(
            (timeout.toMillis * 1.25D).toLong, MILLISECONDS)

          "if fails while processing with existing documents" in {
            implicit ee: EE =>
              @volatile var count = 0
              def onError(last: Unit, e: Throwable): Unit = {
                debug(s"continueOnError: foldWhile(#1): $count")
                count = count + 1
              }
              val cursor = defaultColl(ee.ec).
                find(matchAll("cursorspec44")).cursor()

              cursor.foldWhileM({}, 128)(
                { (_, _) => Future[Cursor.State[Unit]](sys.error("Foo")) },
                Cursor.ContOnError[Unit](onError)).map(_ => count).
                aka("folding") must beEqualTo(128).await(2, delayedTimeout)

          }

          "if fails with initial value" in { implicit ee: EE =>
            @volatile var count = 0
            def onError(last: Unit, e: Throwable): Unit = {
              debug(s"continueOnError: foldWhile(#2): $count")
              count = count + 1
            }
            val c = defaultColl(ee.ec)
            val cursor = c.find(matchAll("cursorspec45")).cursor()

            cursor.foldWhile[Unit](sys.error("Foo"), 128)(
              (_, _) => Cursor.Cont({}), Cursor.ContOnError[Unit](onError)).
              recover({ case _ => count }) must beEqualTo(0).
              await(2, delayedTimeout)
          }

          "if fails to send request" in { implicit ee: EE =>
            @volatile var count = 0
            def onError(last: Unit, e: Throwable): Unit = {
              debug(s"continueOnError: foldWhile(#3): $count")
              count = count + 1
            }
            val c = defaultColl(ee.ec)
            val cursor = c.find(matchAll("cursorspec46")).cursor()

            // Close connection to make the related cursor erroneous
            c.db.connection.askClose()(timeout).
              map(_ => {}) must beEqualTo({}).await(1, timeout) and {
                cursor.foldWhileM({}, 64)(
                  (_, _) => Future(Cursor.Cont({})),
                  Cursor.ContOnError[Unit](onError)).map(_ => count).
                  aka("folding") must beEqualTo(1).await(1, timeout)

              }
          }
        }

        "when folding documents with the default connection" >> {
          foldWhileSpec(
            scol(driver = drv)(_: ExecutionContext),
            { (ec, n) => scol(n, driver = drv)(ec) }, timeout)
        }

        "when folding documents with the default connection" >> {
          withSlowDefaultColl { slowDefaultColl =>
            withSlowSpecColl { slowSpecColl =>
              foldWhileSpec(slowDefaultColl, slowSpecColl, slowTimeout)
            }
          }
        }
      }

      { // .enumerate
        def enumSpec(defaultColl: ExecutionContext => BSONCollection, specCol: (ExecutionContext, String) => BSONCollection, timeout: FiniteDuration) = {
          "if fails while processing with existing documents" in {
            implicit ee: EE =>
              @volatile var count = 0
              var i = 0
              val inc = Iteratee.foreach[BSONDocument] { _ =>
                debug(s"continueOnError: enumerate(#1): $i")
                i = i + 1
                if (i % 2 == 0) sys.error("Foo")
                count = count + 1
              }
              val c = defaultColl(ee.ec)
              val cursor = c.find(matchAll("cursorspec47")).options(QueryOpts(
                batchSizeN = 4)).cursor()

              (cursor.enumerate(128, false) |>>> inc).map(_ => count).
                aka("enumerating") must beEqualTo(32).await(1, timeout)
          }

          "if fails while processing w/o documents" in { implicit ee: EE =>
            @volatile var count = 0
            val inc = Iteratee.foreach[BSONDocument] { _ =>
              debug(s"continueOnError: enumerate(#2): $count")
              count = count + 1
              sys.error("Foo")
            }
            val c = specCol(ee.ec, System.identityHashCode(inc).toString)
            val cursor = c.find(matchAll("cursorspec48")).
              options(QueryOpts(batchSizeN = 2)).cursor()

            (cursor.enumerate(64, false) |>>> inc).map(_ => count).
              aka("enumerating") must beEqualTo(0).await(1, timeout)
          }

          "if fails to send request" in { implicit ee: EE =>
            @volatile var count = 0
            val inc = Iteratee.foreach[BSONDocument] { _ =>
              debug(s"continueOnError: enumerate(#3): $count")
              count = count + 1
            }
            val c = defaultColl(ee.ec)
            val cursor = c.find(matchAll("cursorspec49")).cursor()

            // Close connection to make the related cursor erroneous
            c.db.connection.askClose()(timeout).
              map(_ => {}) must beEqualTo({}).await(1, timeout) and {
                (cursor.enumerate(128, false) |>>> inc).
                  recover({ case _ => count }) must beEqualTo(0).
                  await(1, timeout)
              }
          }
        }

        "when enumerating documents with the default connection" >> {
          enumSpec(
            scol(driver = drv)(_: ExecutionContext),
            { (ec, n) => scol(n, driver = drv)(ec) }, timeout)
        }

        "when enumerating documents with the slow connection" >> {
          withSlowDefaultColl { slowDefaultColl =>
            withSlowSpecColl { slowSpecColl =>
              enumSpec(slowDefaultColl, slowSpecColl, slowTimeout)
            }
          }
        }
      }

      "Driver instance must be closed" in {
        drv.close(timeout) must not(throwA[Exception])
      }
    }

    /*
    "Benchmark" in { implicit ee: EE =>
      import reactivemongo.api.tests

      def resp(reqID: Long) = tests.fakeResponse(
        document("_id" -> reqID),
        reqID.toInt,
        (reqID - 1L).toInt
      )

      var i = 0L
      val max = Int.MaxValue.toLong + 2

      implicit val actorSys = akka.actor.ActorSystem(
        "reactivemongo", defaultExecutionContext = Some(ee.executionContext)
      )

      def result = tests.foldResponses[Unit](
        _ => Future.successful(resp(i)),
        { (_, _) =>
          if (i % 1000 == 0) println(s"i = $i ? ${(i < max)}")

          if (i < max) {
            i = i + 1
            Future.successful(Some(resp(i)))
          } else Future.successful(None)
        },
        (_, _) => {},
        (),
        Int.MaxValue,
        (_, _) => Future.successful(Cursor.Cont({})),
        (_, l) => Cursor.Fail[Unit](l)
      )

      result must beEqualTo({}).await(0, Int.MaxValue.seconds)
    } tag "benchmark"
     */
  }

  "Tailable cursor" should {
    tailableSpec
  }

  // ---

  /* A selector matching all the documents, with an unique content for debug. */
  @inline def matchAll(name: String) =
    BSONDocument(name -> BSONDocument("$exists" -> false))

  trait FooCursor[T] extends Cursor[T] { def foo: String }

  class DefaultFooCursor[T](val wrappee: Cursor[T])
    extends FooCursor[T] with WrappedCursor[T] {
    val foo = "Bar"
  }

  class FlattenedFooCursor[T](cursor: Future[FooCursor[T]])
    extends reactivemongo.api.FlattenedCursor[T](cursor) with FooCursor[T] {

    val foo = "raB"
  }
}

sealed trait CursorSpecEnv { spec: CursorSpec =>
  //import Common._

  // Aliases so sub-spec can reuse the same
  @inline def driver = Common.driver
  val db = Common.db
  @inline def timeout = Common.timeout
  @inline def DefaultOptions = Common.DefaultOptions
  @inline def primaryHost = Common.primaryHost
  @inline def failoverStrategy = Common.failoverStrategy
  @inline def logger = Common.logger
  val slowDb = Common.slowDb
  @inline def slowTimeout = Common.slowTimeout
  @inline def slowFailover = Common.slowFailover
  @inline def slowPrimary = Common.slowPrimary
  @inline def SlowOptions = Common.SlowOptions

  val collName = s"cursorspec${System identityHashCode this}"
  lazy val coll = db(collName)
  lazy val slowColl = slowDb(collName)
  val info = logger.info(_: String)
  val debug = logger.debug(_: String)
}
