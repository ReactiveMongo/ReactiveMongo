import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

import reactivemongo.api.Cursor

import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.BSONCollection

import org.specs2.concurrent.ExecutionEnv

import _root_.tests.Common
import reactivemongo.api.TestCompat._

final class CursorSpec(implicit val ee: ExecutionEnv)
  extends org.specs2.mutable.Specification with CursorSpecEnv
  with Cursor1Spec with TailableCursorSpec {

  "Cursor" title

  sequential
  stopOnFail

  "ReactiveMongo" should {
    group1 // include fixture insert

    "stop on error" >> {
      lazy val cursorDrv = Common.newAsyncDriver()
      lazy val cursorCon = Await.result(
        cursorDrv.connect(List(primaryHost), DefaultOptions),
        timeout)

      lazy val slowCursorCon = Await.result(
        cursorDrv.connect(List(slowPrimary), SlowOptions),
        slowTimeout)

      lazy val (cursorDb, slowCursorDb) =
        Common.databases(Common.commonDb, cursorCon, slowCursorCon)

      @inline def defaultColl = cursorDb(collName)
      @inline def slowDefaultColl = slowCursorDb(collName)

      "when folding bulks" >> {
        "if fails while processing with existing documents" in {
          @volatile var count = 0
          val onError: (Unit, Throwable) => Unit = { (_, _) =>
            debug(s"stopOnError: foldBulks (#1): $count")
            count = count + 1
          }
          val c = defaultColl
          val cursor = c.find(matchAll("cursorspec18")).batchSize(64).cursor()

          cursor.foldBulksM({}, 128)(
            { (_, _) => Future[Cursor.State[Unit]](sys.error("Foo #18")) },
            Cursor.FailOnError[Unit](onError)).
            map(_ => -1).recover {
              case _ => count
            } must beTypedEqualTo(1).await(1, timeout)
        }

        "if fails while processing w/o documents" in {
          @volatile var count = 0
          val onError: (Unit, Throwable) => Unit = { (_, _) =>
            debug(s"stopOnError: foldBulks (#2): $count")
            count = count + 1
          }
          val c = cursorDb(System.identityHashCode(onError).toString)
          val cursor = c.find(matchAll("cursorspec19")).batchSize(64).cursor()

          cursor.foldBulks({}, 128)(
            { (_, _) => sys.error("Foo #19"): Cursor.State[Unit] },
            Cursor.FailOnError[Unit](onError)).
            map(_ => -1).recover({ case _ => count }).
            aka("folding") must beTypedEqualTo(1).await(1, timeout)
        }

        "if fails with initial value" in {
          @volatile var count = 0
          val onError: (Unit, Throwable) => Unit = { (_, _) =>
            debug(s"stopOnError: foldBulks (#3): $count")
            count = count + 1
          }
          val c = cursorDb(System.identityHashCode(onError).toString)
          val cursor = c.find(matchAll("cursorspec20")).batchSize(64).cursor()

          cursor.foldBulksM[Unit](sys.error("Foo #20"), 128)(
            (_, _) => Future.successful(Cursor.Cont({})),
            Cursor.FailOnError[Unit](onError)).map(_ => -1).recover {
              case _ => count
            } must beTypedEqualTo(0).await(1, timeout)
        }

        "if fails to send request" in {
          @volatile var count = 0
          val onError: (Unit, Throwable) => Unit = { (_, _) =>
            debug(s"stopOnError: foldBulks (#4): $count")
            count = count + 1
          }
          val con21 = driver.connect(
            List(primaryHost), DefaultOptions.copy(nbChannelsPerNode = 1))

          val db21 = Await.result(
            con21.flatMap(_.database("dbspec21")), timeout)

          lazy val c = db21(collName)
          lazy val cursor = c.find(matchAll("cursorspec21")).
            batchSize(64).cursor()

          // Close connection to make the related cursor erroneous
          con21.flatMap(_.close()(timeout)).
            map(_ => {}) must beTypedEqualTo({}).await(1, timeout) and {
              cursor.foldBulks({}, 128)(
                { (_, _) => Cursor.Cont({}) },
                Cursor.FailOnError[Unit](onError)).map(_ => -1).recover {
                  case _ => count
                } must beTypedEqualTo(1).await(1, timeout)
            }
        }
      }

      { // .foldWhile
        def foldWhileSpec(defaultColl: DefaultCollection, timeout: FiniteDuration) = {
          "if fails while processing with existing documents" in {
            @volatile var count = 0
            val onError: (Unit, Throwable) => Unit = { (_, _) =>
              debug(s"stopOnError: foldWhile (#1): $count")
              count = count + 1
            }
            val c = defaultColl
            val cursor = c.find(matchAll("cursorspec25")).cursor()
            // default batch size 101

            cursor.foldWhileM({}, 128)(
              { (_, _) => Future[Cursor.State[Unit]](sys.error("Foo #25")) },
              Cursor.FailOnError[Unit](onError)).
              map(_ => -1).recover {
                case _ => count
              } must beTypedEqualTo(1).await(1, timeout)
          }

          "if fails with initial value" in {
            @volatile var count = 0
            val onError: (Unit, Throwable) => Unit = { (_, _) =>
              debug(s"stopOnError: foldWhile (#2): $count")
              count = count + 1
            }
            val c = defaultColl
            val cursor = c.find(matchAll("cursorspec26")).cursor()

            cursor.foldWhile[Unit](sys.error("Foo #26"), 128)(
              (_, _) => Cursor.Cont({}), Cursor.FailOnError[Unit](onError)).
              map(_ => -1).recover {
                case _ => count
              } must beTypedEqualTo(0).await(1, timeout)
          }

          "if fails to send request" in {
            @volatile var count = 0
            val onError: (Unit, Throwable) => Unit = { (_, _) =>
              debug(s"stopOnError: foldWhile (#3): $count")
              count = count + 1
            }
            val con27 = driver.connect(
              List(primaryHost), DefaultOptions.copy(nbChannelsPerNode = 1))

            val db27 = Await.result(
              con27.flatMap(_.database("dbspec27")), timeout)

            lazy val c = db27(collName)
            val cursor = c.find(matchAll("cursorspec27")).cursor()

            // Close connection to make the related cursor erroneous
            con27.flatMap(_.close()(timeout)).
              map(_ => {}) must beTypedEqualTo({}).await(1, timeout) and {
                cursor.foldWhile({}, 128)(
                  (_, _) => Cursor.Cont({}),
                  Cursor.FailOnError[Unit](onError)).map(_ => -1).recover {
                    case _ => count
                  } must beTypedEqualTo(1).await(1, timeout)
              }
          }
        }

        "when folding documents with the default connection" >> {
          foldWhileSpec(defaultColl, timeout)
        }

        "when folding documents with the slow connection" >> {
          foldWhileSpec(slowDefaultColl, slowTimeout)
        }
      }

      "Driver instance must be closed" in {
        cursorDrv.close(timeout) must not(throwA[Exception]).
          eventually(1, timeout)
      }
    }

    "continue on error" >> {
      lazy val cursorDrv = Common.newAsyncDriver()
      lazy val cursorCon = Await.result(
        cursorDrv.connect(List(primaryHost), DefaultOptions), timeout)

      lazy val slowCursorCon = Await.result(
        cursorDrv.connect(List(slowPrimary), SlowOptions), slowTimeout)

      val (cursorDb, slowCursorDb) =
        Common.databases(Common.commonDb, cursorCon, slowCursorCon,
          retries = 1)

      @inline def defaultColl = cursorDb(collName)

      "when folding bulks" >> {
        lazy val delayedTimeout = FiniteDuration(
          (timeout.toMillis * 1.25D).toLong, MILLISECONDS)

        "if fails while processing with existing documents" in {
          @volatile var count = 0

          val onError: (Unit, Throwable) => Unit = { (_, _) =>
            debug(s"continueOnError: foldBulks (#1): $count")
            count = count + 1
          }
          val cursor = defaultColl.find(matchAll("cursorspec37")).
            batchSize(64).cursor()

          cursor.foldBulks({}, 128)(
            { (_, _) => sys.error("Foo #37"): Cursor.State[Unit] },
            Cursor.ContOnError[Unit](onError)).map(_ => count).
            aka("folding") must beTypedEqualTo(2 /* maxDocs / batchSize */ ).
            await(2, delayedTimeout)
        }

        "if fails while processing w/o documents" in {
          @volatile var count = 0
          val onError: (Unit, Throwable) => Unit = { (_, _) =>
            debug(s"continueOnError: foldBulks (#2): $count")
            count = count + 1
          }
          val c = cursorDb(s"emptycoll_${System identityHashCode onError}")
          val cursor = c.find(matchAll("cursorspec38")).batchSize(64).cursor()

          cursor.foldBulksM({}, 64)(
            { (_, _) => Future[Cursor.State[Unit]](sys.error("Foo #38")) },
            Cursor.ContOnError[Unit](onError)).map(_ => count).
            aka("folding") must beTypedEqualTo(1 /* 64 / batchSize */ ).
            await(2, delayedTimeout)

        }

        "if fails with initial value" in {
          @volatile var count = 0
          val onError: (Unit, Throwable) => Unit = { (_, _) =>
            debug(s"continueOnError: foldBulks (#3): $count")
            count = count + 1
          }
          val c = cursorDb(System.identityHashCode(onError).toString)
          val cursor = c.find(matchAll("cursorspec39")).batchSize(64).cursor()

          cursor.foldBulks[Unit](sys.error("Foo #39"), 128)(
            (_, _) => Cursor.Cont({}), Cursor.ContOnError[Unit](onError)).
            map(_ => -1).recover({ case _ => count }) must beTypedEqualTo(0).
            await(2, delayedTimeout)
        }

        "if fails to send request" in {
          @volatile var count = 0
          val onError: (Unit, Throwable) => Unit = { (_, _) =>
            debug(s"continueOnError: foldBulks (#4): $count")
            count = count + 1
          }
          val con40 = driver.connect(
            List(primaryHost), DefaultOptions.copy(nbChannelsPerNode = 1))

          lazy val db40 = Await.result(
            con40.flatMap(_.database("dbspec40")), timeout)

          lazy val c = db40(collName)
          val cursor = c.find(matchAll("cursorspec40")).batchSize(64).cursor()

          // Close connection to make the related cursor erroneous
          con40.flatMap(_.close()(timeout)).
            map(_ => {}) must beTypedEqualTo({}).await(1, timeout) and {
              cursor.foldBulks({}, 128)(
                { (_, _) => Cursor.Cont({}) },
                Cursor.ContOnError[Unit](onError)).
                map(_ => count) must beTypedEqualTo(1).await(2, delayedTimeout)
            }
        }
      }

      { // .foldWhile
        def foldWhileSpec(defaultColl: DefaultCollection, timeout: FiniteDuration) = {
          def delayedTimeout = FiniteDuration(
            (timeout.toMillis * 1.25D).toLong, MILLISECONDS)

          "if fails while processing with existing documents" in {
            @volatile var count = 0
            val onError: (Unit, Throwable) => Unit = { (_, _) =>
              debug(s"continueOnError: foldWhileM(#1): $count")
              count = count + 1
            }
            val cursor = defaultColl.find(matchAll("cursorspec44")).cursor()

            cursor.foldWhileM({}, 128)(
              { (_, _) => Future[Cursor.State[Unit]](sys.error("Foo #44")) },
              Cursor.ContOnError[Unit](onError)).map(_ => count).
              aka("folding") must beTypedEqualTo(128).await(2, delayedTimeout)

          }

          "if fails with initial value" in {
            @volatile var count = 0
            val onError: (Unit, Throwable) => Unit = { (_, _) =>
              debug(s"continueOnError: foldWhile(#2): $count")
              count = count + 1
            }
            val c = defaultColl
            val cursor = c.find(matchAll("cursorspec45")).cursor()

            cursor.foldWhile[Unit](sys.error("Foo #45"), 128)(
              (_, _) => Cursor.Cont({}), Cursor.ContOnError[Unit](onError)).
              map(_ => -1).recover { case _ => count } must beTypedEqualTo(0).
              await(2, delayedTimeout)
          }

          "if fails to send request" in {
            @volatile var count = 0
            val onError: (Unit, Throwable) => Unit = { (_, _) =>
              debug(s"continueOnError: foldWhile(#3): $count")
              count = count + 1
            }
            val con46 = driver.connect(
              List(primaryHost), DefaultOptions.copy(nbChannelsPerNode = 1))

            lazy val db46 = Await.result(
              con46.flatMap(_.database("dbspec46")), timeout)

            lazy val c = db46(collName)
            val cursor = c.find(matchAll("cursorspec46")).cursor()

            // Close connection to make the related cursor erroneous
            con46.flatMap(_.close()(timeout)).
              map(_ => {}) must beTypedEqualTo({}).await(1, timeout) and {
                cursor.foldWhileM({}, 64)(
                  (_, _) => Future.successful(Cursor.Cont({})),
                  Cursor.ContOnError[Unit](onError)).map(_ => count).
                  aka("folding") must beTypedEqualTo(1).await(1, timeout)

              }
          }
        }

        "when folding documents with the default connection" >> {
          foldWhileSpec(defaultColl, timeout)
        }

        "when folding documents with the slow connection" >> {
          foldWhileSpec(slowCursorDb(collName), slowTimeout)
        }
      }

      "Driver instance must be closed" in {
        cursorDrv.close(timeout) must not(throwA[Exception])
      }
    }

    /*
    "Benchmark" in {
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
    BSONDocument(name -> BSONDocument(f"$$exists" -> false))

}

sealed trait CursorSpecEnv { spec: CursorSpec =>
  //import Common._

  // Aliases so sub-spec can reuse the same
  @inline def driver = Common.driver
  @inline def db = Common.db
  @inline def timeout = Common.timeout
  @inline def DefaultOptions = Common.DefaultOptions
  @inline def primaryHost = Common.primaryHost
  @inline def failoverStrategy = Common.failoverStrategy
  @inline def logger = Common.logger
  @inline def slowDb = Common.slowDb
  @inline def slowTimeout = Common.slowTimeout
  @inline def slowFailover = Common.slowFailover
  @inline def slowPrimary = Common.slowPrimary
  @inline def SlowOptions = Common.SlowOptions

  val collName = s"cursorspec${System identityHashCode this}"
  lazy val coll: BSONCollection = db(collName)
  lazy val slowColl: BSONCollection = slowDb(collName)
  val info = logger.info(_: String)
  val debug = logger.debug(_: String)
}
