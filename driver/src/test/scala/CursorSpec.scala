import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.DurationInt

import play.api.libs.iteratee.Iteratee

import reactivemongo.bson._
import reactivemongo.core.protocol.Response
import reactivemongo.core.errors.DetailedDatabaseException
import reactivemongo.api.{
  Cursor,
  CursorFlattener,
  CursorProducer,
  DB,
  QueryOpts,
  MongoDriver,
  WrappedCursor
}

object CursorSpec extends org.specs2.mutable.Specification {
  "Cursor" title

  sequential

  import Common._

  val coll = db("cursorspec")
  val delayedTimeout = (timeoutMillis * 1.25).toInt

  "ReactiveMongo" should {
    val nDocs = 16517
    s"insert $nDocs records" in {
      val futs = for (i <- 0 until nDocs) yield {
        coll.insert(BSONDocument(
          "i" -> BSONInteger(i), "record" -> BSONString("record" + i)))
      }

      Future.sequence(futs).map { _ =>
        println(s"inserted $nDocs records")
      } aka "fixtures" must beEqualTo({}).await(timeoutMillis)
    }

    "get 10 first docs" in {
      coll.find(matchAll("cursorspec1")).cursor().collect[List](10).map(_.size).
        aka("result size") must beEqualTo(10).await(timeoutMillis)
    }

    "fold" >> {
      "all the documents" in {
        coll.find(matchAll("cursorspec2")).cursor().fold(0)((st, _) => st + 1).
          aka("result size") must beEqualTo(16517).await(timeoutMillis)
      }

      "only 1024 documents" in {
        coll.find(matchAll("cursorspec3")).cursor().
          fold(0, 1024)((st, _) => st + 1).
          aka("result size") must beEqualTo(1024).await(timeoutMillis)
      }
    }

    "fold while successful" >> {
      "all the documents" in {
        coll.find(matchAll("cursorspec4")).cursor().foldWhile(0)(
          (st, _) => Cursor.Cont(st + 1)).
          aka("result size") must beEqualTo(16517).await(timeoutMillis)
      }

      "only 1024 documents" in {
        coll.find(matchAll("cursorspec5")).cursor().foldWhile(0, 1024)(
          (st, _) => Cursor.Cont(st + 1)).
          aka("result size") must beEqualTo(1024).await(timeoutMillis)
      }
    }

    "fold the bulks" >> {
      "for all the documents" in {
        coll.find(matchAll("cursorspec6")).cursor().foldBulks(0)(
          (st, bulk) => Cursor.Cont(st + bulk.size)).
          aka("result size") must beEqualTo(16517).await(timeoutMillis)
      }

      "for 1024 documents" in {
        coll.find(matchAll("cursorspec7")).cursor().foldBulks(0, 1024)(
          (st, bulk) => Cursor.Cont(st + bulk.size)).
          aka("result size") must beEqualTo(1024).await(timeoutMillis)
      }
    }

    "fold the responses" >> {
      "for all the documents" in {
        coll.find(matchAll("cursorspec8")).cursor().foldResponses(0)(
          (st, resp) => Cursor.Cont(st + resp.reply.numberReturned)).
          aka("result size") must beEqualTo(16517).await(timeoutMillis)
      }

      "for 1024 documents" in {
        coll.find(matchAll("cursorspec9")).cursor().foldResponses(0, 1024)(
          (st, resp) => Cursor.Cont(st + resp.reply.numberReturned)).
          aka("result size") must beEqualTo(1024).await(timeoutMillis)
      }
    }

    "produce a custom cursor for the results" in {
      implicit def fooProducer[T] = new CursorProducer[T] {
        type ProducedCursor = FooCursor[T]
        def produce(base: Cursor[T]) = new DefaultFooCursor(base)
      }

      implicit object fooFlattener extends CursorFlattener[FooCursor] {
        def flatten[T](future: Future[FooCursor[T]]) =
          new FlattenedFooCursor(future)
      }

      val cursor = coll.find(matchAll("cursorspec10")).cursor()

      cursor.foo must_== "Bar" and (
        Cursor.flatten(Future.successful(cursor)).foo must_== "raB")
    }

    "throw exception when maxTimeout reached" in {
      val futs = for (i <- 0 until 16517)
        yield coll.insert(BSONDocument(
        "i" -> BSONInteger(i), "record" -> BSONString("record" + i)))

      val fut = Future.sequence(futs)
      Await.result(fut, timeout * 1.25D)

      coll.find(BSONDocument("record" -> "asd")).maxTimeMs(1).cursor().
        collect[List](10) must throwA[DetailedDatabaseException].
        await(timeout = timeout + DurationInt(1).seconds)
    }

    "stop on error" >> {
      val drv = new MongoDriver
      def con = drv.connection(List(primaryHost), DefaultOptions)
      def scol(n: String = coll.name) = {
        //println(s"resolve collection $n")
        Await.result(for {
          d <- con.database(db.name, failoverStrategy)
          c <- d.coll(n)
        } yield c, timeout)
      }

      "when folding responses" >> {
        "if fails while processing with existing documents" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec11")).cursor()

          cursor.foldResponses({}, 128)(
            { (_, _) => sys.error("Foo"): Cursor.State[Unit] },
            Cursor.FailOnError[Unit](onError)).recover({ case _ => count }).
            aka("folding") must beEqualTo(1).await(timeoutMillis)
        }

        "if fails while processing w/o documents" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol(System.identityHashCode(onError _).toString)
          val cursor = c.find(matchAll("cursorspec12")).cursor()

          cursor.foldResponses({}, 128)(
            { (_, _) => sys.error("Foo"): Cursor.State[Unit] },
            Cursor.FailOnError[Unit](onError)).recover({ case _ => count }).
            aka("folding") must beEqualTo(1).await(timeoutMillis)
        }

        "if fails with initial value" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol(System.identityHashCode(onError _).toString)
          val cursor = c.find(matchAll("cursorspec13")).cursor()

          cursor.foldResponses[Unit](sys.error("Foo"), 128)(
            (_, _) => Cursor.Cont({}), Cursor.FailOnError[Unit](onError)).
            recover({ case _ => count }) must beEqualTo(0).await(timeoutMillis)
        }

        "if fails to send request" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec14")).cursor()

          c.db.connection.close()
          // Close connection to make the related cursor erroneous

          cursor.foldResponses({}, 128)((_, _) => Cursor.Cont({}),
            Cursor.FailOnError[Unit](onError)).recover({ case _ => count }).
            aka("folding") must beEqualTo(1).await(timeoutMillis)
        }
      }

      "when enumerating responses" >> {
        "if fails while processing with existing documents" in {
          var count = 0
          val inc = Iteratee.foreach[Response] { _ =>
            if (count == 1) sys.error("Foo")

            count = count + 1
          }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec15")).options(
            QueryOpts(batchSizeN = 2)).cursor()

          (cursor.enumerateResponses(10, true) |>>> inc).
            recover({ case _ => count }).
            aka("enumerating") must beEqualTo(1).await(timeoutMillis)
        }

        "if fails while processing w/o documents" in {
          var count = 0
          val inc = Iteratee.foreach[Response] { _ =>
            count = count + 1
            sys.error("Foo")
          }
          val c = scol(System.identityHashCode(inc).toString)
          val cursor = c.find(matchAll("cursorspec16")).options(
            QueryOpts(batchSizeN = 2)).cursor()

          (cursor.enumerateResponses(10, true) |>>> inc).
            recover({ case _ => count }) must beEqualTo(1).await(timeoutMillis)
        }

        "if fails to send request" in {
          var count = 0
          val inc = Iteratee.foreach[Response] { _ => count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec17")).cursor()

          c.db.connection.close()
          // Close connection to make the related cursor erroneous

          (cursor.enumerateResponses(10, true) |>>> inc).
            recover({ case _ => count }) must beEqualTo(0).await(timeoutMillis)
        }
      }

      "when folding bulks" >> {
        "if fails while processing with existing documents" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec18")).options(
            QueryOpts(batchSizeN = 64)).cursor()

          cursor.foldBulks({}, 128)(
            { (_, _) => sys.error("Foo"): Cursor.State[Unit] },
            Cursor.FailOnError[Unit](onError)).recover({ case _ => count }).
            aka("folding") must beEqualTo(1).await(timeoutMillis)
        }

        "if fails while processing w/o documents" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol(System.identityHashCode(onError _).toString)
          val cursor = c.find(matchAll("cursorspec19")).options(
            QueryOpts(batchSizeN = 64)).cursor()

          cursor.foldBulks({}, 128)(
            { (_, _) => sys.error("Foo"): Cursor.State[Unit] },
            Cursor.FailOnError[Unit](onError)).recover({ case _ => count }).
            aka("folding") must beEqualTo(1).await(timeoutMillis)
        }

        "if fails with initial value" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol(System.identityHashCode(onError _).toString)
          val cursor = c.find(matchAll("cursorspec20")).options(
            QueryOpts(batchSizeN = 64)).cursor()

          cursor.foldBulks[Unit](sys.error("Foo"), 128)(
            (_, _) => Cursor.Cont({}), Cursor.FailOnError[Unit](onError)).
            recover({ case _ => count }) must beEqualTo(0).await(timeoutMillis)
        }

        "if fails to send request" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec21")).options(
            QueryOpts(batchSizeN = 64)).cursor()

          c.db.connection.close()
          // Close connection to make the related cursor erroneous

          cursor.foldBulks({}, 128)(
            { (_, _) => Cursor.Cont({}) }, Cursor.FailOnError[Unit](onError)).
            recover({ case _ => count }) must beEqualTo(1).await(timeoutMillis)
        }
      }

      "when enumerating bulks" >> {
        "if fails while processing with existing documents" in {
          var count = 0
          val inc = Iteratee.foreach[Iterator[BSONDocument]] { _ =>
            if (count == 1) sys.error("Foo")

            count = count + 1
          }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec22")).options(
            QueryOpts(batchSizeN = 2)).cursor()

          (cursor.enumerateBulks(10, true) |>>> inc).
            recover({ case _ => count }).
            aka("enumerating") must beEqualTo(1).await(timeoutMillis)
        }

        "if fails while processing w/o documents" in {
          var count = 0
          val inc = Iteratee.foreach[Iterator[BSONDocument]] { _ =>
            count = count + 1
            sys.error("Foo")
          }
          val c = scol(System.identityHashCode(inc).toString)
          val cursor = c.find(matchAll("cursorspec23")).options(
            QueryOpts(batchSizeN = 2)).cursor()

          (cursor.enumerateBulks(10, true) |>>> inc).
            recover({ case _ => count }) must beEqualTo(1).await(timeoutMillis)
        }

        "if fails to send request" in {
          var count = 0
          val inc = Iteratee.foreach[Iterator[BSONDocument]] { _ =>
            count = count + 1
          }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec24")).cursor()

          c.db.connection.close()
          // Close connection to make the related cursor erroneous

          (cursor.enumerateBulks(10, true) |>>> inc).
            recover({ case _ => count }) must beEqualTo(0).await(timeoutMillis)
        }
      }

      "when folding documents" >> {
        "if fails while processing with existing documents" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec25")).cursor()

          cursor.foldWhile({}, 128)(
            { (_, _) => sys.error("Foo"): Cursor.State[Unit] },
            Cursor.FailOnError[Unit](onError)).recover({ case _ => count }).
            aka("folding") must beEqualTo(1).await(timeoutMillis)
        }

        "if fails with initial value" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec26")).cursor()

          cursor.foldWhile[Unit](sys.error("Foo"), 128)(
            (_, _) => Cursor.Cont({}), Cursor.FailOnError[Unit](onError)).
            recover({ case _ => count }) must beEqualTo(0).await(timeoutMillis)
        }

        "if fails to send request" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec27")).cursor()

          c.db.connection.close()
          // Close connection to make the related cursor erroneous

          cursor.foldWhile({}, 128)((_, _) => Cursor.Cont({}),
            Cursor.FailOnError[Unit](onError)).recover({ case _ => count }).
            aka("folding") must beEqualTo(1).await(timeoutMillis)
        }
      }

      "when enumerating documents" >> {
        "if fails while processing with existing documents" in {
          var count = 0
          val inc = Iteratee.foreach[BSONDocument] { _ =>
            if (count == 5) sys.error("Foo")

            count = count + 1
          }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec28")).cursor()

          (cursor.enumerate(10, true) |>>> inc).recover({ case _ => count }).
            aka("enumerating") must beEqualTo(5).await(timeoutMillis)
        }

        "if fails to send request" in {
          var count = 0
          val inc = Iteratee.foreach[BSONDocument] { _ => count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec29")).cursor()

          c.db.connection.close()
          // Close connection to make the related cursor erroneous

          (cursor.enumerate(10, true) |>>> inc).recover({ case _ => count }).
            aka("enumerating") must beEqualTo(0).await(timeoutMillis)
        }
      }

      "Driver instance must be closed" in {
        drv.close() must not(throwA[Exception])
      }
    }

    "continue on error" >> {
      val drv = new MongoDriver
      def con = drv.connection(List(primaryHost), DefaultOptions)
      def scol(n: String = coll.name) = Await.result(for {
        d <- con.database(db.name, failoverStrategy)
        c <- d.coll(n)
      } yield c, timeout)

      "when folding responses" >> {
        "if fails while processing with existing documents" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec30")).cursor()

          // retry on the initial failure - until the max (128) is reached
          cursor.foldResponses({}, 128)(
            { (_, _) => sys.error("Foo"): Cursor.State[Unit] },
            Cursor.ContOnError[Unit](onError)).map(_ => count).
            aka("folding") must beEqualTo(128).await(delayedTimeout)

        }

        "if fails while processing w/o documents" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol(System.identityHashCode(onError _).toString)
          val cursor = c.find(matchAll("cursorspec31")).cursor()

          cursor.foldResponses({}, 64)(
            { (_, _) => sys.error("Foo"): Cursor.State[Unit] },
            Cursor.ContOnError[Unit](onError)).map(_ => count).
            aka("folding") must beEqualTo(64).await(delayedTimeout)
        }

        "if fails with initial value" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol(System.identityHashCode(onError _).toString)
          val cursor = c.find(matchAll("cursorspec32")).cursor()

          cursor.foldResponses[Unit](sys.error("Foo"), 128)(
            (_, _) => Cursor.Cont({}), Cursor.ContOnError[Unit](onError)).
            recover({ case _ => count }) must beEqualTo(0).
            await(delayedTimeout)
        }

        "if fails to send request" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec33")).cursor()

          c.db.connection.close()
          // Close connection to make the related cursor erroneous

          cursor.foldResponses({}, 128)((_, _) => Cursor.Cont({}),
            Cursor.ContOnError[Unit](onError)).map(_ => count).
            aka("folding") must beEqualTo(1).
            await(delayedTimeout)
        }
      }

      "when enumerating responses" >> {
        "if fails while processing with existing documents" in {
          var count = 0
          var i = 0
          val inc = Iteratee.foreach[Response] { _ =>
            i = i + 1
            if (i % 2 == 0) sys.error("Foo")
            count = count + 1
          }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec34")).options(QueryOpts(
            batchSizeN = 4)).cursor()

          (cursor.enumerateResponses(128, false) |>>> inc).map(_ => count).
            aka("enumerating") must beEqualTo(16).
            await(delayedTimeout)
        }

        "if fails while processing w/o documents" in {
          var count = 0
          val inc = Iteratee.foreach[Response] { _ =>
            count = count + 1
            sys.error("Foo")
          }
          val c = scol(System.identityHashCode(inc).toString)
          val cursor = c.find(matchAll("cursorspec35")).
            options(QueryOpts(batchSizeN = 2)).cursor()

          (cursor.enumerateResponses(64, false) |>>> inc).map(_ => count).
            aka("enumerating") must beEqualTo(1).
            await(delayedTimeout)
        }

        "if fails to send request" in {
          var count = 0
          val inc = Iteratee.foreach[Response] { _ => count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec36")).cursor()

          c.db.connection.close()
          // Close connection to make the related cursor erroneous

          (cursor.enumerateResponses(128, false) |>>> inc).
            recover({ case _ => count }) must beEqualTo(0).await(timeoutMillis)
        }
      }

      "when folding bulks" >> {
        "if fails while processing with existing documents" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec37")).options(
            QueryOpts(batchSizeN = 64)).cursor()

          cursor.foldBulks({}, 128)(
            { (_, _) => sys.error("Foo"): Cursor.State[Unit] },
            Cursor.ContOnError[Unit](onError)).map(_ => count).
            aka("folding") must beEqualTo(128).
            await(delayedTimeout)
        }

        "if fails while processing w/o documents" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol(System.identityHashCode(onError _).toString)
          val cursor = c.find(matchAll("cursorspec38")).options(
            QueryOpts(batchSizeN = 64)).cursor()

          cursor.foldBulks({}, 64)(
            { (_, _) => sys.error("Foo"): Cursor.State[Unit] },
            Cursor.ContOnError[Unit](onError)).map(_ => count).
            aka("folding") must beEqualTo(64).
            await(delayedTimeout)
        }

        "if fails with initial value" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol(System.identityHashCode(onError _).toString)
          val cursor = c.find(matchAll("cursorspec39")).options(
            QueryOpts(batchSizeN = 64)).cursor()

          cursor.foldBulks[Unit](sys.error("Foo"), 128)(
            (_, _) => Cursor.Cont({}), Cursor.ContOnError[Unit](onError)).
            recover({ case _ => count }) must beEqualTo(0).
            await(delayedTimeout)
        }

        "if fails to send request" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec40")).options(
            QueryOpts(batchSizeN = 64)).cursor()

          c.db.connection.close()
          // Close connection to make the related cursor erroneous

          cursor.foldBulks({}, 128)(
            { (_, _) => Cursor.Cont({}) }, Cursor.ContOnError[Unit](onError)).
            map(_ => count) must beEqualTo(1).await(delayedTimeout)
        }
      }

      "when enumerating bulks" >> {
        "if fails while processing with existing documents" in {
          var count = 0
          var i = 0
          val inc = Iteratee.foreach[Iterator[BSONDocument]] { _ =>
            i = i + 1
            if (i % 2 == 0) sys.error("Foo")
            count = count + 1
          }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec41")).options(QueryOpts(
            batchSizeN = 4)).cursor()

          (cursor.enumerateBulks(128, false) |>>> inc).map(_ => count).
            aka("enumerating") must beEqualTo(16).await(delayedTimeout)
        }

        "if fails while processing w/o documents" in {
          var count = 0
          val inc = Iteratee.foreach[Iterator[BSONDocument]] { _ =>
            count = count + 1
            sys.error("Foo")
          }
          val c = scol(System.identityHashCode(inc).toString)
          val cursor = c.find(matchAll("cursorspec42")).
            options(QueryOpts(batchSizeN = 2)).cursor()

          (cursor.enumerateBulks(64, false) |>>> inc).map(_ => count).
            aka("enumerating") must beEqualTo(1).await(delayedTimeout)
        }

        "if fails to send request" in {
          var count = 0
          val inc = Iteratee.foreach[Iterator[BSONDocument]] {
            _ => count = count + 1
          }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec43")).cursor()

          c.db.connection.close()
          // Close connection to make the related cursor erroneous

          (cursor.enumerateBulks(128, false) |>>> inc).
            recover({ case _ => count }) must beEqualTo(0).await(timeoutMillis)
        }
      }

      "when folding documents" >> {
        "if fails while processing with existing documents" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec44")).cursor()

          cursor.foldWhile({}, 128)(
            { (_, _) => sys.error("Foo"): Cursor.State[Unit] },
            Cursor.ContOnError[Unit](onError)).map(_ => count).
            aka("folding") must beEqualTo(128).await(delayedTimeout)
        }

        "if fails with initial value" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec45")).cursor()

          cursor.foldWhile[Unit](sys.error("Foo"), 128)(
            (_, _) => Cursor.Cont({}), Cursor.ContOnError[Unit](onError)).
            recover({ case _ => count }) must beEqualTo(0).
            await(delayedTimeout)
        }

        "if fails to send request" in {
          var count = 0
          def onError(last: Unit, e: Throwable): Unit = { count = count + 1 }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec46")).cursor()

          c.db.connection.close()
          // Close connection to make the related cursor erroneous

          cursor.foldWhile({}, 64)((_, _) => Cursor.Cont({}),
            Cursor.ContOnError[Unit](onError)).map(_ => count).
            aka("folding") must beEqualTo(1).await(timeoutMillis)
        }
      }

      "when enumerating documents" >> {
        "if fails while processing with existing documents" in {
          var count = 0
          var i = 0
          val inc = Iteratee.foreach[BSONDocument] { _ =>
            i = i + 1
            if (i % 2 == 0) sys.error("Foo")
            count = count + 1
          }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec47")).options(QueryOpts(
            batchSizeN = 4)).cursor()

          (cursor.enumerate(128, false) |>>> inc).map(_ => count).
            aka("enumerating") must beEqualTo(32).await(timeoutMillis)
        }

        "if fails while processing w/o documents" in {
          var count = 0
          val inc = Iteratee.foreach[BSONDocument] { _ =>
            count = count + 1
            sys.error("Foo")
          }
          val c = scol(System.identityHashCode(inc).toString)
          val cursor = c.find(matchAll("cursorspec48")).
            options(QueryOpts(batchSizeN = 2)).cursor()

          (cursor.enumerate(64, false) |>>> inc).map(_ => count).
            aka("enumerating") must beEqualTo(0).await(timeoutMillis)
        }

        "if fails to send request" in {
          var count = 0
          val inc = Iteratee.foreach[BSONDocument] {
            _ => count = count + 1
          }
          val c = scol()
          val cursor = c.find(matchAll("cursorspec49")).cursor()

          c.db.connection.close()
          // Close connection to make the related cursor erroneous

          (cursor.enumerate(128, false) |>>> inc).
            recover({ case _ => count }) must beEqualTo(0).await(timeoutMillis)
        }
      }

      "Driver instance must be closed" in {
        drv.close() must not(throwA[Exception])
      }
    }
  }

  "BSON cursor" should {
    object IdReader extends BSONDocumentReader[Int] {
      def read(doc: BSONDocument): Int = doc.getAs[Int]("id").get
    }

    "read from capped collection" >> {
      def collection(n: String, database: DB) = {
        val col = database(s"somecollection_captail_$n")

        col.createCapped(4096, Some(10)).flatMap { _ =>
          (0 until 10).foldLeft(Future successful {}) { (f, id) =>
            f.flatMap(_ => col.insert(BSONDocument("id" -> id)).map(_ =>
              Thread.sleep(200)))
          }.map(_ =>
            println(s"-- all documents inserted in test collection $n"))
        }

        col
      }

      @inline def tailable(n: String, database: DB = db) = {
        implicit val reader = IdReader
        collection(n, database).find(matchAll("cursorspec50")).options(
          QueryOpts().tailable).cursor[Int]()
      }

      "using tailable" >> {
        "to fold responses" in {
          implicit val reader = IdReader
          tailable("foldr0").foldResponses(List.empty[Int], 6) { (s, resp) =>
            val bulk = Response.parse(resp).flatMap(_.asOpt[Int].toList)

            Cursor.Cont(s ++ bulk)
          } must beEqualTo(List(0, 1, 2, 3, 4, 5)).await(timeoutMillis)
        }

        "to fold bulks" in {
          tailable("foldw0").foldBulks(List.empty[Int], 6)(
            (s, bulk) => Cursor.Cont(s ++ bulk)) must beEqualTo(List(
              0, 1, 2, 3, 4, 5)).await(timeoutMillis)
        }
      }

      "using tailable foldWhile" >> {
        "successfully" in {
          tailable("foldw1").foldWhile(List.empty[Int], 5)(
            (s, i) => Cursor.Cont(i :: s)) must beEqualTo(List(
              4, 3, 2, 1, 0)).await(timeoutMillis)
        }

        "leading to timeout w/o maxDocs" in {
          Await.result(tailable("foldw2").foldWhile(List.empty[Int])(
            (s, i) => Cursor.Cont(i :: s),
            (_, e) => Cursor.Fail(e)), timeout) must throwA[Exception]

        }

        "gracefully stop at connection close w/o maxDocs" in {
          val con = driver.connection(List(primaryHost), DefaultOptions)

          con.database(
            "specs2-test-reactivemongo", failoverStrategy).flatMap { d =>
              tailable("foldw3", d).foldWhile(List.empty[Int])((s, i) => {
                if (i == 1) con.close() // Force connection close
                Cursor.Cont(i :: s)
              }, (_, e) => Cursor.Fail(e))
            } must beLike[List[Int]] {
              case is => is.reverse must beLike[List[Int]] {
                case 0 :: 1 :: _ => ok
              }
            }.await(delayedTimeout)
        }
      }
    }
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
