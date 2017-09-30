import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.{ DurationInt, FiniteDuration, MILLISECONDS }

import reactivemongo.bson.BSONDocument
import reactivemongo.core.errors.DetailedDatabaseException
import reactivemongo.api.{ Cursor, CursorFlattener, CursorProducer }
import reactivemongo.api.collections.bson.BSONCollection

import org.specs2.concurrent.{ ExecutionEnv => EE }

trait Cursor1Spec { spec: CursorSpec =>
  def group1 = {
    val nDocs = 16517
    s"insert $nDocs records" in { implicit ee: EE =>
      val futs: Seq[Future[Unit]] = for (i <- 0 until nDocs) yield {
        coll.insert(BSONDocument(
          "i" -> i, "record" -> s"record$i")).map(_ => {})
      }

      Future.sequence(futs).map { _ =>
        info(s"inserted $nDocs records")
      } aka "fixtures" must beEqualTo({}).await(1, timeout)
    }

    "make request for cursor query" in { implicit ee: EE =>
      import reactivemongo.core.protocol.{ Response, Reply }
      import reactivemongo.api.tests.{ makeRequest => req }

      def cursor(batchSize: Int = Int.MaxValue) =
        coll.find(matchAll("makeReq1")).batchSize(batchSize).cursor()

      req(cursor(), 10) must beLike[Response] {
        case Response(_, Reply(_, id, from, ret), _, _) =>
          id aka "cursor ID #1" must not(beEqualTo(0)) and {
            from must_== 0 and (ret must_== 10)
          }
      }.await(1, timeout) and {
        req(cursor(), -2) must beLike[Response] {
          case Response(_, Reply(_, id, from, ret), _, _) =>
            id aka "cursor ID #2" must_== 0 and {
              from must_== 0 and (ret must_== 16517)
            }
        }.await(1, timeout)
      } and {
        req(cursor(128), -2) must beLike[Response] {
          case Response(_, Reply(_, id, from, ret), _, _) =>
            id aka "cursor ID #2" must not(beEqualTo(0)) and {
              from must_== 0 and (ret must_== 128)
            }
        }.await(1, timeout)
      } and {
        req(cursor(), 0) must beLike[Response] {
          case Response(_, Reply(_, id, from, ret), _, _) =>
            id aka "cursor ID #4" must not(beEqualTo(0)) and {
              from must_== 0 and (ret must_== 101)
            }
        }.await(1, timeout)
      } and {
        req(cursor(), 1) must beLike[Response] {
          case Response(_, Reply(_, id, from, ret), _, _) =>
            id aka "cursor ID #5" must_== 0 and {
              from must_== 0 and (ret must_== 1)
            }
        }.await(1, timeout)
      }
    }

    { // headOption
      def headOptionSpec(c: BSONCollection, timeout: FiniteDuration) = {
        "find first document when matching" in { implicit ee: EE =>
          c.find(matchAll("headOption1")).cursor().
            headOption must beSome[BSONDocument].await(1, timeout)
        }

        "find first document when not matching" in { implicit ee: EE =>
          c.find(BSONDocument("i" -> -1)).cursor().
            headOption must beNone.await(1, timeout)
        }
      }

      "with the default connection" >> {
        headOptionSpec(coll, timeout)
      }

      "with the default connection" >> {
        headOptionSpec(slowColl, slowTimeout)
      }
    }

    // head
    "find first document when matching" in { implicit ee: EE =>
      coll.find(matchAll("head1") ++ ("i" -> 0)).cursor[BSONDocument]().head.
        map(_ -- "_id") must beEqualTo(BSONDocument(
          "i" -> 0, "record" -> "record0")).await(1, timeout)
    }

    "find first document when not matching" in { implicit ee: EE =>
      Await.result(
        coll.find(BSONDocument("i" -> -1)).cursor().head,
        timeout) must throwA[Cursor.NoSuchResultException.type]
    }

    "read one option document with success" in { implicit ee: EE =>
      coll.find(matchAll("one1")).one[BSONDocument].
        aka("findOne") must beSome[BSONDocument].await(0, timeout)
    }

    "read one document with success" in { implicit ee: EE =>
      coll.find(matchAll("one2") ++ ("i" -> 1)).requireOne[BSONDocument].
        map(_ -- "_id") must beEqualTo(BSONDocument(
          "i" -> 1, "record" -> "record1")).await(0, timeout)
    }

    def foldSpec1(c: BSONCollection, timeout: FiniteDuration) = {
      "get 10 first docs" in { implicit ee: EE =>
        c.find(matchAll("cursorspec1")).cursor().collect[List](10).
          map(_.size) aka "result size" must beEqualTo(10).await(1, timeout)
      }

      { // .fold
        "fold all the documents" in { implicit ee: EE =>
          c.find(matchAll("cursorspec2")).cursor().fold(0)(
            { (st, _) => debug(s"fold: $st"); st + 1 }) aka "result size" must beEqualTo(16517).await(1, timeout) and {
              c.find(matchAll("cursorspec2")).cursor().fold(0, -1)(
                { (st, _) => st + 1 }) aka "result size" must beEqualTo(16517).await(1, timeout)
            }
        }

        "fold only 1024 documents" in { implicit ee: EE =>
          c.find(matchAll("cursorspec3")).cursor().
            fold(0, 1024)((st, _) => st + 1).
            aka("result size") must beEqualTo(1024).await(1, timeout)
        }
      }

      { // .foldWhile
        "fold while all the documents" in { implicit ee: EE =>
          c.find(matchAll("cursorspec4a")).cursor().foldWhile(0)(
            { (st, _) => debug(s"foldWhile: $st"); Cursor.Cont(st + 1) }).
            aka("result size") must beEqualTo(16517).await(1, timeout)
        }

        "fold while only 1024 documents" in { implicit ee: EE =>
          c.find(matchAll("cursorspec5a")).cursor().foldWhile(0, 1024)(
            (st, _) => Cursor.Cont(st + 1)).
            aka("result size") must beEqualTo(1024).await(1, timeout)
        }

        "fold while successfully with async function" >> {
          "all the documents" in { implicit ee: EE =>
            coll.find(matchAll("cursorspec4b")).cursor().foldWhileM(0)(
              (st, _) => Future(Cursor.Cont(st + 1))).
              aka("result size") must beEqualTo(16517).await(1, timeout)
          }

          "only 1024 documents" in { implicit ee: EE =>
            coll.find(matchAll("cursorspec5b")).cursor().foldWhileM(0, 1024)(
              (st, _) => Future.successful(Cursor.Cont(st + 1))).
              aka("result size") must beEqualTo(1024).await(1, timeout)
          }
        }
      }

      { // .foldBulk
        "fold the bulks for all the documents" in { implicit ee: EE =>
          c.find(matchAll("cursorspec6a")).cursor().foldBulks(0)({ (st, bulk) =>
            debug(s"foldBulk: $st")
            Cursor.Cont(st + bulk.size)
          }) aka "result size" must beEqualTo(16517).await(1, timeout)
        }

        "fold the bulks for 1024 documents" in { implicit ee: EE =>
          c.find(matchAll("cursorspec7a")).cursor().foldBulks(0, 1024)(
            (st, bulk) => Cursor.Cont(st + bulk.size)).
            aka("result size") must beEqualTo(1024).await(1, timeout)
        }

        "fold the bulks with async function" >> {
          "for all the documents" in { implicit ee: EE =>
            coll.find(matchAll("cursorspec6b")).cursor().foldBulksM(0)(
              (st, bulk) => Future(Cursor.Cont(st + bulk.size))).
              aka("result size") must beEqualTo(16517).await(1, timeout)
          }

          "for 1024 documents" in { implicit ee: EE =>
            coll.find(matchAll("cursorspec7b")).cursor().foldBulksM(0, 1024)(
              (st, bulk) => Future.successful(Cursor.Cont(st + bulk.size))).
              aka("result size") must beEqualTo(1024).await(1, timeout)
          }
        }
      }

      { // .foldResponse
        "fold the responses for all the documents" in { implicit ee: EE =>
          c.find(matchAll("cursorspec8a")).cursor().foldResponses(0)(
            { (st, resp) =>
              debug(s"foldResponses: $st")
              Cursor.Cont(st + resp.reply.numberReturned)
            }) aka "result size" must beEqualTo(16517).await(1, timeout)
        }

        "fold the responses for 1024 documents" in { implicit ee: EE =>
          c.find(matchAll("cursorspec9a")).cursor().foldResponses(0, 1024)(
            (st, resp) => Cursor.Cont(st + resp.reply.numberReturned)).
            aka("result size") must beEqualTo(1024).await(1, timeout)
        }

        "fold the responses with async function" >> {
          "for all the documents" in { implicit ee: EE =>
            coll.find(matchAll("cursorspec8b")).cursor().foldResponsesM(0)(
              (st, resp) => Future(Cursor.Cont(st + resp.reply.numberReturned))).
              aka("result size") must beEqualTo(16517).await(1, timeout)
          }

          "for 1024 documents" in { implicit ee: EE =>
            coll.find(matchAll("cursorspec9b")).cursor().
              foldResponsesM(0, 1024)(
                (st, resp) => Future(
                  Cursor.Cont(st + resp.reply.numberReturned))).
                aka("result size") must beEqualTo(1024).await(1, timeout)
          }
        }
      }
    }

    "with the default connection" >> {
      foldSpec1(coll, timeout)
    }

    "with the slow connection" >> {
      foldSpec1(slowColl, slowTimeout)
    }

    "fold the responses with async function" >> {
      "for all the documents" in { implicit ee: EE =>
        coll.find(matchAll("cursorspec8")).cursor().foldResponsesM(0)(
          (st, resp) => Future(Cursor.Cont(st + resp.reply.numberReturned))).
          aka("result size") must beEqualTo(16517).await(1, timeout)
      }

      "for 1024 documents" in { implicit ee: EE =>
        coll.find(matchAll("cursorspec9")).cursor().foldResponsesM(0, 1024)(
          (st, resp) => Future(Cursor.Cont(st + resp.reply.numberReturned))).
          aka("result size") must beEqualTo(1024).await(1, timeout)
      }
    }

    "produce a custom cursor for the results" in { implicit ee: EE =>
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

    "throw exception when maxTimeout reached" in { implicit ee: EE =>
      def delayedTimeout = FiniteDuration(
        (slowTimeout.toMillis * 1.25D).toLong, MILLISECONDS)

      def futs: Seq[Future[Unit]] = for (i <- 0 until 16517)
        yield slowColl.insert(BSONDocument(
        "i" -> i, "record" -> s"record$i")).
        map(_ => debug(s"fixture #$i inserted (${slowColl.name})"))

      Future.sequence(futs).map(_ => {}).
        aka("fixtures") must beEqualTo({}).await(1, delayedTimeout) and {
          slowColl.find(BSONDocument("record" -> "asd")).
            batchSize(4096).maxTimeMs(1).cursor().
            collect[List](8192) must throwA[DetailedDatabaseException].
            await(1, slowTimeout + DurationInt(1).seconds)
        }
    }
  }
}
