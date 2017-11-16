import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration._

import reactivemongo.bson.{ BSONDocument, BSONDocumentReader }
import reactivemongo.core.protocol.Response
import reactivemongo.api.{ Cursor, DB, QueryOpts }

trait TailableCursorSpec { specs: CursorSpec =>
  def tailableSpec = {
    object IdReader extends BSONDocumentReader[Int] {
      def read(doc: BSONDocument): Int = doc.getAs[Int]("id").get
    }

    "read from capped collection" >> {
      def collection(n: String, database: DB) = {
        val col = database(s"somecollection_captail_$n")

        col.createCapped(4096, Some(10)).flatMap { _ =>
          val sched = Common.driver.system.scheduler

          (0 until 10).foldLeft(Future successful {}) { (f, id) =>
            f.flatMap(_ => col.insert(BSONDocument("id" -> id)).flatMap { _ =>
              val pause = Promise[Unit]()

              sched.scheduleOnce(200.milliseconds) {
                pause.trySuccess({})
                ()
              }

              pause.future
            })
          }.map(_ => info(s"All fixtures inserted in test collection '$n'"))
        }

        col
      }

      @inline def tailable(n: String, database: DB = db) = {
        implicit val reader = IdReader
        collection(n, database).find(matchAll("cursorspec50")).options(
          QueryOpts().tailable).batchSize(512).cursor[Int]()
      }

      "using tailable" >> {
        "to fold responses" in {
          implicit val reader = IdReader
          tailable("foldr0").foldResponses(List.empty[Int], 6) { (s, resp) =>
            val bulk = Response.parse(resp).flatMap(_.asOpt[Int].toList)

            Cursor.Cont(s ++ bulk)
          } must beEqualTo(List(0, 1, 2, 3, 4, 5)).await(1, timeout)
        }

        "to fold responses with async function" in {
          implicit val reader = IdReader
          tailable("foldr0").foldResponsesM(List.empty[Int], 6) { (s, resp) =>
            val bulk = Response.parse(resp).flatMap(_.asOpt[Int].toList)

            Future.successful(Cursor.Cont(s ++ bulk))
          } must beEqualTo(List(0, 1, 2, 3, 4, 5)).await(1, timeout)
        }

        "to fold bulks" in {
          tailable("foldw0a").foldBulks(List.empty[Int], 6)(
            (s, bulk) => Cursor.Cont(s ++ bulk)) must beEqualTo(List(
              0, 1, 2, 3, 4, 5)).await(1, timeout)
        }

        "to fold bulks with async function" in {
          tailable("foldw0b").foldBulksM(List.empty[Int], 6)((s, bulk) =>
            Future.successful(Cursor.Cont(s ++ bulk))) must beEqualTo(List(
            0, 1, 2, 3, 4, 5)).await(1, timeout)
        }
      }

      "using tailable foldWhile" >> {
        "successfully" in {
          tailable("foldw1a").foldWhile(List.empty[Int], 5)(
            (s, i) => Cursor.Cont(i :: s)) must beEqualTo(List(
              4, 3, 2, 1, 0)).await(1, timeout)
        }

        "successfully with async function" in {
          tailable("foldw1b").foldWhileM(List.empty[Int], 5)((s, i) =>
            Future.successful(Cursor.Cont(i :: s))) must beEqualTo(List(
            4, 3, 2, 1, 0)).await(1, timeout)
        }

        "leading to timeout w/o maxDocs" in {
          Await.result(tailable("foldw2").foldWhile(List.empty[Int])(
            (s, i) => Cursor.Cont(i :: s),
            (_, e) => Cursor.Fail(e)), timeout) must throwA[Exception]
        }

        "gracefully stop at connection close w/o maxDocs" in {
          val con = Common.makeConnection(List(primaryHost), DefaultOptions)
          lazy val delayedTimeout = FiniteDuration(
            (timeout.toMillis * 1.25D).toLong, MILLISECONDS)

          con.database(
            "specs2-test-reactivemongo", failoverStrategy).flatMap { d =>
              tailable("foldw3", d).foldWhile(List.empty[Int])((s, i) => {
                if (i == 1) { // Force connection close
                  Await.result(con.askClose()(timeout), timeout)
                }

                Cursor.Cont(i :: s)
              }, (_, e) => Cursor.Fail(e))
            } must beLike[List[Int]] {
              case is => is.reverse must beLike[List[Int]] {
                case 0 :: 1 :: _ => ok
              }
            }.await(2, delayedTimeout)
        }
      }
    }
  }
}
