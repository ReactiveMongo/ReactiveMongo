import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.{ FiniteDuration, MILLISECONDS }

import reactivemongo.bson.{ BSONDocument, BSONDocumentReader }
import reactivemongo.core.protocol.Response
import reactivemongo.api.{ Cursor, DB, QueryOpts }

import org.specs2.concurrent.{ ExecutionEnv => EE }

trait TailableCursorSpec { specs: CursorSpec =>
  def tailableSpec = {
    object IdReader extends BSONDocumentReader[Int] {
      def read(doc: BSONDocument): Int = doc.getAs[Int]("id").get
    }

    "read from capped collection" >> {
      def collection(n: String, database: DB)(implicit ec: ExecutionContext) = {
        val col = database(s"somecollection_captail_$n")

        col.createCapped(4096, Some(10)).flatMap { _ =>
          (0 until 10).foldLeft(Future successful {}) { (f, id) =>
            f.flatMap(_ => col.insert(BSONDocument("id" -> id)).map(_ =>
              Thread.sleep(200)))
          }.map(_ => info(s"All fixtures inserted in test collection '$n'"))
        }

        col
      }

      @inline def tailable(n: String, database: DB = db)(implicit ec: ExecutionContext) = {
        implicit val reader = IdReader
        collection(n, database).find(matchAll("cursorspec50")).options(
          QueryOpts().tailable
        ).cursor[Int]()
      }

      "using tailable" >> {
        "to fold responses" in { implicit ee: EE =>
          implicit val reader = IdReader
          tailable("foldr0").foldResponses(List.empty[Int], 6) { (s, resp) =>
            val bulk = Response.parse(resp).flatMap(_.asOpt[Int].toList)

            Cursor.Cont(s ++ bulk)
          } must beEqualTo(List(0, 1, 2, 3, 4, 5)).await(1, timeout)
        }

        "to fold responses with async function" in { implicit ee: EE =>
          implicit val reader = IdReader
          tailable("foldr0").foldResponsesM(List.empty[Int], 6) { (s, resp) =>
            val bulk = Response.parse(resp).flatMap(_.asOpt[Int].toList)

            Future.successful(Cursor.Cont(s ++ bulk))
          } must beEqualTo(List(0, 1, 2, 3, 4, 5)).await(1, timeout)
        }

        "to fold bulks" in { implicit ee: EE =>
          tailable("foldw0a").foldBulks(List.empty[Int], 6)(
            (s, bulk) => Cursor.Cont(s ++ bulk)
          ) must beEqualTo(List(
              0, 1, 2, 3, 4, 5
            )).await(1, timeout)
        }

        "to fold bulks with async function" in { implicit ee: EE =>
          tailable("foldw0b").foldBulksM(List.empty[Int], 6)(
            (s, bulk) => Future(Cursor.Cont(s ++ bulk))
          ) must beEqualTo(List(
              0, 1, 2, 3, 4, 5
            )).await(1, timeout)
        }
      }

      "using tailable foldWhile" >> {
        "successfully" in { implicit ee: EE =>
          tailable("foldw1a").foldWhile(List.empty[Int], 5)(
            (s, i) => Cursor.Cont(i :: s)
          ) must beEqualTo(List(
              4, 3, 2, 1, 0
            )).await(1, timeout)
        }

        "successfully with async function" in { implicit ee: EE =>
          tailable("foldw1b").foldWhileM(List.empty[Int], 5)(
            (s, i) => Future(Cursor.Cont(i :: s))
          ) must beEqualTo(List(
              4, 3, 2, 1, 0
            )).await(1, timeout)
        }

        "leading to timeout w/o maxDocs" in { implicit ee: EE =>
          Await.result(tailable("foldw2").foldWhile(List.empty[Int])(
            (s, i) => Cursor.Cont(i :: s),
            (_, e) => Cursor.Fail(e)
          ), timeout) must throwA[Exception]
        }

        "gracefully stop at connection close w/o maxDocs" in {
          implicit ee: EE =>
            val con = driver.connection(List(primaryHost), DefaultOptions)
            lazy val delayedTimeout = FiniteDuration(
              (timeout.toMillis * 1.25D).toLong, MILLISECONDS
            )

            con.database(
              "specs2-test-reactivemongo", failoverStrategy
            ).flatMap { d =>
                tailable("foldw3", d).foldWhile(List.empty[Int])((s, i) => {
                  if (i == 1) con.close() // Force connection close
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
