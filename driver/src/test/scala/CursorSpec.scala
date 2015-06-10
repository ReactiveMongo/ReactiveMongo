import org.specs2.mutable._
import reactivemongo.bson._
import DefaultBSONHandlers._
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import play.api.libs.iteratee.Iteratee
import reactivemongo.api.{ Cursor, CursorProducer, QueryOpts, WrappedCursor }

class CursorSpec extends Specification {
  sequential

  import Common._

  val coll = db("cursorspec")

  "ReactiveMongo" should {
    "insert 16,517 records" in {
      val futs = for(i <- 0 until 16517)
        yield coll.insert(BSONDocument("i" -> BSONInteger(i), "record" -> BSONString("record" + i)))
      val fut = Future.sequence(futs)
      Await.result(fut, DurationInt(20).seconds)
      println("inserted 16,517 records")
      success
    }
    "get all the 16,517 documents" in {
      var i = 0
      val future = coll.find(BSONDocument()).cursor.enumerate() |>>> (Iteratee.foreach({ e =>
        //println(s"doc $i => $e")
        i += 1
      }))
      /*val future = coll.find(BSONDocument()).cursor.documentStream.map { doc =>
        i += 1
        println("fetched " + doc)
        doc
       }.runLast*/
      future.map(_ => i) must beEqualTo(16517).await(21000/*21s*/)
    }

    "get 10 first docs" in {
      coll.find(BSONDocument()).cursor.collect[List](10).map(_.size).
        aka("result size") must beEqualTo(10).await(timeoutMillis)
    }

    "produce a custom cursor for the results" in {
      implicit def fooProducer[T] = new CursorProducer[T] {
        type ProducedCursor = FooCursor[T]
        def produce(base: Cursor[T]) = new FooCursor(base)
      }

      coll.find(BSONDocument()).cursor.foo must_== "Bar"
    }    
  }

  "BSON Cursor" should {
    object IdReader extends BSONDocumentReader[Int] {
      def read(doc: BSONDocument): Int = doc.getAs[Int]("id").get
    }

    val expectedList = List(9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
    @inline def toList =
      Iteratee.fold[Int, List[Int]](List.empty[Int]) { (l, i) => i :: l }

    "read from collection" >> {
      def collection(n: String) = {
        val col = db(s"somecollection_$n")

        Future.sequence((0 until 10) map { id =>
          col.insert(BSONDocument("id" -> id))
        }) map { _ =>
          println(s"-- all documents inserted in test collection $n")
          col
        }
      }

      @inline def cursor(n: String): Cursor[Int] = {
        implicit val reader = IdReader
        Cursor.flatten(collection(n).map(_.find(BSONDocument()).
          sort(BSONDocument("id" -> 1)).cursor[Int]))
      }

      "successfully using cursor" in {
        (cursor("senum1").enumerate(10) |>>> toList).
          aka("enumerated") must beEqualTo(expectedList).await(timeoutMillis)
      }
    }

    "read from capped collection" >> {
      def collection(n: String) = {
        val col = db(s"somecollection_captail_$n")
        col.createCapped(4096, Some(10))

        Future {
          (0 until 10).foreach { id =>
            col.insert(BSONDocument("id" -> id))
            Thread.sleep(500)
          }
          println(s"-- all documents inserted in test collection $n")
        }

        col
      }

      @inline def tailable(n: String) = {
        implicit val reader = IdReader
        collection(n).find(BSONDocument()).options(
          QueryOpts().tailable).cursor[Int]
      }

      "successfully using tailable enumerator with maxDocs" in {
        (tailable("tenum1").enumerate(10) |>>> toList).
          aka("enumerated") must beEqualTo(expectedList).await(timeoutMillis)
      }

      "with timeout using tailable enumerator w/o maxDocs" in {
        Await.result(tailable("tenum2").enumerate() |>>> toList, timeout).
          aka("enumerated") must throwA[Exception]
      }

      "using tailable foldWhile" in {
        tailable("foldw1").foldWhile(List.empty[Int], 5)(
          (s, i) => Cursor.Cont(i :: s),
          (_, e) => Cursor.Fail(e)) must beEqualTo(List(
            4, 3, 2, 1, 0)).await(1000)
      }

      "with timeout using tailable foldWhile w/o maxDocs" in {
        Await.result(tailable("foldw2").foldWhile(List.empty[Int])(
          (s, i) => Cursor.Cont(i :: s),
          (_, e) => Cursor.Fail(e)), timeout) must throwA[Exception]

      }
    }
  }

  class FooCursor[T](val wrappee: Cursor[T]) extends WrappedCursor[T] {
    val foo = "Bar"
  }
}
