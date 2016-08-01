import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

import play.api.libs.iteratee.Iteratee

import reactivemongo.bson.{
  BSONDocument,
  BSONDocumentReader,
  BSONDocumentWriter,
  BSONInteger,
  BSONString
}
import reactivemongo.api.{
  Cursor,
  DB,
  QueryOpts,
  MongoDriver
}, Cursor.{ ContOnError, FailOnError }
import reactivemongo.core.protocol.Response
import reactivemongo.play.iteratees.PlayIterateesCursor

import org.specs2.concurrent.{ ExecutionEnv => EE }

class CursorSpec extends org.specs2.mutable.Specification {
  "Cursor" title

  sequential

  import Common._
  import reactivemongo.play.iteratees.cursorProducer

  "BSON collection" should {
    "be provided the fixtures" >> {
      val fixtures = List(
        Person("Jack", 25),
        Person("James", 16),
        Person("John", 34),
        Person("Jane", 24),
        Person("Joline", 34)
      )

      "with insert" in { implicit ee: EE =>
        implicit val writer = PersonWriter

        Future.sequence(fixtures.map(personColl.insert(_).map(_.ok))).
          aka("fixtures") must beEqualTo(List(true, true, true, true, true)).
          await(0, timeout)
      }
    }

    "read empty cursor" >> {
      @inline def cursor: PlayIterateesCursor[BSONDocument] =
        personColl.find(BSONDocument("plop" -> "plop")).cursor[BSONDocument]()

      "with success using enumerate" in { implicit ee: EE =>
        val cur = cursor
        val enumerator = cur.enumerator(10)

        (enumerator |>>> Iteratee.fold(0) { (r, doc) => r + 1 }).
          aka("read") must beEqualTo(0).await(0, timeout)
      }
    }

    "read documents until error" in { implicit ee: EE =>
      implicit val reader = new SometimesBuggyPersonReader
      val enumerator = personColl.find(BSONDocument()).
        cursor[Person]().enumerator()

      var i = 0
      (enumerator |>>> Iteratee.foreach { doc =>
        i += 1
        //println(s"\tgot doc: $doc")
      } map (_ => -1)).
        recover({ case e => i }) must beEqualTo(3).await(0, timeout)
    }

    "read documents skipping errors" in { implicit ee: EE =>
      implicit val reader = new SometimesBuggyPersonReader
      val enumerator = personColl.find(BSONDocument()).
        cursor[Person]().enumerator(err = ContOnError[Unit]())

      var i = 0
      (enumerator |>>> Iteratee.foreach { doc =>
        i += 1
        //println(s"\t(skipping [$i]) got doc: $doc")
      }).map(_ => i) must beEqualTo(4).await(0, timeout)
    }

    "insert 16,517 records" in { implicit ee: EE =>
      val futs = for (i <- 0 until 16517)
        yield coll2.insert(BSONDocument(
        "i" -> BSONInteger(i), "record" -> BSONString("record" + i)
      ))

      Future.sequence(futs).map(_ => {}) must beEqualTo({}).
        await(0, 20.seconds)
    }

    "enumerate" >> {
      "all the 16,517 documents" in { implicit ee: EE =>
        var i = 0
        coll2.find(BSONDocument.empty).cursor[BSONDocument]().enumerator() |>>> (
          Iteratee.foreach { e: BSONDocument =>
            //println(s"doc $i => $e")
            i += 1
          }
        ).map(_ => i) must beEqualTo(16517).await(0, 21.seconds)
      }

      "only 1024 documents" in { implicit ee: EE =>
        var i = 0
        coll2.find(BSONDocument.empty).cursor[BSONDocument]().
          enumerator(1024) |>>> (Iteratee.foreach { e: BSONDocument =>
            //println(s"doc $i => $e")
            i += 1
          }).map(_ => i) must beEqualTo(1024).await(0, timeout)
      }
    }

    "enumerate bulks" >> {
      "for all the documents" in { implicit ee: EE =>
        var i = 0
        coll2.find(BSONDocument.empty).cursor[BSONDocument]().
          bulkEnumerator() |>>> (
            Iteratee.foreach { it: Iterator[BSONDocument] =>
              //println(s"doc $i => $e")
              i += it.size
            }
          ).map(_ => i) must beEqualTo(16517).await(0, 21.seconds)
      }

      "for only 1024 documents" in { implicit ee: EE =>
        var i = 0
        coll2.find(BSONDocument.empty).cursor[BSONDocument]().
          bulkEnumerator(1024) |>>> (
            Iteratee.foreach { it: Iterator[BSONDocument] =>
              //println(s"doc $i => $e")
              i += it.size
            }
          ).map(_ => i) must beEqualTo(1024).await(0, timeout)
      }
    }

    "enumerate responses" >> {
      "for all the 16,517 documents" in { implicit ee: EE =>
        var i = 0
        coll2.find(BSONDocument.empty).cursor[BSONDocument]().
          responseEnumerator() |>>> (Iteratee.foreach { r: Response =>
            //println(s"doc $i => $e")
            i += r.reply.numberReturned
          }).map(_ => i) must beEqualTo(16517).await(0, 21.seconds)
      }

      "for only 1024 documents" in { implicit ee: EE =>
        var i = 0
        coll2.find(BSONDocument.empty).cursor[BSONDocument]().
          responseEnumerator(1024) |>>> (Iteratee.foreach { r: Response =>
            //println(s"doc $i => $e")
            i += r.reply.numberReturned
          }).map(_ => i) must beEqualTo(1024).await(0, timeout)
      }
    }

    "stop on error" >> {
      val drv = new MongoDriver
      def con = drv.connection(List(primaryHost), DefaultOptions)
      def scol(n: String = coll2.name) =
        Await.result(con.database(db.name).map(_.collection(n)), timeout)

      "when enumerating responses" >> {
        "if fails while processing with existing documents (#1)" in {
          implicit ee: EE =>

            var count = 0
            val inc = Iteratee.foreach[Response] { _ =>
              if (count == 1) sys.error("Foo")

              count = count + 1
            }
            val c = scol()
            val cursor = c.find(BSONDocument.empty).options(
              QueryOpts(batchSizeN = 2)
            ).cursor()

            (cursor.responseEnumerator(10, FailOnError[Unit]()) |>>> inc).
              map(_ => count).recover({ case _ => count }).
              aka("enumerating") must beEqualTo(1).await(0, timeout)

        }

        "if fails while processing with existing documents (#2)" in {
          implicit ee: EE =>

            // As the error is on the consuming side (Iteratee)
            var count = 0
            var i = 0
            val inc = Iteratee.foreach[Response] { _ =>
              i = i + 1
              if (i % 2 == 0) sys.error("Foo")
              count = count + 1
            }
            val c = scol()
            val cursor = c.find(BSONDocument.empty).options(QueryOpts(
              batchSizeN = 4
            )).cursor()

            (cursor.responseEnumerator(128, ContOnError[Unit]()) |>>> inc).
              recover({ case _ => count }) must beEqualTo(1).await(0, timeout)
        }

        "if fails while processing w/o documents (#1)" in {
          implicit ee: EE =>

            var count = 0
            val inc = Iteratee.foreach[Response] { _ =>
              count = count + 1
              sys.error("Foo")
            }
            val c = scol(System.identityHashCode(inc).toString)
            val cursor = c.find(BSONDocument.empty).options(
              QueryOpts(batchSizeN = 2)
            ).cursor()

            (cursor.responseEnumerator(10, FailOnError[Unit]()) |>>> inc).
              recover({ case _ => count }) must beEqualTo(1).await(0, timeout)
        }

        "if fails while processing w/o documents (#2)" in {
          implicit ee: EE =>

            // As the error is on the consuming side (Iteratee)          
            var count = 0
            val inc = Iteratee.foreach[Response] { _ =>
              count = count + 1
              sys.error("Foo")
            }
            val c = scol(System.identityHashCode(inc).toString)
            val cursor = c.find(BSONDocument.empty).
              options(QueryOpts(batchSizeN = 2)).cursor()

            (cursor.responseEnumerator(64, ContOnError[Unit]()) |>>> inc).
              recover({ case _ => count }) must beEqualTo(1).await(0, timeout)
        }

        "if fails to send request" in {
          implicit ee: EE =>

            var count = 0
            val inc = Iteratee.foreach[Response] { _ => count = count + 1 }
            val c = scol()
            val cursor = c.find(BSONDocument.empty).cursor()

            c.db.connection.close()
            // Close connection to make the related cursor erroneous

            (cursor.responseEnumerator(10, FailOnError[Unit]()) |>>> inc).
              map(_ => count).recover({ case _ => count }).
              aka("count") must beEqualTo(0).await(0, timeout)
        }
      }

      "when enumerating bulks" >> {
        "if fails while processing with existing documents (#1)" in {
          implicit ee: EE =>

            var count = 0
            val inc = Iteratee.foreach[Iterator[BSONDocument]] { _ =>
              if (count == 1) sys.error("Foo")

              count = count + 1
            }
            val c = scol()
            val cursor = c.find(BSONDocument.empty).options(
              QueryOpts(batchSizeN = 2)
            ).cursor()

            (cursor.bulkEnumerator(10, FailOnError[Unit]()) |>>> inc).
              recover({ case _ => count }).
              aka("enumerating") must beEqualTo(1).await(0, timeout)
        }

        "if fails while processing with existing documents (#2)" in {
          implicit ee: EE =>

            var count = 0
            var i = 0
            val inc = Iteratee.foreach[Iterator[BSONDocument]] { _ =>
              i = i + 1
              if (i % 2 == 0) sys.error("Foo")
              count = count + 1
            }
            val c = scol()
            val cursor = c.find(BSONDocument.empty).options(QueryOpts(
              batchSizeN = 4
            )).cursor()

            (cursor.bulkEnumerator(128, ContOnError[Unit]()) |>>> inc).
              recover({ case _ => count }) must beEqualTo(1).await(0, timeout)
        }

        "if fails while processing w/o documents (#1)" in {
          implicit ee: EE =>

            var count = 0
            val inc = Iteratee.foreach[Iterator[BSONDocument]] { _ =>
              count = count + 1
              sys.error("Foo")
            }
            val c = scol(System.identityHashCode(inc).toString)
            val cursor = c.find(BSONDocument.empty).options(
              QueryOpts(batchSizeN = 2)
            ).cursor()

            (cursor.bulkEnumerator(10, FailOnError[Unit]()) |>>> inc).
              recover({ case _ => count }) must beEqualTo(1).await(0, timeout)
        }

        "if fails while processing w/o documents (#2)" in {
          implicit ee: EE =>

            var count = 0
            val inc = Iteratee.foreach[Iterator[BSONDocument]] { _ =>
              count = count + 1
              sys.error("Foo")
            }
            val c = scol(System.identityHashCode(inc).toString)
            val cursor = c.find(BSONDocument.empty).
              options(QueryOpts(batchSizeN = 2)).cursor()

            (cursor.bulkEnumerator(64, ContOnError[Unit]()) |>>> inc).
              recover({ case _ => count }) must beEqualTo(1).await(0, timeout)
        }

        "if fails to send request" in {
          implicit ee: EE =>

            var count = 0
            val inc = Iteratee.foreach[Iterator[BSONDocument]] { _ =>
              count = count + 1
            }
            val c = scol()
            val cursor = c.find(BSONDocument.empty).cursor()

            c.db.connection.close()
            // Close connection to make the related cursor erroneous

            (cursor.bulkEnumerator(10, FailOnError[Unit]()) |>>> inc).
              recover({ case _ => count }) must beEqualTo(0).await(0, timeout)
        }
      }

      "when enumerating documents" >> {
        "if fails while processing with existing documents" in {
          implicit ee: EE =>

            var count = 0
            val inc = Iteratee.foreach[BSONDocument] { _ =>
              if (count == 5) sys.error("Foo")

              count = count + 1
            }
            val c = scol()
            val cursor = c.find(BSONDocument.empty).cursor()

            (cursor.enumerator(10, FailOnError[Unit]()) |>>> inc).
              recover({ case _ => count }) must beEqualTo(5).await(0, timeout)
        }

        "if fails to send request" in {
          implicit ee: EE =>

            var count = 0
            val inc = Iteratee.foreach[BSONDocument] { _ => count = count + 1 }
            val c = scol()
            val cursor = c.find(BSONDocument.empty).cursor()

            c.db.connection.close()
            // Close connection to make the related cursor erroneous

            (cursor.enumerator(10, FailOnError[Unit]()) |>>> inc).
              recover({ case _ => count }) must beEqualTo(0).await(0, timeout)
        }
      }

      "Driver instance must be closed" in {
        drv.close() must not(throwA[Exception])
      }
    }

    "continue on error" >> {
      val drv = new MongoDriver
      def con = drv.connection(List(primaryHost), DefaultOptions)
      def scol(n: String = coll2.name) =
        Await.result(con.database(db.name).map(_(n)), timeout)

      "when enumerating responses" >> {
        "if fails to send request" in {
          implicit ee: EE =>

            var count = 0
            val inc = Iteratee.foreach[Response] { _ => count = count + 1 }
            val c = scol()
            val cursor = c.find(BSONDocument.empty).cursor()

            c.db.connection.close()
            // Close connection to make the related cursor erroneous

            (cursor.responseEnumerator(128, ContOnError[Unit]()) |>>> inc).
              map(_ => count) must beEqualTo(0).await(0, timeout)
        }
      }

      "when enumerating bulks" >> {
        "if fails to send request" in {
          implicit ee: EE =>

            var count = 0
            val inc = Iteratee.foreach[Iterator[BSONDocument]] {
              _ => count = count + 1
            }
            val c = scol()
            val cursor = c.find(BSONDocument.empty).cursor()

            c.db.connection.close()
            // Close connection to make the related cursor erroneous

            (cursor.bulkEnumerator(128, ContOnError[Unit]()) |>>> inc).
              map(_ => count) must beEqualTo(0).await(0, timeout)
        }
      }

      "when enumerating documents" >> {
        "if fails while processing with existing documents" in {
          implicit ee: EE =>

            var count = 0
            var i = 0
            val inc = Iteratee.foreach[BSONDocument] { _ =>
              i = i + 1
              if (i % 2 == 0) sys.error("Foo")
              count = count + 1
            }
            val c = scol()
            val cursor = c.find(BSONDocument.empty).options(QueryOpts(
              batchSizeN = 4
            )).cursor()

            (cursor.enumerator(128, ContOnError[Unit]()) |>>> inc).
              recover({ case _ => count }) must beEqualTo(1).await(0, timeout)
        }

        "if fails while processing w/o documents" in {
          implicit ee: EE =>

            var count = 0
            val inc = Iteratee.foreach[BSONDocument] { _ =>
              count = count + 1
              sys.error("Foo")
            }
            val c = scol(System.identityHashCode(inc).toString)
            val cursor = c.find(BSONDocument.empty).
              options(QueryOpts(batchSizeN = 2)).cursor()

            (cursor.enumerator(64, ContOnError[Unit]()) |>>> inc).map(_ => count).
              aka("enumerating") must beEqualTo(0).await(0, timeout)
        }

        "if fails to send request" in {
          implicit ee: EE =>

            var count = 0
            val inc = Iteratee.foreach[BSONDocument] {
              _ => count = count + 1
            }
            val c = scol()
            val cursor = c.find(BSONDocument.empty).cursor()

            c.db.connection.close()
            // Close connection to make the related cursor erroneous

            (cursor.enumerator(128, ContOnError[Unit]()) |>>> inc).
              map(_ => count) must beEqualTo(0).await(0, timeout)
        }
      }

      "Driver instance must be closed" in {
        drv.close() must not(throwA[Exception])
      }
    }
  }

  "BSON Cursor" should {
    object IdReader extends BSONDocumentReader[Int] {
      def read(doc: BSONDocument): Int = doc.getAs[Int]("id").get
    }

    val expectedList = List(9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
    val toList =
      Iteratee.fold[Int, List[Int]](List.empty[Int]) { (l, i) => i :: l }

    "read from collection" >> {
      def collection(n: String) = {
        val col = db(s"somecollection_$n")

        Future.sequence((0 until 10) map { id =>
          col.insert(BSONDocument("id" -> id))
        }) map { _ =>
          logger.debug(s"-- all documents inserted in test collection $n")
          col
        }
      }

      @inline def cursor(n: String): PlayIterateesCursor[Int] = {
        implicit val reader = IdReader
        Cursor.flatten(collection(n).map(_.find(BSONDocument()).
          sort(BSONDocument("id" -> 1)).cursor[Int]()))
      }

      "successfully using cursor enumerator" >> {
        "per document" in {
          implicit ee: EE =>
            (cursor("senum1").enumerator(10) |>>> toList).
              aka("enumerated") must beEqualTo(expectedList).await(0, timeout)
        }

        "per bulk" in {
          implicit ee: EE =>

            val collect = Iteratee.fold[Iterator[Int], List[Int]](
              List.empty[Int]
            ) { _ ++ _ }

            (cursor("senum2").bulkEnumerator(10) |>>> collect).map(_.reverse).
              aka("enumerated") must beEqualTo(expectedList).await(0, timeout)
        }
      }
    }

    "read from capped collection" >> {
      def collection(n: String, database: DB) = {
        val col = database(s"somecollection_captail_$n")

        col.createCapped(4096, Some(10)).flatMap { _ =>
          (0 until 10).foldLeft(Future successful {}) { (f, id) =>
            f.flatMap(_ => col.insert(BSONDocument("id" -> id)).map(_ =>
              Thread.sleep(200)))
          }.map(_ =>
            logger.debug(s"-- all documents inserted in test collection $n"))
        }

        col
      }

      @inline def tailable(n: String, database: DB = db) = {
        implicit val reader = IdReader
        collection(n, database).find(BSONDocument()).options(
          QueryOpts().tailable
        ).cursor[Int]()
      }

      "successfully using tailable enumerator with maxDocs" in {
        implicit ee: EE =>
          (tailable("tenum1").enumerator(10) |>>> toList).
            aka("enumerated") must beEqualTo(expectedList).await(0, timeout)
      }

      "with timeout using tailable enumerator w/o maxDocs" in {
        implicit ee: EE =>
          Await.result((tailable("tenum2").enumerator() |>>> toList), timeout).
            aka("enumerated") must throwA[java.util.concurrent.TimeoutException]
      }
    }
  }

  // ---

  lazy val personColl = db("playiteratees_person")
  lazy val coll2 = db(s"playiteratees_${System identityHashCode personColl}")

  case class Person(name: String, age: Int)

  class SometimesBuggyPersonReader extends BSONDocumentReader[Person] {
    private var i = 0
    def read(doc: BSONDocument): Person = {
      i += 1
      if (i % 4 == 0) throw CustomException("hey hey hey")
      else Person(doc.getAs[String]("name").get, doc.getAs[Int]("age").get)
    }
  }

  object PersonWriter extends BSONDocumentWriter[Person] {
    def write(p: Person): BSONDocument =
      BSONDocument("age" -> p.age, "name" -> p.name)
  }

  case class CustomException(msg: String) extends Exception(msg)
}
