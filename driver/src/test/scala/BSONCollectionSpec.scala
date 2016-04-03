import scala.util.{ Failure, Success }
import scala.concurrent._

import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.core.errors.GenericDatabaseException

import org.specs2.concurrent.{ ExecutionEnv => EE }

object BSONCollectionSpec extends org.specs2.mutable.Specification {
  "BSON collection" title

  import reactivemongo.api.commands.bson.DefaultBSONCommandError
  import reactivemongo.api.collections.bson._
  import Common._

  sequential

  lazy val collection = db("somecollection_bsoncollectionspec")

  case class Person(name: String, age: Int)
  case class CustomException(msg: String) extends Exception(msg)

  object BuggyPersonWriter extends BSONDocumentWriter[Person] {
    def write(p: Person): BSONDocument =
      throw CustomException("PersonWrite error")
  }

  object BuggyPersonReader extends BSONDocumentReader[Person] {
    def read(doc: BSONDocument): Person = throw CustomException("hey hey hey")
  }

  class SometimesBuggyPersonReader extends BSONDocumentReader[Person] {
    var i = 0
    def read(doc: BSONDocument): Person = {
      i += 1
      if (i % 4 == 0)
        throw CustomException("hey hey hey")
      else Person(doc.getAs[String]("name").get, doc.getAs[Int]("age").get)
    }
  }

  object PersonWriter extends BSONDocumentWriter[Person] {
    def write(p: Person): BSONDocument =
      BSONDocument("age" -> p.age, "name" -> p.name)
  }

  object PersonReader extends BSONDocumentReader[Person] {
    def read(doc: BSONDocument): Person = Person(doc.getAs[String]("name").get, doc.getAs[Int]("age").get)
  }

  val person = Person("Jack", 25)
  val person2 = Person("James", 16)
  val person3 = Person("John", 34)
  val person4 = Person("Jane", 24)
  val person5 = Person("Joline", 34)

  "BSON collection" should {
    "write five docs with success" >> {
      sequential

      implicit val writer = PersonWriter

      "with insert" in { implicit ee: EE =>
        collection.insert(person).map(_.ok) must beTrue.await(1, timeout) and (
          collection.insert(person2).map(_.ok) must beTrue.await(1, timeout))
      }

      "with bulkInsert" in { implicit ee: EE =>
        val persons =
          Seq[collection.ImplicitlyDocumentProducer](person3, person4, person5)
        /* OR
        val persons = Seq(person3, person4, person5).
          map(implicitly[collection.ImplicitlyDocumentProducer](_))
         */

        collection.bulkInsert(true)(persons: _*).map(_.ok).
          aka("insertion") must beTrue.await(1, timeout)
      }
    }

    "read empty cursor" >> {
      @inline def cursor(implicit ec: ExecutionContext): Cursor[BSONDocument] =
        collection.find(BSONDocument("plop" -> "plop")).cursor[BSONDocument]()

      "with success using collect" in { implicit ee: EE =>
        cursor.collect[Vector](10).map(_.length) must beEqualTo(0).
          await(1, timeout)
      }

      "read empty cursor with success using collect" in { implicit ee: EE =>
        collection.find(
          BSONDocument("age" -> 25), BSONDocument("name" -> 1)).
          one[BSONDocument] must beSome[BSONDocument].like({
            case doc =>
              doc.elements.size must_== 2 /* _id+name */ and (
                doc.getAs[String]("name") aka "name" must beSome("Jack"))
          }).await(2, timeout)
      }

      "explain query result" >> {
        "when MongoDB > 2.6" in { implicit ee: EE =>
          collection.find(BSONDocument.empty).explain().one[BSONDocument].
            aka("explanation") must beSome[BSONDocument].which { result =>
              result.getAs[BSONDocument]("queryPlanner").
                aka("queryPlanner") must beSome and (
                  result.getAs[BSONDocument]("executionStats").
                  aka("stats") must beSome) and (
                    result.getAs[BSONDocument]("serverInfo").
                    aka("serverInfo") must beSome)

            }.await(1, timeout)
        } tag "not_mongo26"

        "when MongoDB = 2.6" in { implicit ee: EE =>
          collection.find(BSONDocument.empty).explain().one[BSONDocument].
            aka("explanation") must beSome[BSONDocument].which { result =>
              result.getAs[List[BSONDocument]]("allPlans").
                aka("plans") must beSome[List[BSONDocument]] and (
                  result.getAs[String]("server").
                  aka("server") must beSome[String])

            }.await(1, timeout)
        } tag "mongo2"
      }

      "with success using foldResponses" in { implicit ee: EE =>
        cursor.foldResponses(0)(
          (i, _) => Cursor.Cont(i + 1), (_, e) => Cursor.Fail(e)).
          aka("result") must beEqualTo(1 /* one empty response */ ).
          await(1, timeout)

      }

      "with success using foldBulks" in { implicit ee: EE =>
        cursor.foldBulks(0)(
          (i, _) => Cursor.Cont(i + 1), (_, e) => Cursor.Fail(e)).
          aka("result") must beEqualTo(1 /* one empty response */ ).
          await(1, timeout)

      }

      "with success using foldWhile" in { implicit ee: EE =>
        cursor.foldWhile(0)(
          (i, _) => Cursor.Cont(i + 1), (_, e) => Cursor.Fail(e)).
          aka("result") must beEqualTo(0).await(1, timeout)

      }

      "with success as option" in { implicit ee: EE =>
        cursor.headOption must beNone.await(1, timeout)
      }
    }

    "read a document with success" in { implicit ee: EE =>
      implicit val reader = PersonReader
      collection.find(BSONDocument.empty).one[Person] must beSome(person).
        await(1, timeout)
    }

    "read all with success" >> {
      implicit val reader = PersonReader
      @inline def cursor(implicit ec: ExecutionContext) =
        collection.find(BSONDocument.empty).cursor[Person]()

      val persons = Seq(person, person2, person3, person4, person5)

      "as list" in { implicit ee: EE =>
        (cursor.collect[List]() must beEqualTo(persons).await(1, timeout)).
          and(cursor.headOption must beSome(person).await(1, timeout))
      }

      "using foldResponses" in { implicit ee: EE =>
        cursor.foldResponses(0)({ (s, _) => Cursor.Cont(s + 1) },
          (_, e) => Cursor.Fail(e)) must beEqualTo(1).await(1, timeout)

      }

      "using foldBulks" in { implicit ee: EE =>
        cursor.foldBulks(1)({ (s, _) => Cursor.Cont(s + 1) },
          (_, e) => Cursor.Fail(e)) must beEqualTo(2).await(1, timeout)

      }

      "using foldWhile" in { implicit ee: EE =>
        cursor.foldWhile(Nil: Seq[Person])((s, p) => Cursor.Cont(s :+ p),
          (_, e) => Cursor.Fail(e)) must beEqualTo(persons).await(1, timeout)

      }
    }

    "read until John" in { implicit ee: EE =>
      implicit val reader = PersonReader
      @inline def cursor = collection.find(BSONDocument.empty).cursor[Person]()
      val persons = Seq(person, person2, person3)

      cursor.foldWhile(Nil: Seq[Person])({ (s, p) =>
        if (p.name == "John") Cursor.Done(s :+ p)
        else Cursor.Cont(s :+ p)
      }, (_, e) => Cursor.Fail(e)) must beEqualTo(persons).await(1, timeout)
    }

    "read a document with error" in { implicit ee: EE =>
      implicit val reader = BuggyPersonReader
      val future = collection.find(BSONDocument.empty).one[Person].map(_ => 0).recover {
        case e if e.getMessage == "hey hey hey" => -1
        case e =>
          /* e.printStackTrace() */ -2
      }

      future must beEqualTo(-1).await(1, timeout)
    }

    "read documents with error" >> {
      implicit val reader = new SometimesBuggyPersonReader
      @inline def cursor(implicit ec: ExecutionContext) =
        collection.find(BSONDocument.empty).cursor[Person]()

      "using collect" in { implicit ee: EE =>
        val collect = cursor.collect[Vector]().map(_.size).recover {
          case e if e.getMessage == "hey hey hey" => -1
          case e =>
            /* e.printStackTrace() */ -2
        }

        collect aka "first collect" must not(throwA[Exception]).
          await(1, timeout) and (collect must beEqualTo(-1).
            await(1, timeout))
      }

      "using foldWhile" in { implicit ee: EE =>
        Await.result(cursor.foldWhile(0)((i, _) => Cursor.Cont(i + 1),
          (_, e) => Cursor.Fail(e)), timeout) must throwA[CustomException]
      }

      "fallbacking to final value using foldWhile" in { implicit ee: EE =>
        cursor.foldWhile(0)((i, _) => Cursor.Cont(i + 1),
          (_, e) => Cursor.Done(-1)) must beEqualTo(-1).await(1, timeout)
      }

      "skiping failure using foldWhile" in { implicit ee: EE =>
        cursor.foldWhile(0)((i, _) => Cursor.Cont(i + 1),
          (_, e) => Cursor.Cont(-3)) must beEqualTo(-2).await(1, timeout)
      }
    }

    "read documents skipping errors using collect" in { implicit ee: EE =>
      implicit val reader = new SometimesBuggyPersonReader
      val result = Await.result(collection.find(BSONDocument.empty).
        cursor[Person]().collect[Vector](stopOnError = false), timeout)

      //println(s"(read docs skipping errors using collect) got result $result")
      result.length mustEqual 4
    }

    "write a document with error" in { implicit ee: EE =>
      implicit val writer = BuggyPersonWriter

      collection.insert(person).map { lastError =>
        //println(s"person write succeed??  $lastError")
        0
      }.recover {
        case ce: CustomException => -1
        case e =>
          e.printStackTrace()
          -2
      } aka "write result" must beEqualTo(-1).await(1, timeout)
    }

    "write a JavaScript value" in { implicit ee: EE =>
      collection.insert(BSONDocument("age" -> 101,
        "name" -> BSONJavaScript("db.getName()"))).flatMap { _ =>
        implicit val reader = PersonReader
        collection.find(BSONDocument("age" -> 101)).one[BSONDocument].map(
          _.flatMap(_.getAs[BSONJavaScript]("name")).map(_.value))
      } aka "inserted" must beSome("db.getName()").await(1, timeout)
    }

    "find and update" >> {
      implicit val reader = PersonReader
      implicit val writer = PersonWriter

      "by updating age of 'Joline', & returns the old document" in {
        implicit ee: EE =>
          val updateOp = collection.updateModifier(
            BSONDocument("$set" -> BSONDocument("age" -> 35)))

          collection.findAndModify(BSONDocument("name" -> "Joline"), updateOp).
            map(_.result[Person]) must beSome(person5).await(1, timeout)
      }

      "by updating age of 'James', & returns the updated document" in {
        implicit ee: EE =>
          collection.findAndUpdate(
            BSONDocument("name" -> "James"), person2.copy(age = 17),
            fetchNewObject = true).map(_.result[Person]).
            aka("result") must beSome(person2.copy(age = 17)).await(1, timeout)
      }

      "by inserting a new 'Foo' person (with upsert = true)" in {
        implicit ee: EE =>
          val fooPerson = Person("Foo", -1)

          collection.findAndUpdate(fooPerson, fooPerson,
            fetchNewObject = true, upsert = true).
            map(_.result[Person]) must beSome(fooPerson).await(1, timeout)
      }
    }

    "find and remove" >> {
      implicit val reader = PersonReader

      "'Joline' using findAndModify" in {
        implicit ee: EE =>
          collection.findAndModify(BSONDocument("name" -> "Joline"),
            collection.removeModifier).map(_.result[Person]).
            aka("removed person") must beSome(person5.copy(age = 35)).
            await(1, timeout)
      }

      "'Foo' using findAndRemove" in {
        implicit ee: EE =>
          collection.findAndRemove(BSONDocument("name" -> "Foo")).
            map(_.result[Person]) aka "removed" must beSome(Person("Foo", -1)).
            await(1, timeout)
      }
    }

    "be renamed" >> {
      "with failure" in { implicit ee: EE =>
        db(s"foo_${System identityHashCode collection}").
          rename("renamed").map(_ => false).recover({
            case DefaultBSONCommandError(Some(13), Some(msg), _) if (
              msg contains "renameCollection ") => true
            case _ => false
          }) must beTrue.await(1, timeout)
      }
    }

    "be dropped" >> {
      "successfully if exists (deprecated)" in { implicit ee: EE =>
        val col = db(s"foo_${System identityHashCode collection}")

        col.create().flatMap(_ => col.drop(false)).
          aka("legacy drop") must beTrue.await(1, timeout)
      }

      "with failure if doesn't exist (deprecated)" in { implicit ee: EE =>
        val col = db(s"foo_${System identityHashCode collection}")

        Await.result(col.drop(), timeout).
          aka("legacy drop") must throwA[Exception].like {
            case GenericDatabaseException(_, Some(26)) => ok
          }
      }

      "successfully if exist" in { implicit ee: EE =>
        val col = db(s"foo1_${System identityHashCode collection}")

        col.create().flatMap(_ => col.drop(false)).
          aka("drop") must beTrue.await(1, timeout)
      }

      "successfully if doesn't exist" in { implicit ee: EE =>
        val col = db(s"foo2_${System identityHashCode collection}")

        col.drop(false) aka "drop" must beFalse.await(1, timeout)
      }
    }
  }

  "Index" should {
    import reactivemongo.api.indexes._
    lazy val col = db(s"indexed_col_${hashCode}")

    "be first created" in { implicit ee: EE =>
      col.indexesManager.ensure(Index(
        Seq("token" -> IndexType.Ascending), unique = true)).
        aka("index creation") must beTrue.await(1, timeout)
    }

    "not be created if already exists" in { implicit ee: EE =>
      col.indexesManager.ensure(Index(
        Seq("token" -> IndexType.Ascending), unique = true)).
        aka("index creation") must beFalse.await(1, timeout)

    }
  }
}
