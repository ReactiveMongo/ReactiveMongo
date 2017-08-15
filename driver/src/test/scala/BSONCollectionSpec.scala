import scala.concurrent._, duration.FiniteDuration

import reactivemongo.api._, collections.bson.BSONCollection
import reactivemongo.bson._
import reactivemongo.core.errors.GenericDatabaseException

import org.specs2.concurrent.{ ExecutionEnv => EE }

class BSONCollectionSpec extends org.specs2.mutable.Specification {
  "BSON collection" title

  import reactivemongo.api.commands.bson.DefaultBSONCommandError
  import reactivemongo.api.collections.bson._
  import Common._

  sequential

  val colName = s"bsoncoll${System identityHashCode this}"
  lazy val collection = db(colName)
  lazy val slowColl = slowDb(colName)

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
      if (i % 4 == 0) throw CustomException("hey hey hey")
      else Person(doc.getAs[String]("name").get, doc.getAs[Int]("age").get)
    }
  }

  object PersonWriter extends BSONDocumentWriter[Person] {
    def write(p: Person): BSONDocument =
      BSONDocument("age" -> p.age, "name" -> p.name)
  }

  object PersonReader extends BSONDocumentReader[Person] {
    def read(doc: BSONDocument): Person =
      Person(doc.getAs[String]("name").get, doc.getAs[Int]("age").get)
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
        collection.insert(person).map(_.ok) must beTrue.await(1, timeout) and {
          val coll = collection.withReadPreference(ReadPreference.secondary)

          coll.readPreference must_== ReadPreference.secondary and {
            // Anyway use ReadPreference.Primary for insert op
            coll.insert(person2).map(_.ok) must beTrue.await(1, timeout)
          }
        }
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

    "read cursor" >> {
      @inline def cursor(implicit ec: ExecutionContext): Cursor[BSONDocument] =
        collection.find(BSONDocument("plop" -> "plop")).cursor[BSONDocument]()

      "when empty with success using collect" in { implicit ee: EE =>
        cursor.collect[Vector](10).map(_.length) must beEqualTo(0).
          await(1, timeout)
      }

      "successfully with 'name' projection using collect" in {
        implicit ee: EE =>
          collection.find(BSONDocument("age" -> 25), BSONDocument("name" -> 1)).
            one[BSONDocument] must beSome[BSONDocument].which { doc =>
              doc.elements.size must_== 2 /* _id+name */ and (
                doc.getAs[String]("name") aka "name" must beSome("Jack"))
            }.await(2, timeout)
      }

      "explain query result" >> {
        "when MongoDB > 2.6" in { implicit ee: EE =>
          findAll(collection).explain().one[BSONDocument].
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
          findAll(collection).explain().one[BSONDocument].
            aka("explanation") must beSome[BSONDocument].which { result =>
              result.getAs[List[BSONDocument]]("allPlans").
                aka("plans") must beSome[List[BSONDocument]] and (
                  result.getAs[String]("server").
                  aka("server") must beSome[String])

            }.await(1, timeout)
        } tag "mongo2"
      }
    }

    "read until John" in { implicit ee: EE =>
      implicit val reader = PersonReader
      @inline def cursor = findAll(collection).sort(
        BSONDocument("age" -> 1)).cursor[Person]()

      val persons = Seq(person2, person4, person, person3)

      cursor.foldWhile(Nil: Seq[Person])({ (s, p) =>
        if (p.name == "John") Cursor.Done(s :+ p)
        else Cursor.Cont(s :+ p)
      }, (_, e) => Cursor.Fail(e)) must beEqualTo(persons).await(1, timeout)
    }

    "read a document with error" in { implicit ee: EE =>
      implicit val reader = BuggyPersonReader
      val future = findAll(collection).one[Person].map(_ => 0).recover {
        case e if e.getMessage == "hey hey hey" => -1
        case _ =>
          /* e.printStackTrace() */ -2
      }

      future must beEqualTo(-1).await(1, timeout)
    }

    {
      def cursorSpec(c: BSONCollection, timeout: FiniteDuration) = {
        implicit val reader = new SometimesBuggyPersonReader
        @inline def cursor(implicit ec: ExecutionContext) =
          findAll(c).cursor[Person]()

        "using collect" in { implicit ee: EE =>
          val collect = cursor.collect[Vector]().map(_.size).recover {
            case e if e.getMessage == "hey hey hey" => -1
            case _ =>
              /* e.printStackTrace() */ -2
          }

          collect aka "first collect" must not(throwA[Exception]).
            await(1, timeout) and (collect must beEqualTo(-1).await(1, timeout))
        }

        "using foldWhile" in { implicit ee: EE =>
          cursor.foldWhile(0)(
            (i, _) => Cursor.Cont(i + 1),
            (_, e) => Cursor.Fail(e)) must throwA[CustomException].
            await(1, timeout)
        }

        "fallbacking to final value using foldWhile" in { implicit ee: EE =>
          cursor.foldWhile(0)(
            (i, _) => Cursor.Cont(i + 1),
            (_, e) => Cursor.Done(-1)) must beEqualTo(-1).await(1, timeout)
        }

        "skiping failure using foldWhile" in { implicit ee: EE =>
          cursor.foldWhile(0)(
            (i, _) => Cursor.Cont(i + 1),
            (_, e) => Cursor.Cont(-3)) must beEqualTo(-2).await(1, timeout)
        }
      }

      "read documents with error with the default connection" >> {
        cursorSpec(collection, timeout)
      }

      "read documents with error with the slow connection" >> {
        cursorSpec(slowColl, slowTimeout)
      }
    }

    "read documents skipping errors using collect" >> {
      implicit val reader = new SometimesBuggyPersonReader
      def resultSpec(c: BSONCollection, timeout: FiniteDuration)(implicit ee: EE) = findAll(c).cursor[Person]().collect[Vector](stopOnError = false).
        map(_.length) must beTypedEqualTo(4).await(1, timeout)

      "with the default connection" in { implicit ee: EE =>
        resultSpec(collection, timeout)
      }

      "with the default connection" in { implicit ee: EE =>
        resultSpec(slowColl, slowTimeout)
      }
    }

    "write a document with error" >> {
      implicit val writer = BuggyPersonWriter
      def writeSpec(c: BSONCollection, timeout: FiniteDuration)(implicit ee: EE) = c.insert(person).map { lastError =>
        //println(s"person write succeed??  $lastError")
        0
      }.recover {
        case _: CustomException => -1
        case e =>
          e.printStackTrace()
          -2
      } aka "write result" must beEqualTo(-1).await(1, timeout)

      "with the default connection" in { implicit ee: EE =>
        writeSpec(collection, timeout)
      }

      "with the slow connection" in { implicit ee: EE =>
        writeSpec(slowColl, slowTimeout)
      }
    }

    "write a JavaScript value" in { implicit ee: EE =>
      collection.insert(BSONDocument(
        "age" -> 101,
        "name" -> BSONJavaScript("db.getName()"))).flatMap { _ =>
        collection.find(BSONDocument("age" -> 101)).one[BSONDocument].map(
          _.flatMap(_.getAs[BSONJavaScript]("name")).map(_.value))
      } aka "inserted" must beSome("db.getName()").await(1, timeout)
    }

    { // Find & update
      implicit val reader = PersonReader
      implicit val writer = PersonWriter

      def findAndUpdateSpec(c: BSONCollection, timeout: FiniteDuration, five: Person = person5) = {
        "by updating age of 'Joline', & returns the old document" in {
          implicit ee: EE =>
            val updateOp = c.updateModifier(
              BSONDocument("$set" -> BSONDocument("age" -> 35)))

            c.findAndModify(BSONDocument("name" -> "Joline"), updateOp).
              map(_.result[Person]) must beSome(five).await(1, timeout)
        }

        "by updating age of 'James', & returns the updated document" in {
          implicit ee: EE =>
            c.findAndUpdate(
              BSONDocument("name" -> "James"), person2.copy(age = 17),
              fetchNewObject = true).map(_.result[Person]).
              aka("result") must beSome(person2.copy(age = 17)).await(1, timeout)
        }

        "by inserting a new 'Foo' person (with upsert = true)" in {
          implicit ee: EE =>
            val fooPerson = Person("Foo", -1)

            c.findAndUpdate(fooPerson, fooPerson,
              fetchNewObject = true, upsert = true).
              map(_.result[Person]) must beSome(fooPerson).await(1, timeout)
        }
      }

      "find and update with the default connection" >> {
        findAndUpdateSpec(collection, timeout)
      }

      "find and update with the slow connection" >> {
        findAndUpdateSpec(slowColl, slowTimeout, person5.copy(age = 35))
      }
    }

    {
      implicit val reader = PersonReader

      "with default connection" >> {
        "find and remove 'Joline' using findAndModify" in { implicit ee: EE =>
          collection.findAndModify(
            BSONDocument("name" -> "Joline"),
            collection.removeModifier).map(_.result[Person]).
            aka("removed person") must beSome(person5.copy(age = 35)).
            await(1, timeout)
        }

        "find and remove 'Foo' using findAndRemove" in { implicit ee: EE =>
          collection.findAndRemove(BSONDocument("name" -> "Foo")).
            map(_.result[Person]) aka "removed" must beSome(Person("Foo", -1)).
            await(1, timeout)
        }
      }

      "with slow connection" >> {
        "find and remove 'Joline' using findAndModify" in { implicit ee: EE =>
          collection.findAndModify(
            BSONDocument("name" -> "Joline"),
            collection.removeModifier).map(_.result[Person]).
            aka("removed person") must beNone.await(1, timeout)
        }

        "find and remove 'Foo' using findAndRemove" in { implicit ee: EE =>
          collection.findAndRemove(BSONDocument("name" -> "Foo")).
            map(_.result[Person]) aka "removed" must beNone.await(1, timeout)
        }
      }
    }

    {
      def renameSpec(_db: DefaultDB) =
        "be renamed with failure" in { implicit ee: EE =>
          _db(s"foo_${System identityHashCode _db}").
            rename("renamed").map(_ => false).recover({
              case DefaultBSONCommandError(Some(13), Some(msg), _) if (
                msg contains "renameCollection ") => true
              case _ => false
            }) must beTrue.await(1, timeout)
        }

      "with the default connection" >> renameSpec(db)
      "with the default connection" >> renameSpec(slowDb)
    }

    {
      def dropSpec(_db: DefaultDB) = {
        "be dropped successfully if exists (deprecated)" in { implicit ee: EE =>
          val col = _db(s"foo_${System identityHashCode _db}")

          col.create().flatMap(_ => col.drop(false)).
            aka("legacy drop") must beTrue.await(1, timeout)
        }

        "be dropped with failure if doesn't exist (deprecated)" in {
          implicit ee: EE =>
            val col = _db(s"foo_${System identityHashCode _db}")

            col.drop() aka "legacy drop" must throwA[Exception].like {
              case GenericDatabaseException(_, Some(26)) => ok
            }.await(1, timeout)
        }

        "be dropped successfully if exist" in { implicit ee: EE =>
          val col = _db(s"foo1_${System identityHashCode _db}")

          col.create().flatMap(_ => col.drop(false)).
            aka("dropped") must beTrue.await(1, timeout)
        }

        "be dropped successfully if doesn't exist" in { implicit ee: EE =>
          val col = _db(s"foo2_${System identityHashCode _db}")

          col.drop(false) aka "dropped" must beFalse.await(1, timeout)
        }
      }

      "with the default connection" >> dropSpec(db)
      "with the slow connection" >> dropSpec(db)
    }
  }

  @inline def findAll(c: BSONCollection) = c.find(BSONDocument.empty)
}
