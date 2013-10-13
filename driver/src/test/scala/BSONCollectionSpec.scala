import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.core.commands.Count
import scala.concurrent._
import org.specs2.mutable._
import play.api.libs.iteratee.Iteratee

class BSONCollectionSpec extends Specification {
  import Common._

  sequential

  import reactivemongo.api.collections.default._

  lazy val collection = db("somecollection_bsoncollectionspec")

  case class Person(name: String, age: Int)
  case class CustomException(msg: String) extends Exception(msg)

  object BuggyPersonWriter extends BSONDocumentWriter[Person] {
    def write(p: Person): BSONDocument = throw CustomException("PersonWrite error")
  }

  object BuggyPersonReader extends BSONDocumentReader[Person] {
    def read(doc: BSONDocument): Person = throw CustomException("hey hey hey")
  }

  class SometimesBuggyPersonReader extends BSONDocumentReader[Person] {
    var i = 0
    def read(doc: BSONDocument): Person = {
      i += 1
      if(i % 4 == 0)
        throw CustomException("hey hey hey")
      else Person(doc.getAs[String]("name").get, doc.getAs[Int]("age").get)
    }
  }

  object PersonWriter extends BSONDocumentWriter[Person] {
    def write(p: Person): BSONDocument = BSONDocument("age" -> p.age, "name" -> p.name)
  }
  object PersonReader extends BSONDocumentReader[Person] {
    def read(doc: BSONDocument): Person = Person(doc.getAs[String]("name").get, doc.getAs[Int]("age").get)
  }

  val person = Person("Jack", 25)
  val person2 = Person("James", 16)
  val person3 = Person("John", 34)
  val person4 = Person("Jane", 24)
  val person5 = Person("Joline", 34)

  "BSONCollection" should {
    "write three docs with success" in {
      implicit val writer = PersonWriter
      Await.result(collection.insert(person), timeout).ok mustEqual true
      Await.result(collection.insert(person2), timeout).ok mustEqual true
      Await.result(collection.insert(person3), timeout).ok mustEqual true
      Await.result(collection.insert(person4), timeout).ok mustEqual true
      Await.result(collection.insert(person5), timeout).ok mustEqual true
    }
    "read a doc with success" in {
      implicit val reader = PersonReader
      Await.result(collection.find(BSONDocument()).one[Person], timeout).get mustEqual person
    }/*
    "read all with success" in {
      implicit val reader = PersonReader
      Await.result(collection.find(BSONDocument()).cursor[Person].toList, timeout) mustEqual List(person, person2, person3, person4, person5)
    }
    "read a doc with error" in {
      implicit val reader = BuggyPersonReader
      val future = collection.find(BSONDocument()).one[Person].map(_ => 0).recover {
        case e if e.getMessage == "hey hey hey" => -1
        case e =>
          e.printStackTrace()
          -2
      }
      val r = Await.result(future, timeout)
      println(s"read a doc with error: $r")
      Await.result(future, timeout) mustEqual -1
    }
    /*"read docs with error" in {
      implicit val reader = new SometimesBuggyPersonReader
      val future = collection.find(BSONDocument()).cursor[Person].toList.map(_.size).recover {
        case ce: CustomException => -1
        case e =>
          e.printStackTrace()
          -2
      }
      val r = Await.result(future, timeout)
      println(s"read docs with error: $r")
      Await.result(future, timeout) mustEqual -1
    }*/
    "read docs until error" in {
      implicit val reader = new SometimesBuggyPersonReader
      /*val stream = collection.find(BSONDocument()).cursor[Person].documentStream
      
      var i = 0
      val future = stream.map { doc =>
        i += 1
        println(s"\tgot doc: $doc")
        doc
      }.run
      val r = Await.result(future.recover { case e => i }, timeout)
      println(s"read $r/5 docs (expected 3/5)")
      r mustEqual 3 */
      
      val enumerator = collection.find(BSONDocument()).cursor[Person].enumerate(stopOnError = true)
      var i = 0
      val future = enumerator |>>> Iteratee.foreach { doc =>
        i += 1
        println(s"\tgot doc: $doc")
      } map(_ => -1)
      val r = Await.result(future.recover { case e => i }, timeout)
      println(s"read $r/5 docs (expected 3/5)")
      r mustEqual 3
      
      /*val r = Await.result(future, timeout)
      println(s"read a doc with error: $r")
      Await.result(future, timeout) mustEqual -1*/
    }
    "write a doc with error" in {
      implicit val writer = BuggyPersonWriter
      Await.result(
        collection.insert(person).map { lastError =>
          println(s"person write succeed??  $lastError")
          0
        }.recover {
          case ce: CustomException => -1
          case e =>
            e.printStackTrace()
            -2
        }, timeout) mustEqual -1
    }*/
  }
}