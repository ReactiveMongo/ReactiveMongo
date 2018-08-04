import scala.concurrent.duration.FiniteDuration

import reactivemongo.bson.{
  BSON,
  BSONDocument,
  BSONDocumentReader,
  BSONDocumentWriter
}

import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.api.{ BSONSerializationPack, ReadPreference }
import reactivemongo.api.commands._

import reactivemongo.api.collections.bson.BSONCollection

import org.specs2.concurrent.ExecutionEnv

final class FindAndModifySpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification { // with FindAndModifyFixtures {

  "FindAndModify" title

  import tests.Common
  import Common._

  "Raw findAndModify" should {
    import reactivemongo.api.commands.bson.BSONFindAndModifyCommand
    import BSONFindAndModifyCommand._
    import reactivemongo.api.commands.bson.
      BSONFindAndModifyImplicits.FindAndModifyResultReader

    case class Person(
      firstName: String,
      lastName: String,
      age: Int)

    implicit object PersonReader extends BSONDocumentReader[Person] {
      def read(doc: BSONDocument): Person = Person(
        doc.getAs[String]("firstName").getOrElse(""),
        doc.getAs[String]("lastName").getOrElse(""),
        doc.getAs[Int]("age").getOrElse(0))
    }

    implicit object PersonWriter extends BSONDocumentWriter[Person] {
      def write(person: Person): BSONDocument = BSONDocument(
        "firstName" -> person.firstName,
        "lastName" -> person.lastName,
        "age" -> person.age)
    }

    implicit val writer: BSONDocumentWriter[ResolvedCollectionCommand[FindAndModify]] = BSONSerializationPack.writer[ResolvedCollectionCommand[FindAndModify]] { BSONFindAndModifyCommand.serialize(MongoWireVersion.V34, None) }

    val jack1 = Person("Jack", "London", 27)
    val jack2 = jack1.copy(age = /* updated to */ 40)

    "upsert a doc and fetch it" >> {
      def upsertAndFetch(
        c: BSONCollection,
        p: Person,
        age: Int,
        timeout: FiniteDuration)(
        check: FindAndModifyResult => org.specs2.matcher.MatchResult[Any]) = {
        val upsertOp = Update(
          BSONDocument(
            "$set" -> BSONDocument("age" -> age)),
          fetchNewObject = true, upsert = true)

        val after = p.copy(age = age)

        val cmd = FindAndModify(
          query = p,
          modify = upsertOp,
          sort = Option.empty,
          fields = Option.empty,
          bypassDocumentValidation = false,
          writeConcern = WriteConcern.Journaled,
          maxTimeMS = Option.empty,
          collation = Option.empty,
          arrayFilters = Seq.empty)

        c.runCommand(cmd, ReadPreference.primary).
          aka("result") must (beLike[FindAndModifyResult] {
            case result =>
              //println(s"FindAndModify#0: $age -> ${result.lastError}")

              result.lastError.map(_.n) must beSome(1) and check(result) and {
                result.result[Person] must beSome[Person](after)
              } and {
                //println(s"FindAndModify#1{$age}: ${c.name}")

                /*c.find(BSONDocument.empty).cursor[BSONDocument]().
                  collect[List]().flatMap { docs =>
                    println(s"FindAndModify#2: docs = ${docs.map(BSONDocument.pretty)}")
                    c.count(Some(BSON.writeDocument(after)))
                  }.*/
                c.count(Some(BSON.writeDocument(after))).
                  aka("count after") must beTypedEqualTo(1).await(1, timeout)
              }
          }).await(1, timeout)
      }

      "with the default connection" in {
        val after = jack1.copy(age = 37)
        val colName = s"FindAndModifySpec${System identityHashCode this}-1"
        val collection = db(colName)

        collection.count(Some(BSON.writeDocument(jack1))).
          aka("count before") must beTypedEqualTo(0).await(1, timeout) and {
            upsertAndFetch(collection, jack1, after.age, timeout) { result =>
              result.lastError.exists(_.upsertedId.isDefined) must beTrue
            }
          }
      }

      "with the slow connection" in {
        val before = jack1
        val after = jack2
        val colName = s"FindAndModifySpec${System identityHashCode this}-2"
        val slowColl = slowDb(colName)

        slowColl.insert[Person](ordered = true).one(before).
          map(_.n) must beTypedEqualTo(1).await(0, slowTimeout) and {
            slowColl.count(Some(BSON.writeDocument(before))).
              aka("count before") must beTypedEqualTo(1).await(1, slowTimeout)
          } and eventually(2, timeout) {
            upsertAndFetch(
              slowColl, before, after.age, slowTimeout) { result =>
              result.lastError.exists(_.upsertedId.isDefined) must beFalse
            }
          }
      }
    }

    "modify a document and fetch its previous value" in {
      val incrementAge = BSONDocument("$inc" -> BSONDocument("age" -> 1))

      val colName = s"FindAndModifySpec${System identityHashCode this}-3"
      val collection = db(colName)

      def future = collection.findAndUpdate(jack2, incrementAge)

      collection.insert[Person](ordered = true).one(jack2).
        map(_.n) must beTypedEqualTo(1).await(0, slowTimeout) and {
          future must (beLike[FindAndModifyResult] {
            case res =>
              /*
              scala.concurrent.Await.result(collection.find(BSONDocument.empty).
                cursor[BSONDocument]().collect[List]().map { docs =>
                  println(s"persons[${collection.name}]#3{${jack2}} = ${docs.map(BSONDocument.pretty)}")
                }, timeout)
               */

              res.result[Person] aka "previous value" must beSome(jack2)
          }).await(1, timeout)
        } and {
          collection.find(jack2.copy(age = jack2.age + 1)).one[Person].
            aka("incremented") must beSome[Person].await(1, timeout)
        }
    }

    "fail with invalid $$inc clause" in {
      val colName = s"FindAndModifySpec${System identityHashCode this}-4"
      val collection = db(colName)

      val query = BSONDocument("FindAndModifySpecFail" -> BSONDocument(
        f"$$exists" -> false))

      val future = for {
        _ <- collection.insert[Person](ordered = true).one(jack2)
        r <- collection.runCommand(
          FindAndModify(query, Update(BSONDocument("$inc" -> "age"))),
          ReadPreference.Primary)
      } yield r

      future.map(_ => Option.empty[Int]).recover {
        case e: CommandError =>
          //e.printStackTrace
          e.code
      } must beSome( /*code = */ 9).await(1, timeout)
    }
  }
}
