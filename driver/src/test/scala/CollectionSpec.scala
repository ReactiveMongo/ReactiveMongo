import scala.concurrent._, duration.FiniteDuration

import reactivemongo.api.bson.BSONDocument

import reactivemongo.api.{ Cursor, DefaultDB, ReadConcern, ReadPreference }

import reactivemongo.api.commands.{ CommandError, WriteConcern }

import reactivemongo.api.collections.Hint

import org.specs2.concurrent.ExecutionEnv

import _root_.tests.Common

import reactivemongo.api.TestCompat._
import reactivemongo.api.tests.{ builder, decoder, pack, reader, writer }

final class CollectionSpec(implicit protected val ee: ExecutionEnv)
  extends org.specs2.mutable.Specification
  with org.specs2.specification.AfterAll
  with UpdateSpec with CollectionMetaSpec with CollectionFixtures {

  "Collection" title

  sequential

  // ---

  import Common.{ timeout, slowTimeout }

  lazy val (db, slowDb) = Common.databases(s"reactivemongo-${System identityHashCode this}", Common.connection, Common.slowConnection)

  def afterAll = { db.drop(); () }

  // ---

  "BSON collection" should {
    "support creation" >> {
      "successfully when not exist" in {
        collection.create() must beTypedEqualTo({}).awaitFor(timeout)
      }

      "with error when already exists (failsIfExists = true)" in {
        def test(create: => Future[Unit]): Future[Boolean] =
          create.map(_ => true).recover {
            case CommandError.Code(48 /*already exists */ ) => false

            case CommandError.Message(
              "collection already exists") => false
          }

        test(collection.create()) must beFalse.awaitFor(timeout) and {
          test(collection.create(
            failsIfExists = true)) must beFalse.awaitFor(timeout)
        }
      }

      "successfully when already exists (failsIfExists = false)" in {
        collection.create(
          failsIfExists = false) must beTypedEqualTo({}).awaitFor(timeout)
      }
    }

    "expose stats" in {
      db.collection(s"not_exists_${System identityHashCode collection}").
        stats().map(
          _.size > 0 /* since 4.2 stats are returns anyway */ ).recover {
            case _ => false
          } must beFalse.awaitFor(timeout) and {
            collection.stats().
              map(s => s.ns -> s.count) must beLike[(String, Int)] {
                case (ns, 0) => ns.endsWith(collection.name) must beTrue
              }.awaitFor(timeout)
          }
    }

    "write successfully 5 documents" >> {
      implicit val writer = PersonWriter

      "with insert" in {
        collection.insert.
          one(person).map(_.ok) must beTrue.await(1, timeout) and {
            val coll = slowColl.withReadPreference(ReadPreference.secondary)

            coll.readPreference must_=== ReadPreference.secondary and {
              // Anyway use ReadPreference.Primary for insert op
              coll.insert.one(person2).map { r =>
                r.ok -> r.n
              } must beTypedEqualTo(true -> 1).await(1, timeout)
            }
          } and {
            slowColl.find(BSONDocument.empty).cursor[BSONDocument]().
              collect[List](-1, Cursor.FailOnError[List[BSONDocument]]()).
              map(_.size) must beTypedEqualTo(2).await(1, slowTimeout)
          }
      }

      "with bulkInsert" in {
        val persons = Seq(person3, person4, person5)

        collection.insert(ordered = true).many(persons).map(_.ok).
          aka("insertion") must beTrue.await(1, timeout)
      }
    }

    "count the inserted documents" in {
      def count(
        selector: Option[BSONDocument] = None,
        limit: Option[Int] = None,
        skip: Int = 0,
        hint: Option[Hint[pack.type]] = None,
        readConcern: ReadConcern = ReadConcern.Local) =
        collection.count(selector, limit, skip, hint, readConcern)

      count() must beTypedEqualTo(5L).await(1, timeout) and {
        count(skip = 1) must beTypedEqualTo(4L).await(1, timeout)
      } and {
        count(selector = Some(BSONDocument("name" -> "Jack"))).
          aka("matching count") must beTypedEqualTo(1L).await(1, timeout)
      } and {
        count(selector = Some(BSONDocument("name" -> "Foo"))).
          aka("not matching count") must beTypedEqualTo(0L).await(1, timeout)
      }
    }

    "read cursor" >> {
      @inline def cursor: Cursor[BSONDocument] =
        collection.withReadPreference(ReadPreference.secondaryPreferred).
          find(BSONDocument("plop" -> "plop")).cursor[BSONDocument]()

      "with maxDocs=0" in {
        collection.find(BSONDocument.empty).cursor[BSONDocument]().
          collect[List](
            maxDocs = 0,
            err = Cursor.FailOnError[List[BSONDocument]]()).
            aka("result") must beTypedEqualTo(List.empty[BSONDocument]).
            awaitFor(timeout)
      }

      "use read preference from the collection" in {
        import scala.language.reflectiveCalls
        val withPref = cursor.asInstanceOf[{ def preference: ReadPreference }]

        withPref.preference must_== ReadPreference.secondaryPreferred
      }

      "when empty with success using collect" in {
        cursor.collect[Vector](10, Cursor.FailOnError[Vector[BSONDocument]]()).
          map(_.length) must beTypedEqualTo(0).await(1, timeout)
      }

      "successfully with 'name' projection using collect" in {
        collection.find(BSONDocument("age" -> 25), BSONDocument("name" -> 1)).
          one[BSONDocument] must beSome[BSONDocument].which { doc =>
            doc.elements.size must_=== 2 /* _id+name */ and {
              decoder.string(doc, "name") aka "name" must beSome("Jack")
            }
          }.await(1, timeout)
      }

      "explain query result" >> {
        "when MongoDB > 2.6" in {
          findAll(collection).explain().one[BSONDocument].
            aka("explanation") must beSome[BSONDocument].which { result =>
              decoder.child(result, "queryPlanner").
                aka("queryPlanner") must beSome and {
                  decoder.child(result, "executionStats").
                    aka("stats") must beSome
                } and {
                  decoder.child(result, "serverInfo").
                    aka("serverInfo") must beSome
                }
            }.await(1, timeout)
        } tag "not_mongo26"

        "when MongoDB = 2.6" in {
          findAll(collection).explain().one[BSONDocument].
            aka("explanation") must beSome[BSONDocument].which { result =>
              decoder.children(result, "allPlans").
                aka("plans") must beLike[List[BSONDocument]] {
                  case _ => decoder.string(result, "server").
                    aka("server") must beSome[String]
                }
            }.await(1, timeout)
        } tag "mongo2"
      }
    }

    "read until John" in {
      implicit val reader = PersonReader
      @inline def cursor = findAll(collection).sort(
        BSONDocument("age" -> 1)).cursor[Person]()

      val persons = Seq(person2, person4, person, person3)

      cursor.foldWhile(Nil: Seq[Person])({ (s, p) =>
        if (p.name == "John") Cursor.Done(s :+ p)
        else Cursor.Cont(s :+ p)
      }, (_, e) => Cursor.Fail(e)) must beEqualTo(persons).await(1, timeout)
    }

    "read a document with error" in {
      implicit val reader = BuggyPersonReader
      val future = findAll(collection).one[Person].map(_ => 0).recover {
        case e if e.getMessage == "hey hey hey" => -1
        case _ =>
          /* e.printStackTrace(); */ -2
      }

      future must beTypedEqualTo(-1).await(1, timeout)
    }

    {
      def cursorSpec(c: DefaultCollection, timeout: FiniteDuration) = {
        implicit val reader = SometimesBuggyPersonReader()
        @inline def cursor = findAll(c).cursor[Person]()

        "using collect" in {
          val collect = cursor.
            collect(-1, Cursor.FailOnError[Vector[Person]]()).
            map(_.size).recover {
              case e if e.getMessage == "hey hey hey" => -1
              case _ =>
                /* e.printStackTrace() */ -2
            }

          collect aka "first collect" must not(throwA[Exception]).
            await(1, timeout) and (collect must beEqualTo(-1).await(1, timeout))
        }

        "using foldWhile" in {
          cursor.foldWhile(0)(
            (i, _) => Cursor.Cont(i + 1),
            (_, e) => Cursor.Fail(e)) must throwA[CustomException].
            await(1, timeout)
        }

        "fallbacking to final value using foldWhile" in {
          cursor.foldWhile(0)(
            (i, _) => Cursor.Cont(i + 1),
            (_, _) => Cursor.Done(-1)) must beEqualTo(-1).await(1, timeout)
        }

        "skiping failure using foldWhile" in {
          cursor.foldWhile(0)(
            (i, _) => Cursor.Cont(i + 1),
            (_, _) => Cursor.Cont(-3)) must beEqualTo(-2).await(1, timeout)
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
      // TODO: Move to CursorSpec?
      implicit val reader = SometimesBuggyPersonReader()

      def resultSpec(c: DefaultCollection, timeout: FiniteDuration) =
        findAll(c).cursor[Person]().collect[Vector](
          Int.MaxValue, Cursor.ContOnError[Vector[Person]]()).
          map(_.length) must beTypedEqualTo(4).await(1, timeout)

      "with the default connection" in {
        resultSpec(collection, timeout)
      }

      "with the slow connection" in {
        resultSpec(slowColl, slowTimeout)
      }
    }

    "write a document with error" >> {
      implicit val writer = BuggyPersonWriter
      def writeSpec(c: DefaultCollection, timeout: FiniteDuration) =
        c.insert.one(person).map { _ /*lastError*/ =>
          //println(s"person write succeed??  $lastError")
          0
        }.recover {
          case _: CustomException => -1
          case e =>
            e.printStackTrace()
            -2
        } aka "write result" must beEqualTo(-1).await(1, timeout)

      "with the default connection" in {
        writeSpec(collection, timeout)
      }

      "with the slow connection" in {
        writeSpec(slowColl, slowTimeout)
      }
    }

    "write a JavaScript value" in {
      val selector = BSONDocument("age" -> 101)
      def find = collection.find(selector).one[BSONDocument]

      collection.insert.one(BSONDocument(
        "age" -> 101,
        "name" -> BSONJavaScript("db.getName()"))).flatMap { _ =>

        find.map(_.flatMap {
          decoder.value[BSONJavaScript](_, "name")
        }.map(_.value))
      } aka "inserted" must beSome("db.getName()").await(1, timeout) and {
        collection.delete().one(selector).map(_.n) must beTypedEqualTo(1).
          await(1, timeout)
      } and {
        find must beNone.await(1, timeout)
      }
    }

    { // Find & update
      implicit val reader = PersonReader
      implicit val writer = PersonWriter
      // TODO: Move to FindAndModifySpec

      def findAndUpdateSpec(c: DefaultCollection, timeout: FiniteDuration, five: Person = person5) = {
        "by updating age of 'Joline', & returns the old document" in {
          val updateOp = c.updateModifier(
            BSONDocument("$set" -> BSONDocument("age" -> 35)))

          c.findAndModify(
            selector = BSONDocument("name" -> "Joline"),
            modifier = updateOp,
            sort = None,
            fields = None,
            bypassDocumentValidation = false,
            writeConcern = WriteConcern.Default,
            maxTime = None,
            collation = None,
            arrayFilters = Seq.empty).map(_.result[Person]) must beSome(five).await(1, timeout)
        }

        "by updating age of 'James', & returns the updated document" in {
          c.findAndUpdate(
            BSONDocument("name" -> "James"), person2.copy(age = 17),
            fetchNewObject = true).map(_.result[Person]).
            aka("result") must beSome(person2.copy(age = 17)).await(1, timeout)
        }

        "by inserting a new 'Foo' person (with upsert = true)" in {
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

    "manage session" >> {
      section("gt_mongo32")

      import builder.{ elementProducer => elm, int }

      "start & end" in {
        Common.db.startSession() must beLike[DefaultDB] {
          case db =>
            val coll = db.collection(s"session_${System identityHashCode this}")
            val id = System.identityHashCode(db)
            val baseElms = {
              val b = Seq.newBuilder[pack.ElementProducer]
              b += elm("_id", int(id))
            }
            val base = builder.document(baseElms.result())
            val inserted = builder.document(
              { baseElms += elm("value", int(1)) }.result())
            val updated = builder.document(
              { baseElms += elm("value", int(2)) }.result())

            (for {
              _ <- coll.insert(ordered = false).one(inserted)
              r <- coll.find(base).one[BSONDocument]
            } yield r) must beSome(inserted).awaitFor(timeout) and {
              (for {
                _ <- coll.update(false).one(
                  q = base,
                  u = BSONDocument(f"$$set" -> BSONDocument("value" -> 2)),
                  upsert = false,
                  multi = false)

                r <- coll.find(base).one[BSONDocument]
              } yield r) must beSome(updated).awaitFor(timeout)
            } and {
              coll.distinct[Int, List](
                key = "_id",
                selector = None,
                readConcern = ReadConcern.Local,
                collation = None) must beTypedEqualTo(List(id)).
                awaitFor(timeout)

            } and {
              coll.delete(ordered = true).one(base).
                map(_ => {}) must beTypedEqualTo({}).awaitFor(timeout)

            } and {
              coll.count(
                selector = Some(base), hint = None,
                limit = None, skip = 0,
                readConcern = ReadConcern.Local) must beTypedEqualTo(0L).
                awaitFor(timeout)

            } and {
              db.endSession().map(_ => {}) must beEqualTo({}).awaitFor(timeout)
            }
        }.awaitFor(timeout)
      }

      section("gt_mongo32")
    }

    "be managed so that" >> metaSpec

    "use bulks" >> {
      "to insert (including 3 duplicate errors)" >> {
        val nDocs = 1000000
        def colName(n: Int) = s"bulked${System identityHashCode this}_${n}"

        import reactivemongo.api.indexes._
        import reactivemongo.api.indexes.IndexType.Ascending

        def bulkSpec(
          c: DefaultCollection,
          n: Int,
          e: Int,
          timeout: FiniteDuration) = {
          @inline def docs = (0 until n).toStream.map { i =>
            if (i == 0 || i == 1529 || i == 3026 || i == 19862) {
              // duplicate plop -3
              BSONDocument("bulk" -> true, "i" -> i, "plop" -> -3)
            } else BSONDocument("bulk" -> true, "i" -> i, "plop" -> i)
          }

          (for {
            _ <- c.create()
            r <- c.indexesManager.ensure(Index(pack)(
              key = List("plop" -> Ascending),
              name = None,
              unique = true,
              background = false,
              dropDups = false,
              sparse = false,
              expireAfterSeconds = None,
              storageEngine = None,
              weights = None,
              defaultLanguage = None,
              languageOverride = None,
              textIndexVersion = None,
              sphereIndexVersion = None,
              bits = None,
              max = None,
              min = None,
              bucketSize = None,
              collation = None,
              wildcardProjection = None,
              version = None,
              partialFilter = None,
              options = BSONDocument()))
          } yield r) must beTrue.await(1, timeout) and {
            c.insert(ordered = false).many(docs).
              map(_.n) must beTypedEqualTo(e).await(1, timeout * (n / 2L))

          } and {
            c.count(
              selector = Some(BSONDocument("bulk" -> true)),
              limit = None,
              skip = 0,
              hint = None,
              readConcern = ReadConcern.Local).
              aka("count") must beTypedEqualTo(e.toLong).await(1, timeout)
            // all docs minus errors
          }
        }

        s"$nDocs documents with the default connection" in {
          bulkSpec(db(colName(nDocs)), nDocs, nDocs - 3, timeout)
        }

        s"${nDocs / 1000} with the slow connection" in {
          bulkSpec(
            slowDb(colName(nDocs / 1000)),
            nDocs / 1000, nDocs / 1000, slowTimeout)
        }
      }

      {
        lazy val coll = db(s"bulked${System identityHashCode this}_2")

        "to insert" in {
          val docs = Stream.empty[BSONDocument]

          coll.insert(ordered = true).many(docs).
            map(_.n) must beEqualTo(0).await(1, timeout)
        }

        "to update" in {
          val builder = coll.update(ordered = false)

          Future.sequence(Seq(
            builder.element(
              q = BSONDocument("upsert" -> 1),
              u = BSONDocument("i" -> -1, "foo" -> "bar"),
              upsert = true,
              multi = false),
            builder.element(
              q = BSONDocument("i" -> BSONDocument(f"$$lte" -> 3)),
              u = BSONDocument(f"$$set" -> BSONDocument("foo" -> "bar")),
              upsert = false,
              multi = true))).flatMap(builder.many(_)).map { r =>
            (r.n, r.nModified, r.upserted.size)
          } must beEqualTo((2, 0, 1)).await(0, timeout)
        }
      }
    }

    updateSpecs
  }

  @inline def findAll(c: DefaultCollection) = c.find(BSONDocument.empty)
}

sealed trait CollectionFixtures { specs: CollectionSpec =>
  val colName = s"bsoncoll${System identityHashCode this}"
  lazy val collection = db(colName)
  lazy val slowColl = slowDb(colName)

  case class Person(name: String, age: Int)
  case class CustomException(msg: String) extends Exception(msg)

  lazy val BuggyPersonWriter = writer[Person] { _ =>
    throw CustomException("PersonWrite error")
  }

  lazy val BuggyPersonReader = reader[Person] { _ =>
    throw CustomException("hey hey hey")
  }

  def SometimesBuggyPersonReader() = {
    var i = 0

    reader[Person] { doc =>
      i += 1
      if (i % 4 == 0) throw CustomException("hey hey hey")
      else pack.deserialize(doc, PersonReader)
    }
  }

  lazy val PersonWriter = writer[Person] { p =>
    import builder.{ elementProducer => e }

    builder.document(Seq(
      e("age", builder.int(p.age)),
      e("name", builder.string(p.name))))
  }

  lazy val PersonReader = reader[Person] { doc =>
    (for {
      nme <- decoder.string(doc, "name")
      age <- decoder.int(doc, "age")
    } yield Person(nme, age)).get
  }

  val person = Person("Jack", 25)
  val person2 = Person("James", 16)
  val person3 = Person("John", 34)
  val person4 = Person("Jane", 24)
  val person5 = Person("Joline", 34)
}
