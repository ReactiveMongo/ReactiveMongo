import reactivemongo.api.{ DefaultDB, WriteConcern, tests => apiTests }
import reactivemongo.api.commands.GetLastError

import reactivemongo.api.bson.BSONDocument

trait DBSessionSpec { specs: DatabaseSpec =>
  import tests.Common
  import Common._

  def sessionSpecs = "manage session" >> {
    section("gt_mongo32")

    "start & end" in {
      (for {
        db <- Common.db.startSession()

        // NoOp startSession
        _ <- db.startSession()
        _ <- db.startSession(failIfAlreadyStarted = false)
        _ <- db.startSession(failIfAlreadyStarted = true).failed

        after <- db.endSession()

        // NoOp endSession
        _ <- after.endSession()
        _ <- after.endSession(failIfNotStarted = false)
        _ <- after.endSession(failIfNotStarted = true).failed
      } yield {
        System.identityHashCode(db) -> System.identityHashCode(after)
      }) must beLike[(Int, Int)] {
        case (hash1, hash2) => hash1 must not(beEqualTo(hash2))
      }.awaitFor(timeout)
    }

    "not kill without start" in {
      Common.db.killSession() must beAnInstanceOf[DefaultDB].await
    }

    "start & kill" in {
      Common.db.startSession().flatMap(
        _.killSession()) must beAnInstanceOf[DefaultDB].awaitFor(timeout)

    }

    if (replSetOn) {
      section("ge_mongo4")
      "start & abort transaction" in {
        val colName = s"tx1_${System identityHashCode this}"
        @volatile var database = Option.empty[DefaultDB]

        Common.db.startSession().flatMap { _db =>
          for {
            _ <- _db.startTransaction(None)

            // NoOp
            _ <- _db.startTransaction(None)
            _ <- _db.startTransaction(None, false)
            _ <- _db.startTransaction(None, true).failed

            _ <- _db.collection(colName).create()
          } yield {
            database = Some(_db)
            database
          }
        } must beSome[DefaultDB].awaitFor(timeout) and (
          database must beSome[DefaultDB].which { db =>
            lazy val coll = db.collection(colName)

            def find() = coll.find(
              selector = BSONDocument.empty,
              projection = Option.empty[BSONDocument]).one[BSONDocument]

            apiTests.session(db).flatMap(
              _.transaction.toOption.map(_.txnNumber)) must beSome(1L) and {
                (for {
                  n <- find().map(_.size)

                  // See recover(code=251) in endTransaction
                  s <- db.abortTransaction().map(apiTests.session)

                  // NoOp abort
                  _ <- db.abortTransaction()
                  _ <- db.abortTransaction(failIfNotStarted = false)
                  _ <- db.abortTransaction(failIfNotStarted = true).failed
                } yield s.map(n -> _.transaction.toOption.map(_.txnNumber))).
                  aka("session after tx") must beSome(0 -> Option.empty[Long]).
                  awaitFor(timeout)

              } and {
                // Start a new transaction with the same session
                db.startTransaction(None, failIfAlreadyStarted = false).
                  map(apiTests.session).map {
                    _.flatMap(_.transaction.toOption).map(_.txnNumber)
                  } must beSome(2L).awaitFor(timeout)
              } and {
                // Insert a doc in the transaction,
                // and check the count before & after

                val inserted = BSONDocument("_id" -> 1)

                (for {
                  n1 <- find().map(_.size) // before insert
                  _ <- coll.insert.one(inserted)

                  n2 <- coll.find(
                    selector = inserted,
                    projection = Option.empty[BSONDocument]).
                    one[BSONDocument].map(_.size)

                } yield n1 -> n2) must beTypedEqualTo(0 -> 1).awaitFor(timeout)
              } and {
                // 0 document found outside the transaction
                Common.db.collection(colName).find(
                  selector = BSONDocument.empty,
                  projection = Option.empty[BSONDocument]).
                  one[BSONDocument].map(_.size) must beTypedEqualTo(0).
                  awaitFor(timeout)

              } and {
                // 0 document found in session after transaction is aborted

                db.abortTransaction().map { aborted =>
                  val session = apiTests.session(aborted)

                  session.map { s =>
                    s.lsid.toString -> s.transaction.toOption.map(_.txnNumber)
                  }
                } must beSome[(String, Option[Long])].like {
                  case (_ /*lsid*/ , None /*transaction*/ ) =>
                    find().map(_.size) must beTypedEqualTo(0).awaitFor(timeout)

                }.awaitFor(timeout)
              }
          })
      }

      "cannot abort transaction after session is killed" in {
        (for {
          sdb <- Common.db.startSession()
          tdb <- sdb.startTransaction(None)

          c = sdb.collection(s"session${System identityHashCode sdb}")
          _ <- c.insert.one(BSONDocument("foo" -> 1))
          _ <- c.insert.many(Seq(
            BSONDocument("foo" -> 2), BSONDocument("bar" -> 3)))

          kdb <- tdb.killSession()
          _ <- kdb.abortTransaction(failIfNotStarted = true).failed
        } yield ()) must beTypedEqualTo({}).awaitFor(timeout)
      }

      "cannot commit transaction after session is killed" in {
        (for {
          sdb <- Common.db.startSession()
          tdb <- sdb.startTransaction(None)

          kdb <- tdb.killSession()
          _ <- kdb.commitTransaction(failIfNotStarted = true).failed
        } yield ()) must beTypedEqualTo({}).awaitFor(timeout)
      }

      "start & commit transaction" in {
        val colName = s"tx2_${System identityHashCode this}"
        @volatile var database = Option.empty[DefaultDB]

        Common.db.startSession().flatMap { _db =>
          _db.startTransaction(Some(WriteConcern.Default.copy(
            w = GetLastError.Majority))).flatMap { _ =>
            _db.collection(colName).create().map { _ =>
              database = Some(_db); database
            }
          }
        } must beSome[DefaultDB].awaitFor(timeout) and (
          database must beSome[DefaultDB].which { db =>
            lazy val coll = db.collection(colName)

            def find() = coll.find(
              selector = BSONDocument.empty,
              projection = Option.empty[BSONDocument]).one[BSONDocument]

            find().map(_.size) must beTypedEqualTo(0).awaitFor(timeout) and {
              coll.insert.one(BSONDocument("_id" -> 1)).
                map(_ => {}) must beTypedEqualTo({}).awaitFor(timeout)
            } and {
              // 1 document found in transaction after insert
              find().map(_.size) must beTypedEqualTo(1).awaitFor(timeout)
            } and {
              // 0 document found outside transaction
              Common.db.collection(colName).find(
                selector = BSONDocument("_id" -> 1),
                projection = Option.empty[BSONDocument]).
                one[BSONDocument].map(_.size).
                aka("found") must beTypedEqualTo(0).awaitFor(timeout)
            } and {
              coll.insert.many(Seq(
                BSONDocument("_id" -> 2), BSONDocument("_id" -> 3))).
                map(_.n) must beTypedEqualTo(2).awaitFor(timeout)
            } and {
              coll.count() must beTypedEqualTo(3).awaitFor(timeout)
            } and {
              db.commitTransaction().
                aka("commited") must beAnInstanceOf[DefaultDB].awaitFor(timeout)

            } and {
              // 1 document found outside transaction after commit

              Common.db.collection(colName).find(
                selector = BSONDocument("_id" -> 1),
                projection = Option.empty[BSONDocument]).
                one[BSONDocument].map(_.size).
                aka("found") must beTypedEqualTo(1).awaitFor(timeout)
            }
          })
      }
      section("ge_mongo4")
    } // end: replSetOn

    section("gt_mongo32")
  }
}
