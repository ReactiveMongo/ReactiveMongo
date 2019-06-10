import scala.concurrent.Future

import reactivemongo.api.{ DefaultDB, WriteConcern, tests => apiTests }
import reactivemongo.api.commands.GetLastError

import reactivemongo.bson.BSONDocument

trait DBSessionSpec { specs: DatabaseSpec =>
  import tests.Common
  import Common._

  def sessionSpecs = "manage session" >> {
    section("gt_mongo32")

    "start & end" in {
      (for {
        Some(db) <- Common.db.startSession()
        _ <- db.startSession() // no-op
        Some(after) <- db.endSession()
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
      Common.db.startSession().collect {
        case Some(db) => db
      }.flatMap(
        _.killSession()) must beAnInstanceOf[DefaultDB].awaitFor(timeout)

    }

    if (replSetOn) {
      section("ge_mongo4")
      "start & abort transaction" in {
        val colName = s"tx1_${System identityHashCode this}"
        @volatile var database = Option.empty[DefaultDB]

        Common.db.startSession().flatMap {
          case Some(_db) => _db.startTransaction(None) match {
            case Some(_) =>
              _db.collection(colName).create().map { _ =>
                database = Some(_db); database
              }

            case _ =>
              Future.successful(Option.empty[DefaultDB])
          }

          case _ => Future.successful(Option.empty[DefaultDB])
        } must beSome[DefaultDB].awaitFor(timeout) and (
          database must beSome[DefaultDB].which { db =>
            lazy val coll = db.collection(colName)

            def find() = coll.find(
              selector = BSONDocument.empty,
              projection = Option.empty[BSONDocument]).one[BSONDocument]

            apiTests.session(db).flatMap(
              _.transaction.map(_.txnNumber)) must beSome(1L) and {
                (for {
                  n <- find().map(_.size)

                  // See recover(code=251) in endTransaction
                  aborted <- db.abortTransaction()
                  s = aborted.flatMap(apiTests.session)
                } yield s.map(n -> _.transaction.map(_.txnNumber))).
                  aka("session after tx") must beSome(0 -> Option.empty[Long]).
                  awaitFor(timeout)

              } and {
                // Start a new transaction with the same session
                (for {
                  s <- db.startTransaction(None).flatMap(apiTests.session)
                  txn <- s.transaction.map(_.txnNumber)
                } yield txn) must beSome(2L)
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
                  val session = aborted.flatMap(apiTests.session)

                  session.map { s =>
                    s.lsid.toString -> s.transaction.map(_.txnNumber)
                  }
                } must beSome[(String, Option[Long])].like {
                  case (_ /*lsid*/ , None /*transaction*/ ) =>
                    find().map(_.size) must beTypedEqualTo(0).awaitFor(timeout)

                }.awaitFor(timeout)
              }
          })
      }

      "cannot abort transaction after session is killed" in {
        Common.db.startSession().map(_.flatMap { db =>
          db.startTransaction(None).map(_ => db)
        }) must beSome[DefaultDB].which { db =>
          db.killSession().flatMap(_.abortTransaction()).
            aka("aborted") must beNone.awaitFor(timeout)
        }.awaitFor(timeout)
      }

      "cannot commit transaction after session is killed" in {
        Common.db.startSession().map(_.flatMap { db =>
          db.startTransaction(None).map(_ => db)
        }) must beSome[DefaultDB].which { db =>
          db.killSession().flatMap(_.commitTransaction()).
            aka("aborted") must beNone.awaitFor(timeout)
        }.awaitFor(timeout)
      }

      "start & commit transaction" in {
        val colName = s"tx2_${System identityHashCode this}"
        @volatile var database = Option.empty[DefaultDB]

        Common.db.startSession().flatMap {
          case Some(_db) => _db.startTransaction(Some(WriteConcern.Default.copy(
            w = GetLastError.Majority))) match {
            case Some(_) =>
              _db.collection(colName).create().map { _ =>
                database = Some(_db); database
              }

            case _ =>
              Future.successful(Option.empty[DefaultDB])
          }

          case _ => Future.successful(Option.empty[DefaultDB])
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
              db.commitTransaction() must beSome[DefaultDB].awaitFor(timeout)
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
