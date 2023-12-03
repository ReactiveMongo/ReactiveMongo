import scala.concurrent.duration.FiniteDuration

import reactivemongo.api._
import reactivemongo.api.bson.{ BSONDocument, BSONString }
import reactivemongo.api.bson.collection.BSONCollection
import reactivemongo.api.commands.CommandException.Code

// TODO: Separate Spec?
trait CollectionMetaSpec { collSpec: CollectionSpec =>
  import _root_.tests.Common
  import Common._

  def metaSpec = {
    "with the default connection" >> {
      val colName = s"collmeta${System identityHashCode this}"

      "be created" in {
        db(colName).create() must beTypedEqualTo({}).await(1, timeout)
      }

      cappedSpec(db(colName), timeout)

      listSpec(db, timeout)

      "be renamed" >> {
        successfulRename(db, connection, timeout)

        failedRename(db, connection, s"missing-${colName}", timeout)
      }

      dropSpec(db, colName, timeout)
    }

    "with the slow connection" >> {
      val colName = s"slowmeta${System identityHashCode db}"

      "be created" in {
        slowDb(colName).create() must beTypedEqualTo({}).await(1, slowTimeout)
      }

      cappedSpec(slowDb(colName), slowTimeout)

      listSpec(slowDb, slowTimeout)

      "be renamed" >> {
        successfulRename(slowDb, slowConnection, slowTimeout)

        failedRename(slowDb, slowConnection, s"missing-${colName}", slowTimeout)
      }

      dropSpec(slowDb, colName, slowTimeout)
    }
  }

  // ---

  val cappedMaxSize: Long = 2 * 1024 * 1024

  def cappedSpec(c: BSONCollection, timeout: FiniteDuration) =
    "be capped" >> {
      "after conversion" in {
        c.convertToCapped(cappedMaxSize, None) must beEqualTo({})
          .await(1, timeout)
      }

      "with statistics (MongoDB >= 3.0)" in {
        c.stats() must beLike[CollectionStats] {
          case stats =>
            stats.capped must beTrue and (stats.maxSize must beSome(
              cappedMaxSize
            ))
        }.await(1, timeout)
      }
    }

  def successfulRename(
      _db: DB,
      c: MongoConnection,
      timeout: FiniteDuration
    ) =
    "successfully" in {
      val coll = _db.collection(s"foo_${System identityHashCode timeout}")

      (for {
        adminDb <- c.database("admin")
        _ <- coll.create()
        _ <- adminDb.renameCollection(
          _db.name,
          coll.name,
          s"renamed${System identityHashCode coll}"
        )

      } yield ()) must beTypedEqualTo({}).await(0, timeout)
    }

  def failedRename(
      _db: DB,
      c: MongoConnection,
      colName: String,
      timeout: FiniteDuration
    ) = "with failure" in {
    (for {
      adminDb <- c.database("admin")
      _ <- adminDb.renameCollection(_db.name, colName, "renamed")
    } yield false).recover({
      case Code(c) => c == 26 // source doesn't exist
    }) must beTrue.await(1, timeout)
  }

  def listSpec(db2: DB, timeout: FiniteDuration) = "be listed" in {
    val doc = BSONDocument("foo" -> BSONString("bar"))
    val name1 = s"collection_one${System identityHashCode doc}"
    val name2 = s"collection_two${System identityHashCode doc}"

    def i1 = db2(name1).insert.one(doc).map(_.n)

    def i2 = db2(name2).insert.one(doc).map(_.n)

    i1 aka "insert #1" must beTypedEqualTo(1).await(1, timeout) and {
      i2 aka "insert #2" must beTypedEqualTo(1).await(1, timeout)
    } and {
      db2.collectionNames must contain(atLeast(name1, name2)).await(2, timeout)
      // ... as the concurrent tests could create other collections
    }
  }

  def dropSpec(_db: DB, name: String, timeout: FiniteDuration) = {
    def col = _db(name)

    "be dropped successfully if exist" in {
      col.drop(false) aka "legacy drop" must beTrue.await(1, timeout) and {
        col.create().flatMap(_ => col.drop(false)).aka("dropped") must beTrue
          .await(1, timeout)
      }
    }

    "be dropped successfully if doesn't exist" in {
      col.drop(false) aka "dropped" must beFalse.await(1, timeout)
    } tag "lt_mongo7"
  }

  object & {
    def unapply[T](any: T): Option[(T, T)] = Some(any -> any)
  }
}
