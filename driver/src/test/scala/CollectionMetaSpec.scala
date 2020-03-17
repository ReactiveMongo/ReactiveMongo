import scala.concurrent.duration.FiniteDuration

import reactivemongo.api._

import reactivemongo.api.commands.CollStatsResult
import reactivemongo.api.commands.CommandError.{ Code, Message }

import reactivemongo.api.bson.{ BSONDocument, BSONString }
import reactivemongo.api.bson.collection.BSONCollection

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
        successfulRename(connection, timeout)

        failedRename(db, colName, timeout)
      }

      dropSpec(db, colName, timeout)
    }

    "with the slow connection" >> {
      val colName = s"collmeta${System identityHashCode db}"

      "be created" in {
        slowDb(colName).create() must beTypedEqualTo({}).
          await(1, slowTimeout)
      }

      cappedSpec(slowDb(colName), slowTimeout)

      listSpec(slowDb, slowTimeout)

      "be renamed" >> {
        successfulRename(slowConnection, slowTimeout)

        failedRename(slowDb, colName, slowTimeout)
      }

      dropSpec(slowDb, colName, slowTimeout)
    }
  }

  // ---

  val cappedMaxSize: Long = 2 * 1024 * 1024

  def cappedSpec(c: BSONCollection, timeout: FiniteDuration) =
    "be capped" >> {
      "after conversion" in {
        c.convertToCapped(cappedMaxSize, None) must beEqualTo({}).
          await(1, timeout)
      }

      "with statistics (MongoDB <= 2.6)" in {
        c.stats must beLike[CollStatsResult] {
          case stats => stats.capped must beTrue and (stats.maxSize must beNone)
        }.await(1, timeout)
      } tag "mongo2"

      "with statistics (MongoDB >= 3.0)" >> {
        c.stats must beLike[CollStatsResult] {
          case stats => stats.capped must beTrue and (
            stats.maxSize must beSome(cappedMaxSize))
        }.await(1, timeout)
      } tag "not_mongo26"
    }

  def successfulRename(c: MongoConnection, timeout: FiniteDuration) =
    "successfully" in {
      (for {
        coll <- c.database("admin").map(
          _(s"foo_${System identityHashCode timeout}"))

        _ <- coll.create()
        _ <- coll.rename(s"renamed${System identityHashCode coll}")
      } yield ()) aka "renaming" must beEqualTo({}).await(0, timeout)
    }

  def failedRename(_db: DefaultDB, colName: String, timeout: FiniteDuration) =
    "with failure" in {
      _db(colName).rename("renamed").map(_ => false).recover({
        case Code(13) & Message(msg) if (
          msg contains "renameCollection ") => true
        case _ => false
      }) must beTrue.await(1, timeout)
    }

  def listSpec(db2: DefaultDB, timeout: FiniteDuration) = "be listed" in {
    val doc = BSONDocument("foo" -> BSONString("bar"))
    val name1 = s"collection_one${System identityHashCode doc}"
    val name2 = s"collection_two${System identityHashCode doc}"

    def i1 = db2(name1).insert.one(doc).map(_.ok)

    def i2 = db2(name2).insert.one(doc).map(_.ok)

    i1 aka "insert #1" must beTrue.await(1, timeout) and {
      i2 aka "insert #2" must beTrue.await(1, timeout)
    } and {
      db2.collectionNames must contain(atLeast(name1, name2)).await(2, timeout)
      // ... as the concurrent tests could create other collections
    }
  }

  def dropSpec(_db: DefaultDB, name: String, timeout: FiniteDuration) = {
    def col = _db(name)

    "be dropped successfully if exists (deprecated)" in {
      col.drop(false) aka "legacy drop" must beTrue.await(1, timeout)
    }

    "be dropped with failure if doesn't exist (deprecated)" in {
      col.drop() aka "legacy drop" must throwA[Exception].like {
        case Code(26) => ok
      }.await(1, timeout)
    }

    "be dropped successfully if exist" in {
      col.create().flatMap(_ => col.drop(false)).
        aka("dropped") must beTrue.await(1, timeout)
    }

    "be dropped successfully if doesn't exist" in {
      col.drop(false) aka "dropped" must beFalse.await(1, timeout)
    }
  }

  object & {
    def unapply[T](any: T): Option[(T, T)] = Some(any -> any)
  }
}
