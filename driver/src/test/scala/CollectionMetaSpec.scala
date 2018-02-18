import reactivemongo.api._, collections.bson.BSONCollection
import reactivemongo.bson._

import org.specs2.concurrent.ExecutionEnv

class CollectionMetaSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification {

  "Collection meta operations" title

  import reactivemongo.api.commands.CommandError.{ Code, Message }
  import reactivemongo.api.collections.bson._
  import Common._

  sequential

  val colName = s"collmeta${System identityHashCode this}"
  lazy val collection = db(colName)
  lazy val slowColl = slowDb(colName)

  "BSON collection" should {
    "be created" in {
      collection.create() must beTypedEqualTo({}).await(1, timeout)
    }

    {
      def renameSpec(_db: DefaultDB) =
        "be renamed with failure" in {
          _db(colName).rename("renamed").map(_ => false).recover({
            case Code(13) & Message(msg) if (
              msg contains "renameCollection ") => true
            case _ => false
          }) must beTrue.await(1, timeout)
        }

      "with the default connection" >> renameSpec(db)
      "with the slow connection" >> renameSpec(slowDb)
    }

    {
      def dropSpec(_db: DefaultDB) = {
        "be dropped successfully if exists (deprecated)" in {
          val col = _db(s"foo_${System identityHashCode _db}")

          col.create().flatMap(_ => col.drop(false)).
            aka("legacy drop") must beTrue.await(1, timeout)
        }

        "be dropped with failure if doesn't exist (deprecated)" in {
          val col = _db(s"foo_${System identityHashCode _db}")

          col.drop() aka "legacy drop" must throwA[Exception].like {
            case Code(26) => ok
          }.await(1, timeout)
        }

        "be dropped successfully if exist" in {
          val col = _db(s"foo1_${System identityHashCode _db}")

          col.create().flatMap(_ => col.drop(false)).
            aka("dropped") must beTrue.await(1, timeout)
        }

        "be dropped successfully if doesn't exist" in {
          val col = _db(s"foo2_${System identityHashCode _db}")

          col.drop(false) aka "dropped" must beFalse.await(1, timeout)
        }
      }

      "with the default connection" >> dropSpec(db)
      "with the slow connection" >> dropSpec(db)
    }
  }

  @inline def findAll(c: BSONCollection) = c.find(BSONDocument())

  object & {
    def unapply[T](any: T): Option[(T, T)] = Some(any -> any)
  }
}
