import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.{ FailoverStrategy, MongoConnection }
import reactivemongo.api.commands.CommandError

import org.specs2.concurrent.ExecutionEnv

class DatabaseSpec(implicit ee: ExecutionEnv)
  extends org.specs2.mutable.Specification {

  "Database" title

  sequential

  import Common._

  "Database" should {
    "be resolved from connection according the failover strategy" >> {
      "successfully" in {
        val fos = FailoverStrategy(FiniteDuration(50, "ms"), 20, _ * 2D)

        Common.connection.database(Common.commonDb, fos).
          map(_ => {}) must beEqualTo({}).await(1, estTimeout(fos))

      }

      "with failure" in {
        lazy val con = Common.driver.connection(List("unavailable:27017"))
        val ws = scala.collection.mutable.ListBuffer.empty[Int]
        val expected = List(2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40)
        val fos1 = FailoverStrategy(FiniteDuration(50, "ms"), 20,
          { n => val w = n * 2; ws += w; w.toDouble })
        val fos2 = FailoverStrategy(FiniteDuration(50, "ms"), 20,
          _ * 2 toDouble) // without accumulator

        val before = System.currentTimeMillis()
        val estmout = estTimeout(fos2)

        con.database("foo", fos1).map(_ => List.empty[Int]).
          recover({ case _ => ws.result() }) must beEqualTo(expected).
          await(0, estmout * 2) and {
            val duration = System.currentTimeMillis() - before

            duration must be_<(estmout.toMillis + 750 /* ms */ )
          }
      } tag "unit"
    }

    section("mongo2", "mongo24", "not_mongo26")
    "fail with MongoDB < 2.6" in {

      import reactivemongo.core.errors.ConnectionException

      Common.connection.database(Common.commonDb, failoverStrategy).
        map(_ => {}) aka "database resolution" must (
          throwA[ConnectionException]("unsupported MongoDB version")).
          await(0, timeout) and {
            Await.result(Common.connection.database(Common.commonDb), timeout).
              aka("database") must throwA[ConnectionException](
                "unsupported MongoDB version")
          }
    }
    section("mongo2", "mongo24", "not_mongo26")

    "admin" >> {
      "rename successfully collection if target doesn't exist" in {
        (for {
          admin <- connection.database("admin", failoverStrategy)
          name1 <- {
            val name = s"foo_${System identityHashCode admin}"

            db.collection(name).create().map(_ => name)
          }
          name = s"renamed_${System identityHashCode name1}"
          c2 <- admin.renameCollection(db.name, name1, name)
        } yield name -> c2.name) must beLike[(String, String)] {
          case (expected, name) => name aka "new name" must_== expected
        }.await(0, timeout)
      }

      "fail to rename collection if target exists" in {
        val c1 = db.collection(s"foo_${System identityHashCode ee}")

        (for {
          _ <- c1.create()
          name = s"renamed_${System identityHashCode c1}"
          c2 = db.collection(name)
          _ <- c2.create()
        } yield name) must beLike[String] {
          case name => name must not(beEqualTo(c1.name)) and {
            Await.result(for {
              admin <- connection.database("admin", failoverStrategy)
              _ <- admin.renameCollection(db.name, c1.name, name)
            } yield {}, timeout) must throwA[Exception].like {
              case err: CommandError =>
                err.errmsg aka err.toString must beSome[String].which {
                  _.indexOf("target namespace exists") != -1
                }
            }
          }
        }.await(0, timeout)
      }
    }

    "be hashed" in {
      import reactivemongo.api.commands.DBHashResult

      reactivemongo.api.tests.dbHash(db) must beLike[DBHashResult] {
        case hash => hash.host must not(beEmpty[String]) and {
          hash.md5 must not(beEmpty[String])
        } and {
          hash.collectionHashes must not(beEmpty[Map[String, String]])
        }
      }.await(1, timeout)
    }

    {
      val dbName = s"databasespec-${System identityHashCode ee}"

      def dropSpec(con: MongoConnection, timeout: FiniteDuration) =
        con.database(dbName).flatMap(_.drop()).
          aka("drop") must beTypedEqualTo({}).await(2, timeout)

      "be dropped with the default connection" in {
        dropSpec(connection, timeout)
      }

      "be dropped with the slow connection" in {
        dropSpec(slowConnection, slowTimeout)
      }
    }
  }
}
