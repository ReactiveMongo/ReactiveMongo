package reactivemongo

import reactivemongo.api.{ NodeSetSession, SessionTransaction }
import reactivemongo.api.bson.{ BSONArray, BSONBinary, BSONDocument }
import reactivemongo.api.commands.{ DeleteCommand, ResolvedCollectionCommand }
import reactivemongo.api.{ PackSupport, Session, WriteConcern }

final class DeleteCommandSpec extends org.specs2.mutable.Specification {
  "Delete command".title

  section("unit")
  "Delete command" should {
    "be written" >> {

      val base: BSONDocument =
        BSONDocument("delete" -> "foo", "ordered" -> true)

      val writeConcern =
        BSONDocument("writeConcern" -> BSONDocument("w" -> 1, "j" -> false))

      def deletes(xs: Seq[BSONDocument]): BSONDocument =
        BSONDocument("deletes" -> BSONArray(xs))

      // ---

      "with correct `deletes` elements" in {
        val cmd = new Command(None)
        val element = new cmd.DeleteElement(
          _q = BSONDocument("_id" -> 1),
          _limit = 1,
          _collation = None
        )

        val bson = BSONDocument("q" -> BSONDocument("_id" -> 1), "limit" -> 1)

        withDelete(cmd)(element :: Nil) { delete =>
          cmd.pack.serialize(
            delete,
            cmd.deleteWriter
          ) must_=== (base ++ writeConcern ++ deletes(bson :: Nil))
        }
      }

      lazy val session = new NodeSetSession(java.util.UUID.randomUUID())

      lazy val lsid =
        BSONDocument("lsid" -> BSONDocument("id" -> BSONBinary(session.lsid)))

      "with session" in {
        val cmd = new Command(Some(session))

        val element = new cmd.DeleteElement(
          _q = BSONDocument("_id" -> 1),
          _limit = 1,
          _collation = None
        )

        val bson = BSONDocument("q" -> BSONDocument("_id" -> 1), "limit" -> 1)

        withDelete(cmd)(element :: Nil) { delete =>
          val write = cmd.pack.serialize(_: cmd.DeleteCmd, cmd.deleteWriter)
          val dels = deletes(bson :: Nil)

          // w/o transaction started
          write(delete) must_=== (base ++ lsid ++ writeConcern ++ dels) and {
            session
              .startTransaction(WriteConcern.Default, None)
              .aka("transaction") must beSuccessfulTry[
              (SessionTransaction, Boolean)
            ].which { _ =>
              // w/ transaction started

              write(delete) must_=== (base ++ lsid ++ BSONDocument(
                "txnNumber" -> 1L,
                "startTransaction" -> true, // as first command in tx
                "autocommit" -> false
              ) ++ dels)
            }
          } and {
            // w/o 'startTransaction' flag after first command in tx

            write(delete) must_=== (base ++ lsid ++ BSONDocument(
              "txnNumber" -> 1L,
              "autocommit" -> false
            ) ++ dels)
          }
        }
      }
    }
  }
  section("unit")

  // ---

  private def withDelete[T](
      cmd: Command
    )(deletes: Seq[cmd.DeleteElement]
    )(f: cmd.DeleteCmd => T
    ): T = {
    f(
      new ResolvedCollectionCommand(
        collection = "foo",
        command = new cmd.Delete(
          deletes = deletes,
          ordered = true,
          writeConcern = WriteConcern.Default
        )
      )
    )
  }

  import reactivemongo.api.tests.Pack

  private final class Command(s: Option[Session])
      extends PackSupport[Pack]
      with DeleteCommand[Pack] {

    override private[reactivemongo] def session(): Option[Session] = s

    override val pack: Pack = reactivemongo.api.tests.pack
  }
}
