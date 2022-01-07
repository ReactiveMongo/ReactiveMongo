package reactivemongo

import reactivemongo.api.bson.BSONDocument.pretty
import reactivemongo.api.bson.{ BSONArray, BSONDocument }
import reactivemongo.api.commands.{ DeleteCommand, ResolvedCollectionCommand }
import reactivemongo.api.{ PackSupport, Session, WriteConcern }

final class DeleteCommandSpec extends org.specs2.mutable.Specification {
  "Delete command".title

  section("unit")
  "Delete command" should {
    "be written" >> {

      val base: BSONDocument = BSONDocument(
        "delete" -> "foo",
        "ordered" -> true)

      val writeConcern = BSONDocument(
        "writeConcern" -> BSONDocument(
          "w" -> 1,
          "j" -> false))

      def deletes(xs: Seq[BSONDocument]): BSONDocument = BSONDocument(
        "deletes" -> BSONArray(xs))

      // ---

      "with correct `deletes` elements" in {
        val cmd = new Command(None)
        val element = new cmd.DeleteElement(
          _q = BSONDocument("_id" -> 1),
          _limit = 1,
          _collation = None)
        val bson = BSONDocument(
          "q" -> BSONDocument("_id" -> 1),
          "limit" -> 1)

        withDelete(cmd)(element :: Nil) { delete =>
          cmd.pack.serialize(delete, cmd.deleteWriter) must_=== (base ++ writeConcern ++ deletes(bson :: Nil))
        }
      }
    }
  }
  section("unit")

  // ---

  private def withDelete[T](cmd: Command)(deletes: Seq[cmd.DeleteElement])(f: cmd.DeleteCmd => T): T = {
    f(new ResolvedCollectionCommand(
      collection = "foo",
      command = new cmd.Delete(
        deletes = deletes,
        ordered = true,
        writeConcern = WriteConcern.Default)))
  }

  import reactivemongo.api.tests.Pack

  private final class Command(s: Option[Session])
    extends PackSupport[Pack] with DeleteCommand[Pack] {

    override private[reactivemongo] def session(): Option[Session] = s

    override val pack: Pack = reactivemongo.api.tests.pack
  }
}
