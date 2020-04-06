package reactivemongo

import reactivemongo.api.bson.{ BSONArray, BSONBinary, BSONDocument }

import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.api.{
  NodeSetSession,
  PackSupport,
  Session,
  SessionTransaction,
  WriteConcern
}

import reactivemongo.api.commands.{
  UpdateCommand,
  UpdateWriteResultFactory,
  UpsertedFactory,
  ResolvedCollectionCommand
}

final class UpdateCommandSpec extends org.specs2.mutable.Specification {
  "Update command" title

  //private val writer = UpdateCommand.writer(BSONSerializationPack)(Command)

  section("unit")
  "Update command" should {
    "be written" >> {
      val base = BSONDocument(
        "update" -> "foo",
        "ordered" -> true,
        "updates" -> BSONArray(
          BSONDocument(
            "q" -> BSONDocument("_id" -> 1),
            "u" -> BSONDocument(f"$$set" -> BSONDocument("value" -> 1)),
            "upsert" -> true,
            "multi" -> false),
          BSONDocument(
            "q" -> BSONDocument("value" -> 2),
            "u" -> BSONDocument(f"$$set" -> BSONDocument("label" -> "two")),
            "upsert" -> false,
            "multi" -> true)))

      lazy val session = new NodeSetSession(java.util.UUID.randomUUID())

      val lsid = BSONDocument(
        "lsid" -> BSONDocument(
          "id" -> BSONBinary(session.lsid)))

      val writeConcern = BSONDocument(
        "writeConcern" -> BSONDocument(
          "w" -> 1,
          "j" -> false))

      // ---

      "without session" in {
        val cmd = new Command(None)

        withUpdate(cmd) { update =>
          cmd.pack.serialize(
            update, cmd.updateWriter) must_=== (base ++ writeConcern)
        }
      }

      "with session" in {
        val cmd = new Command(Some(session))

        withUpdate(cmd) { update =>
          val write = cmd.pack.serialize(_: cmd.UpdateCmd, cmd.updateWriter)

          // w/o transaction started
          write(update) must_=== (base ++ lsid ++ writeConcern) and {
            session.startTransaction(WriteConcern.Default, None).
              aka("transaction") must beSuccessfulTry[(SessionTransaction, Boolean)].which { _ =>
                // w/ transaction started

                write(update) must_=== (base ++ lsid ++ BSONDocument(
                  "txnNumber" -> 1L,
                  "startTransaction" -> true, // as first command in tx
                  "autocommit" -> false))
              }
          } and {
            // w/o 'startTransaction' flag after first command in tx

            write(update) must_=== (base ++ lsid ++ BSONDocument(
              "txnNumber" -> 1L, "autocommit" -> false))
          }
        }
      }
    }
  }
  section("unit")

  // ---

  private def withUpdate[T](cmd: Command)(f: cmd.UpdateCmd => T): T = {
    val element1 = new cmd.UpdateElement(
      q = BSONDocument("_id" -> 1),
      u = BSONDocument(f"$$set" -> BSONDocument("value" -> 1)),
      upsert = true,
      multi = false,
      collation = None,
      arrayFilters = Seq.empty)

    val element2 = new cmd.UpdateElement(
      q = BSONDocument("value" -> 2),
      u = BSONDocument(f"$$set" -> BSONDocument("label" -> "two")),
      upsert = false,
      multi = true,
      collation = None,
      arrayFilters = Seq.empty)

    f(new ResolvedCollectionCommand(
      collection = "foo",
      command = new cmd.Update(
        ordered = true,
        writeConcern = WriteConcern.Default,
        bypassDocumentValidation = false,
        firstUpdate = element1,
        updates = Seq(element2))))
  }

  import reactivemongo.api.tests.Pack

  private final class Command(s: Option[Session])
    extends PackSupport[Pack] with UpdateCommand[Pack]
    with UpsertedFactory[Pack] with UpdateWriteResultFactory[Pack] {

    private[reactivemongo] def session(): Option[Session] = s

    protected val maxWireVersion = MongoWireVersion.V30

    val pack = reactivemongo.api.tests.pack
  }
}
