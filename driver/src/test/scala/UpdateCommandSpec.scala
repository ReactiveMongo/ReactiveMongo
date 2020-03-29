package reactivemongo

import reactivemongo.api.bson.{ BSONArray, BSONBinary, BSONDocument }
import reactivemongo.api.bson.collection.BSONSerializationPack

import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.api.{ NodeSetSession, SessionTransaction, WriteConcern }

import reactivemongo.api.commands.{
  UpdateCommand,
  ResolvedCollectionCommand
}

final class UpdateCommandSpec extends org.specs2.mutable.Specification {
  "Update command" title

  private val writer = UpdateCommand.writer(BSONSerializationPack)(Command)

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
        writer(None, MongoWireVersion.V30)(
          update1) must_=== (base ++ writeConcern)
      }

      "with session" in {
        val write = writer(Some(session), MongoWireVersion.V30)

        // w/o transaction started
        write(update1) must_=== (base ++ lsid ++ writeConcern) and {
          session.startTransaction(WriteConcern.Default, None).
            aka("transaction") must beSuccessfulTry[(SessionTransaction, Boolean)].which { _ =>
              // w/ transaction started

              write(update1) must_=== (base ++ lsid ++ BSONDocument(
                "txnNumber" -> 1L,
                "startTransaction" -> true, // as first command in tx
                "autocommit" -> false))
            }
        } and {
          // w/o 'startTransaction' flag after first command in tx

          write(update1) must_=== (base ++ lsid ++ BSONDocument(
            "txnNumber" -> 1L, "autocommit" -> false))
        }
      }
    }
  }
  section("unit")

  // ---

  private lazy val element1 = new Command.UpdateElement(
    q = BSONDocument("_id" -> 1),
    u = BSONDocument(f"$$set" -> BSONDocument("value" -> 1)),
    upsert = true,
    multi = false,
    collation = None,
    arrayFilters = Seq.empty)

  private lazy val element2 = new Command.UpdateElement(
    q = BSONDocument("value" -> 2),
    u = BSONDocument(f"$$set" -> BSONDocument("label" -> "two")),
    upsert = false,
    multi = true,
    collation = None,
    arrayFilters = Seq.empty)

  private lazy val update1 = new ResolvedCollectionCommand(
    collection = "foo",
    command = new Command.Update(
      ordered = true,
      writeConcern = WriteConcern.Default,
      bypassDocumentValidation = false,
      firstUpdate = element1,
      updates = Seq(element2)))

  private object Command extends UpdateCommand[BSONSerializationPack.type] {
    val pack = BSONSerializationPack
  }
}
