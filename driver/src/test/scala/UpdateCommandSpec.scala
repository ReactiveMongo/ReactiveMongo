package reactivemongo

import reactivemongo.bson.{ BSONArray, BSONBinary, BSONDocument }

import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.api.{
  BSONSerializationPack,
  NodeSetSession,
  SessionTransaction,
  WriteConcern
}

import reactivemongo.api.commands.{
  UpdateCommand,
  ResolvedCollectionCommand,
  WriteConcern => WC
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
        writer(None, MongoWireVersion.V26)(
          update1) must_=== (base ++ writeConcern)
      }

      "with session" in {
        val write = writer(Some(session), MongoWireVersion.V26)

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

  private lazy val elements1 = Command.UpdateElement(
    q = BSONDocument("_id" -> 1),
    u = BSONDocument(f"$$set" -> BSONDocument("value" -> 1)),
    upsert = true,
    multi = false)

  private lazy val elements2 = Command.UpdateElement(
    q = BSONDocument("value" -> 2),
    u = BSONDocument(f"$$set" -> BSONDocument("label" -> "two")),
    upsert = false,
    multi = true)

  private lazy val update1 = ResolvedCollectionCommand(
    collection = "foo",
    command = Command.Update(
      updates = Seq(elements1, elements2),
      ordered = true,
      writeConcern = WC.Default))

  private object Command extends UpdateCommand[BSONSerializationPack.type] {
    val pack = BSONSerializationPack
  }
}
