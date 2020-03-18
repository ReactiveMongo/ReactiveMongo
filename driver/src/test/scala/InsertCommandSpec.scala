package reactivemongo

import reactivemongo.bson.{ BSONBinary, BSONDocument }

import reactivemongo.api.{
  BSONSerializationPack,
  NodeSetSession,
  SessionTransaction,
  WriteConcern
}

import reactivemongo.api.commands.{
  InsertCommand,
  ResolvedCollectionCommand,
  WriteConcern => WC
}

final class InsertCommandSpec extends org.specs2.mutable.Specification {
  "Insert command" title

  private val writer = InsertCommand.writer(BSONSerializationPack)(Command)

  section("unit")
  "Insert command" should {
    "be written" >> {
      val base = BSONDocument(
        "insert" -> "foo",
        "ordered" -> false,
        "documents" -> (firstDoc +: otherDocs))

      lazy val session = new NodeSetSession(java.util.UUID.randomUUID())

      val lsid = BSONDocument(
        "lsid" -> BSONDocument(
          "id" -> BSONBinary(session.lsid)))

      val writeConcern = BSONDocument(
        "writeConcern" -> BSONDocument("w" -> 1, "j" -> false))

      // ---

      "without session" in {
        writer(None)(insert1) must_=== (base ++ writeConcern)
      }

      "with session" in {
        val write = writer(Some(session))

        // w/o transaction started
        write(insert1) must_=== (base ++ lsid ++ writeConcern) and {
          session.startTransaction(WriteConcern.Default, None).
            aka("transaction") must beSuccessfulTry[(SessionTransaction, Boolean)].which { _ =>
              // w/ transaction started

              write(insert1) must_=== (base ++ lsid ++ BSONDocument(
                "txnNumber" -> 1L,
                "startTransaction" -> true, // as first command in tx
                "autocommit" -> false))
            }
        } and {
          // w/o 'startTransaction' flag after first command in tx

          write(insert1) must_=== (base ++ lsid ++ BSONDocument(
            "txnNumber" -> 1L, "autocommit" -> false))
        }
      }
    }
  }
  section("unit")

  // ---

  private lazy val firstDoc = BSONDocument("_id" -> 1, "value" -> "foo")

  private lazy val otherDocs = Seq(
    BSONDocument("_id" -> 2, "value" -> "bar"),
    BSONDocument("_id" -> 3, "value" -> "lorem"))

  private lazy val insert1 = ResolvedCollectionCommand(
    collection = "foo",
    command = new Command.Insert(
      head = firstDoc,
      tail = otherDocs,
      ordered = false,
      writeConcern = WC.Default,
      bypassDocumentValidation = false))

  private object Command extends InsertCommand[BSONSerializationPack.type] {
    val pack = BSONSerializationPack
  }
}
