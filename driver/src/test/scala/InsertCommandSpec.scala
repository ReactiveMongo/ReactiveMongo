package reactivemongo

import reactivemongo.bson.{ BSONBinary, BSONDocument }

import reactivemongo.api.{ BSONSerializationPack, ReplicaSetSession }

import reactivemongo.api.commands.{
  InsertCommand,
  ResolvedCollectionCommand,
  WriteConcern
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
        "writeConcern" -> BSONDocument("w" -> 1, "j" -> false),
        "documents" -> (firstDoc +: otherDocs))

      "without session" in {
        writer(None)(insert1) must_=== base
      }

      "with session" in {
        val session = new ReplicaSetSession(java.util.UUID.randomUUID())

        writer(Some(session))(insert1) must_=== (base ++ BSONDocument(
          "lsid" -> BSONDocument(
            "id" -> BSONBinary(session.lsid)),
          "txnNumber" -> 1L))
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
    command = Command.Insert(
      head = firstDoc,
      tail = otherDocs,
      ordered = false,
      writeConcern = WriteConcern.Default))

  private object Command extends InsertCommand[BSONSerializationPack.type] {
    val pack = BSONSerializationPack
  }
}
