package reactivemongo

import reactivemongo.bson.{ BSONArray, BSONBinary, BSONDocument }

import reactivemongo.api.{ BSONSerializationPack, ReplicaSetSession }

import reactivemongo.api.commands.{
  UpdateCommand,
  ResolvedCollectionCommand,
  WriteConcern
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
        "writeConcern" -> BSONDocument(
          "w" -> 1,
          "j" -> true),
        "updates" -> BSONArray(
          BSONDocument(
            "q" -> BSONDocument("_id" -> 1),
            "u" -> BSONDocument("$set" -> BSONDocument("value" -> 1)),
            "upsert" -> true,
            "multi" -> false),
          BSONDocument(
            "q" -> BSONDocument("value" -> 2),
            "u" -> BSONDocument("$set" -> BSONDocument("label" -> "two")),
            "upsert" -> false,
            "multi" -> true)))

      "without session" in {
        writer(None)(update1) must_=== base
      }

      "with session" in {
        val session = new ReplicaSetSession(java.util.UUID.randomUUID())

        writer(Some(session))(update1) must_=== (base ++ BSONDocument(
          "lsid" -> BSONDocument(
            "id" -> BSONBinary(session.lsid)),
          "txnNumber" -> 1L))
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
      writeConcern = WriteConcern.Default))

  private object Command extends UpdateCommand[BSONSerializationPack.type] {
    val pack = BSONSerializationPack
  }
}
