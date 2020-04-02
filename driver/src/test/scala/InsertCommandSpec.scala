package reactivemongo

import reactivemongo.api.bson.{ BSONBinary, BSONDocument }

import reactivemongo.api.{
  PackSupport,
  NodeSetSession,
  Session,
  SessionTransaction,
  WriteConcern
}

import reactivemongo.api.commands.{ InsertCommand, ResolvedCollectionCommand }

final class InsertCommandSpec extends org.specs2.mutable.Specification {
  "Insert command" title

  section("unit")
  "Insert command" should {
    "be written" >> {
      val base = BSONDocument(
        "insert" -> "foo",
        "ordered" -> false,
        "documents" -> (firstDoc +: otherDocs),
        "bypassDocumentValidation" -> false)

      val writeConcern = BSONDocument(
        "writeConcern" -> BSONDocument("w" -> 1, "j" -> false))

      // ---

      "without session" in withInsert(new Support()) { (insert, writer) =>
        writer(insert) must_=== (base ++ writeConcern)
      }

      "with session" in {
        lazy val session = new NodeSetSession(java.util.UUID.randomUUID())

        withInsert(new Support(Some(session))) { (insert, writer) =>
          val lsid = BSONDocument(
            "lsid" -> BSONDocument(
              "id" -> BSONBinary(session.lsid)))

          // w/o transaction started
          writer(insert) must_=== (base ++ lsid ++ writeConcern) and {
            session.startTransaction(WriteConcern.Default, None).
              aka("transaction") must beSuccessfulTry[(SessionTransaction, Boolean)].which { _ =>
                // w/ transaction started

                writer(insert) must_=== (base ++ lsid ++ BSONDocument(
                  "txnNumber" -> 1L,
                  "startTransaction" -> true, // as first command in tx
                  "autocommit" -> false))
              }
          } and {
            // w/o 'startTransaction' flag after first command in tx

            writer(insert) must_=== (base ++ lsid ++ BSONDocument(
              "txnNumber" -> 1L, "autocommit" -> false))
          }
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

  def withInsert[T](support: Support)(f: Function2[ResolvedCollectionCommand[support.Insert], support.InsertCmd => support.pack.Document, T]): T = f(
    new ResolvedCollectionCommand(
      collection = "foo",
      command = new support.Insert(
        head = firstDoc,
        tail = otherDocs,
        ordered = false,
        writeConcern = WriteConcern.Default,
        bypassDocumentValidation = false)),
    support.pack.serialize(_: support.InsertCmd, support.insertWriter))

  import reactivemongo.api.tests.Pack

  private class Support(s: Option[Session] = None)
    extends PackSupport[Pack] with InsertCommand[Pack] {

    val pack = reactivemongo.api.tests.pack

    def session() = s
  }
}
