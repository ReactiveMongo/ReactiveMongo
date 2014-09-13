package reactivemongo.api.commands

import reactivemongo.api.{ BSONSerializationPack, Cursor, SerializationPack }
import reactivemongo.bson.BSONObjectID

/*
j Boolean If true, wait for the next journal commit before returning, rather than waiting for a full disk flush. If mongod does not have journaling enabled, this option has no effect. If this option is enabled for a write operation, mongod will wait no more than 1/3 of the current commitIntervalMs before writing data to the journal.
w integer or string When running with replication, this is the number of servers to replicate to before returning. A w value of 1 indicates the primary only. A w value of 2 includes the primary and at least one secondary, etc. In place of a number, you may also set w to majority to indicate that the command should wait until the latest write propagates to a majority of replica set members. If using w, you should also use wtimeout. Specifying a value for w without also providing a wtimeout may cause getLastError to block indefinitely.
fsync Boolean If true, wait for mongod to write this data to disk before returning. Defaults to false. In most cases, use the j option to ensure durability and consistency of the data set.
wtimeout  integer
*/

case class GetLastError(
  w: GetLastError.W,
  j: Boolean,
  fsync: Boolean,
  wtimeout: Option[Int] = None
) extends Command with CommandWithResult[LastError]
object GetLastError {
  sealed trait W
  case object Majority extends W
  case class TagSet(s: String) extends W
  case class WaitForAknowledgments(i: Int) extends W
  object W {
    implicit def strToTagSet(s: String): W = TagSet(s)
    implicit def intToWaitForAknowledgments(i: Int): W = WaitForAknowledgments(i)
  }
  val DefaultWriteConcern = GetLastError(WaitForAknowledgments(1), false, false, Some(0))
}
case class LastError(
  ok: Boolean,
  err: Option[String],
  code: Option[Int],
  lastOp: Option[Long],
  n: Int,
  singleShard: Option[String], // string?
  updatedExisting: Boolean,
  upserted: Option[BSONObjectID],
  wnote: Option[WriteConcern.W],
  wtimeout: Boolean,
  waited: Option[Int],
  wtime: Option[Int]
)

sealed trait WriteCommandsCommon[P <: SerializationPack] {
  case class WriteError(
    index: Int,
    code: Int,
    errmsg: String)
  case class WriteConcernError(
    code: Int,
    errmsg: String)
}

trait InsertCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] with WriteCommandsCommon[P] {
  case class Insert(
    documents: Seq[P#Document],
    ordered: Boolean,
    writeConcern: WriteConcern) extends CollectionCommand with CommandWithResult[InsertResult]

  case class InsertResult(
    ok: Boolean,
    n: Int,
    writeErrors: Seq[WriteError],
    writeConcernError: Option[WriteConcernError])

  object Insert {
    def apply(firstDoc: ImplicitlyDocumentProducer, otherDocs: ImplicitlyDocumentProducer*): Insert =
      apply()(firstDoc, otherDocs: _*)
    def apply(ordered: Boolean = true, writeConcern: WriteConcern = WriteConcern.DefaultWriteConcern)(firstDoc: ImplicitlyDocumentProducer, otherDocs: ImplicitlyDocumentProducer*): Insert =
      new Insert(firstDoc.produce #:: otherDocs.toStream.map(_.produce), ordered, writeConcern)
  }
}

trait UpdateCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] with WriteCommandsCommon[P] {
  case class Update(
    documents: Seq[UpdateElement],
    ordered: Boolean,
    writeConcern: WriteConcern) extends CollectionCommand with CommandWithResult[UpdateResult]

  case class Upserted(
    index: Int,
    _id: Any) // TODO: _id

  case class UpdateResult(
    ok: Boolean,
    n: Int,
    nModified: Int,
    upserted: Seq[Upserted],
    writeErrors: Seq[WriteError],
    writeConcernError: Option[WriteConcernError])

  case class UpdateElement(
    q: P#Document,
    u: P#Document,
    upsert: Boolean,
    multi: Boolean)

  object UpdateElement {
    def apply(q: ImplicitlyDocumentProducer, u: ImplicitlyDocumentProducer, upsert: Boolean = false, multi: Boolean = false): UpdateElement =
      UpdateElement(
        q.produce,
        u.produce,
        upsert,
        multi)
  }

  object Update {
    def apply(firstUpdate: UpdateElement, updates: UpdateElement*): Update =
      apply()(firstUpdate, updates: _*)
    def apply(ordered: Boolean = true, writeConcern: WriteConcern = WriteConcern.DefaultWriteConcern)(firstUpdate: UpdateElement, updates: UpdateElement*): Update =
      Update(
        firstUpdate +: updates,
        ordered,
        writeConcern)
  }
}

trait DeleteCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] with WriteCommandsCommon[P] {
  case class Delete(
    deletes: Seq[DeleteElement],
    ordered: Boolean,
    writeConcern: WriteConcern) extends CollectionCommand with CommandWithResult[DeleteResult]

  object Delete {
    def apply(ordered: Boolean = true, writeConcern: WriteConcern = WriteConcern.DefaultWriteConcern)(firstDelete: DeleteElement, deletes: DeleteElement*): Delete =
      Delete(firstDelete +: deletes, ordered, writeConcern)
  }

  case class DeleteElement(
    q: P#Document,
    limit: Int) {
    def apply(doc: ImplicitlyDocumentProducer, limit: Int = 0) =
      DeleteElement(doc.produce, limit)
  }

  case class DeleteResult(
    ok: Boolean,
    n: Int,
    writeErrors: Seq[WriteError],
    writeConcernError: Option[WriteConcernError])
}