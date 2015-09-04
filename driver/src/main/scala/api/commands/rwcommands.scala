package reactivemongo.api.commands

import scala.util.control.NoStackTrace

import reactivemongo.bson.{ BSONObjectID, BSONValue }
import reactivemongo.core.errors.DatabaseException
import reactivemongo.api.{ Cursor, SerializationPack }

trait Mongo26WriteCommand

/**
 * @param wtimeout the [[http://docs.mongodb.org/manual/reference/write-concern/#wtimeout time limit]]
 */
case class GetLastError(
  w: GetLastError.W,
  j: Boolean,
  fsync: Boolean,
  wtimeout: Option[Int] = None) extends Command
    with CommandWithResult[LastError]

object GetLastError {
  sealed trait W
  case object Majority extends W
  case class TagSet(tag: String) extends W
  case class WaitForAknowledgments(i: Int) extends W
  object W {
    implicit def strToTagSet(s: String): W = TagSet(s)
    implicit def intToWaitForAknowledgments(i: Int): W = WaitForAknowledgments(i)
  }

  val Unacknowledged: GetLastError =
    GetLastError(WaitForAknowledgments(0), false, false, None)

  val Acknowledged: GetLastError =
    GetLastError(WaitForAknowledgments(1), false, false, None)

  val Journaled: GetLastError =
    GetLastError(WaitForAknowledgments(1), true, false, None)

  def ReplicaAcknowledged(n: Int, timeout: Int, journaled: Boolean): GetLastError = GetLastError(WaitForAknowledgments(if (n < 2) 2 else n), journaled, false, (if (timeout <= 0) None else Some(timeout)))

  def TagReplicaAcknowledged(tag: String, timeout: Int, journaled: Boolean): GetLastError = GetLastError(TagSet(tag), journaled, false, (if (timeout <= 0) None else Some(timeout)))

  def Default: GetLastError = Acknowledged
}

case class LastError(
    ok: Boolean,
    err: Option[String],
    code: Option[Int],
    lastOp: Option[Long],
    n: Int,
    singleShard: Option[String], // string?
    updatedExisting: Boolean,
    upserted: Option[BSONValue],
    wnote: Option[WriteConcern.W],
    wtimeout: Boolean,
    waited: Option[Int],
    wtime: Option[Int]) extends WriteResult {
  def writeErrors: Seq[WriteError] = Seq.empty
  def writeConcernError: Option[WriteConcernError] = None
  def errmsg = err

  override def inError: Boolean = !ok || err.isDefined
  //def stringify: String = toString + " [inError: " + inError + "]"
}

sealed trait WriteResult extends DatabaseException with NoStackTrace {
  def ok: Boolean
  def n: Int
  def writeErrors: Seq[WriteError]
  def writeConcernError: Option[WriteConcernError]
  def code: Option[Int]
  def errmsg: Option[String]

  def hasErrors: Boolean = !writeErrors.isEmpty || !writeConcernError.isEmpty
  def inError: Boolean = !ok || code.isDefined
  def message = errmsg.getOrElse("<none>")
  override def originalDocument = None // TODO
  //def stringify: String = toString + " [inError: " + inError + "]"
  //override def getMessage() = toString + " [inError: " + inError + "]"
}

case class WriteError(
  index: Int,
  code: Int,
  errmsg: String)

case class WriteConcernError(
  code: Int,
  errmsg: String)

case class DefaultWriteResult(
    ok: Boolean,
    n: Int,
    writeErrors: Seq[WriteError],
    writeConcernError: Option[WriteConcernError],
    code: Option[Int],
    errmsg: Option[String]) extends WriteResult {
  def flatten =
    if (!writeErrors.isEmpty)
      DefaultWriteResult(
        ok = false,
        n = n,
        writeErrors = writeErrors,
        writeConcernError = writeConcernError,
        code = code.orElse(Some(writeErrors.head.code)),
        errmsg = errmsg.orElse(Some(writeErrors.head.errmsg)))
    else this
}

case class Upserted(index: Int, _id: BSONValue)

case class UpdateWriteResult(
    ok: Boolean,
    n: Int,
    nModified: Int,
    upserted: Seq[Upserted],
    writeErrors: Seq[WriteError],
    writeConcernError: Option[WriteConcernError],
    code: Option[Int],
    errmsg: Option[String]) extends WriteResult {
  def flatten = if (!writeErrors.isEmpty) {
    UpdateWriteResult(
      ok = false,
      n = n,
      nModified = nModified,
      upserted = upserted,
      writeErrors = writeErrors,
      writeConcernError = writeConcernError,
      code = code.orElse(Some(writeErrors.head.code)),
      errmsg = errmsg.orElse(Some(writeErrors.head.errmsg)))
  } else this
}

object MultiBulkWriteResult {
  def apply(): MultiBulkWriteResult =
    MultiBulkWriteResult(true, 0, 0, Seq.empty, Seq.empty, None, None, None, 0)
  def apply(wr: WriteResult): MultiBulkWriteResult =
    apply().merge(wr)
}

case class MultiBulkWriteResult(
    ok: Boolean,
    n: Int,
    nModified: Int,
    upserted: Seq[Upserted],
    writeErrors: Seq[WriteError],
    writeConcernError: Option[WriteConcernError], // TODO ?
    code: Option[Int],
    errmsg: Option[String],
    totalN: Int) {
  def merge(wr: WriteResult): MultiBulkWriteResult = wr match {
    case wr: UpdateWriteResult => MultiBulkWriteResult(
      ok = ok && wr.ok,
      n = n + wr.n,
      writeErrors = writeErrors ++ wr.writeErrors.map(e => e.copy(index = e.index + totalN)),
      writeConcernError = writeConcernError.orElse(wr.writeConcernError),
      code = code.orElse(wr.code),
      errmsg = errmsg.orElse(wr.errmsg),
      nModified = wr.nModified,
      upserted = wr.upserted,
      totalN = totalN + wr.n + wr.writeErrors.size)
    case _ =>
      MultiBulkWriteResult(
        ok = ok && wr.ok,
        n = n + wr.n,
        writeErrors = writeErrors ++ wr.writeErrors.map(e => e.copy(index = e.index + totalN)),
        writeConcernError = writeConcernError.orElse(wr.writeConcernError),
        code = code.orElse(wr.code),
        errmsg = errmsg.orElse(wr.errmsg),
        nModified = nModified,
        upserted = upserted,
        totalN = totalN + wr.n + wr.writeErrors.size)
  }

}

trait InsertCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  case class Insert(
    documents: Seq[P#Document],
    ordered: Boolean,
    writeConcern: WriteConcern) extends CollectionCommand with CommandWithResult[InsertResult] with Mongo26WriteCommand

  type InsertResult = DefaultWriteResult // for simplified imports

  object Insert {
    def apply(firstDoc: ImplicitlyDocumentProducer, otherDocs: ImplicitlyDocumentProducer*): Insert = apply()(firstDoc, otherDocs: _*)

    def apply(ordered: Boolean = true, writeConcern: WriteConcern = WriteConcern.Default)(firstDoc: ImplicitlyDocumentProducer, otherDocs: ImplicitlyDocumentProducer*): Insert = new Insert(firstDoc.produce #:: otherDocs.toStream.map(_.produce), ordered, writeConcern)
  }
}

trait UpdateCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  case class Update(
    documents: Seq[UpdateElement],
    ordered: Boolean,
    writeConcern: WriteConcern) extends CollectionCommand with CommandWithResult[UpdateResult] with Mongo26WriteCommand

  type UpdateResult = UpdateWriteResult

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
    def apply(ordered: Boolean = true, writeConcern: WriteConcern = WriteConcern.Default)(firstUpdate: UpdateElement, updates: UpdateElement*): Update =
      Update(
        firstUpdate +: updates,
        ordered,
        writeConcern)
  }
}

trait DeleteCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  case class Delete(
    deletes: Seq[DeleteElement],
    ordered: Boolean,
    writeConcern: WriteConcern) extends CollectionCommand with CommandWithResult[DeleteResult] with Mongo26WriteCommand

  object Delete {
    def apply(firstDelete: DeleteElement, deletes: DeleteElement*): Delete =
      apply()(firstDelete, deletes: _*)
    def apply(ordered: Boolean = true, writeConcern: WriteConcern = WriteConcern.Default)(firstDelete: DeleteElement, deletes: DeleteElement*): Delete =
      Delete(firstDelete +: deletes, ordered, writeConcern)
  }

  case class DeleteElement(
    q: P#Document,
    limit: Int)

  object DeleteElement {
    def apply(doc: ImplicitlyDocumentProducer, limit: Int = 0): DeleteElement =
      DeleteElement(doc.produce, limit)
  }

  type DeleteResult = DefaultWriteResult
}
