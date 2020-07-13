package reactivemongo.api.commands

import scala.util.control.NoStackTrace

import reactivemongo.api.WriteConcern
import reactivemongo.api.commands.WriteResult.{
  Internal => InternalWriteResult
}

import reactivemongo.core.errors.DatabaseException

/** TestHelpers about commands. */
object TestHelpers {
  /** '''EXPERIMENTAL:''' Test factory for [[WriteResult]] */
  def WriteResult(
    ok: Boolean = true,
    n: Int = 1,
    writeErrors: Seq[WriteError] = Nil,
    writeConcernError: Option[WriteConcernError] = None,
    code: Option[Int] = None,
    errmsg: Option[String] = None): WriteResult =
    new DefaultWriteResult(ok, n, writeErrors, writeConcernError, code, errmsg)

  /** '''EXPERIMENTAL:''' */
  def WriteError(
    errmsg: Option[String] = None,
    code: Option[Int] = None,
    lastOp: Option[Long] = None,
    singleShard: Option[String] = None,
    wnote: Option[WriteConcern.W] = None,
    wtimeout: Boolean = false,
    waited: Option[Int] = None,
    wtime: Option[Int] = None,
    writeErrors: Seq[WriteError] = Seq.empty,
    writeConcernError: Option[WriteConcernError] = None): WriteResult with DatabaseException = new WriteException(errmsg, code, lastOp, singleShard,
    wnote, wtimeout, waited, wtime, writeErrors, writeConcernError)

  // ---

  private final class WriteException(
    val errmsg: Option[String],
    val code: Option[Int],
    val lastOp: Option[Long],
    val singleShard: Option[String],
    val wnote: Option[WriteConcern.W],
    val wtimeout: Boolean,
    val waited: Option[Int],
    val wtime: Option[Int],
    val writeErrors: Seq[WriteError],
    val writeConcernError: Option[WriteConcernError])
    extends Exception with InternalWriteResult
    with DatabaseException with NoStackTrace {

    val ok: Boolean = false

    val n: Int = 0

    val updatedExisting: Boolean = false

    type Document = Nothing

    override def inError: Boolean = true

    private[reactivemongo] def originalDocument = Option.empty[Nothing]

    override lazy val message = errmsg.getOrElse("<none>")

    override protected lazy val tupled = Tuple13(ok, errmsg, code, lastOp, n, singleShard, updatedExisting, wnote, wtimeout, waited, wtime, writeErrors, writeConcernError)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    override def toString = s"LastError${tupled.toString}"
  }
}
