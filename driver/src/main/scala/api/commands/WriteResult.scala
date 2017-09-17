package reactivemongo.api.commands

import scala.util.control.NoStackTrace

import reactivemongo.bson.BSONValue

import reactivemongo.core.errors.DatabaseException

sealed trait WriteResult {
  def ok: Boolean
  def n: Int
  def writeErrors: Seq[WriteError]
  def writeConcernError: Option[WriteConcernError]

  /** The result code */
  def code: Option[Int]

  /** If the result is a failure, the error message */
  private[commands] def errmsg: Option[String]

  private[reactivemongo] def hasErrors: Boolean = !writeErrors.isEmpty || !writeConcernError.isEmpty
  private[reactivemongo] def inError: Boolean = !ok || code.isDefined

  protected def message = errmsg.getOrElse("<none>")

  @deprecated("Use the detailed properties (e.g. `code`)", "0.12.0")
  def originalDocument = Option.empty[reactivemongo.bson.BSONDocument] // TODO
  //def stringify: String = toString + " [inError: " + inError + "]"
  //override def getMessage() = toString + " [inError: " + inError + "]"
}

object WriteResult {
  def lastError(result: WriteResult): Option[LastError] = result match {
    case error @ LastError(_, _, _, _, _, _, _, _, _, _, _, _, _, _) => Some(error)
    case _ if (result.ok) => None
    case _ => Some(LastError(
      false, // ok
      result.errmsg,
      result.code,
      None, // lastOp
      result.n,
      None, // singleShard
      false, // updatedExisting,
      None, // upserted
      None, // wnote
      false, // wtimeout,
      None, // waited,
      None, // wtime,
      result.writeErrors,
      result.writeConcernError))
  }

  /**
   * Code extractor for [[WriteResult]]
   *
   * {{{
   * import reactivemongo.api.commands.WriteResult
   *
   * def codeOr(res: WriteResult, or: => Int): Int = res match {
   *   case WriteResult.Code(code) => code
   *   case _ => or
   * }
   * }}}
   */
  object Code {
    def unapply(result: WriteResult): Option[Int] = result.code
  }

  /**
   * Code extractor for [[WriteResult]]
   *
   * {{{
   * import reactivemongo.api.commands.WriteResult
   *
   * def messageOr(res: WriteResult, or: => String): String = res match {
   *   case WriteResult.Message(msg) => msg
   *   case _ => or
   * }
   * }}}
   */
  object Message {
    def unapply(result: WriteResult): Option[String] = result.errmsg
  }

  private[reactivemongo] def empty: WriteResult = DefaultWriteResult(
    true, 0, Seq.empty, Option.empty, Option.empty, Option.empty)
}

case class LastError(
  ok: Boolean,
  errmsg: Option[String],
  code: Option[Int],
  lastOp: Option[Long],
  n: Int,
  singleShard: Option[String], // string?
  updatedExisting: Boolean,
  upserted: Option[BSONValue],
  wnote: Option[WriteConcern.W],
  wtimeout: Boolean,
  waited: Option[Int],
  wtime: Option[Int],
  writeErrors: Seq[WriteError] = Nil,
  writeConcernError: Option[WriteConcernError] = None) extends DatabaseException with WriteResult with NoStackTrace {

  @deprecated("Use [[errmsg]]", "0.12.0")
  val err = errmsg

  override def inError: Boolean = !ok || errmsg.isDefined
  //def stringify: String = toString + " [inError: " + inError + "]"

  override lazy val message = errmsg.getOrElse("<none>")
}

/**
 * @param code the error code
 * @param errmsg the error message
 */
case class WriteError(
  index: Int,
  code: Int,
  errmsg: String)

/**
 * @param code the error code
 * @param errmsg the error message
 */
case class WriteConcernError(code: Int, errmsg: String)

case class DefaultWriteResult(
  ok: Boolean,
  n: Int,
  writeErrors: Seq[WriteError],
  writeConcernError: Option[WriteConcernError],
  code: Option[Int],
  errmsg: Option[String]) extends WriteResult {
  def flatten = writeErrors.headOption.fold(this) { firstError =>
    DefaultWriteResult(
      ok = false,
      n = n,
      writeErrors = writeErrors,
      writeConcernError = writeConcernError,
      code = code.orElse(Some(firstError.code)),
      errmsg = errmsg.orElse(Some(firstError.errmsg)))
  }
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
  def flatten = writeErrors.headOption.fold(this) { firstError =>
    UpdateWriteResult(
      ok = false,
      n = n,
      nModified = nModified,
      upserted = upserted,
      writeErrors = writeErrors,
      writeConcernError = writeConcernError,
      code = code.orElse(Some(firstError.code)),
      errmsg = errmsg.orElse(Some(firstError.errmsg)))
  }
}
