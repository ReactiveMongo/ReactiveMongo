package reactivemongo.api.commands

import scala.util.control.NoStackTrace

import reactivemongo.api.SerializationPack

/** Base definition for all the errors for the command execution errors. */
trait CommandError extends Exception with NoStackTrace {
  /** The error code */
  def code: Option[Int]

  /** The error message */
  def errmsg: Option[String]

  override def getMessage = s"CommandError[code=${code.getOrElse("<unknown>")}, errmsg=${errmsg.getOrElse("<unknown>")}]"
}

object CommandError {
  import reactivemongo.core.errors.DatabaseException

  object Code {
    /**
     * Pattern matching extractor for the error code.
     *
     * {{{
     * import reactivemongo.api.commands.CommandError
     *
     * def testError(err: CommandError): String = err match {
     *   case CommandError.Code(code) => s"hasCode: \$code"
     *   case _ => "no-code"
     * }
     *
     * def testWriteRes(res: WriteResult): String = res match {
     *   case CommandError.Code(code) => s"onlyIfFailure: \$code"
     *   case _ => "no-code"
     * }
     * }}}
     *
     * @see [[WriteResult.Code]]
     */
    def unapply(scrutinee: Any): Option[Int] = scrutinee match {
      case err: CommandError           => err.code
      case res: WriteResult if !res.ok => WriteResult.Code.unapply(res)
      case err: DatabaseException      => err.code
      case _                           => None
    }
  }

  object Message {
    /**
     * Pattern matching extractor for the error message.
     *
     * {{{
     * import reactivemongo.api.commands.CommandError
     *
     * def testError(err: CommandError): String = err match {
     *   case CommandError.Message(msg) => s"hasMessage: \$msg"
     *   case _ => "no-message"
     * }
     *
     * def testWriteRes(res: WriteResult): String = res match {
     *   case CommandError.Message(msg) => s"onlyIfFailure: \$msg"
     *   case _ => "no-message"
     * }
     * }}}
     */
    def unapply(scrutinee: Any): Option[String] = scrutinee match {
      case err: CommandError           => err.errmsg
      case res: WriteResult if !res.ok => WriteResult.Message.unapply(res)
      case err: DatabaseException      => Option(err.getMessage)
      case _                           => None
    }
  }

  /**
   * @param pack the serialization pack
   * @param code the error code
   * @param errmsg the error message
   * @param originalDocument the response document
   */
  private[reactivemongo] def apply[P <: SerializationPack](pack: P)(
    code: Option[Int],
    errmsg: Option[String],
    originalDocument: pack.Document): CommandError =
    new DefaultCommandError(code, errmsg, () => pack pretty originalDocument)

  private[reactivemongo] def parse[P <: SerializationPack](
    pack: P)(error: DatabaseException): CommandError =
    new DefaultCommandError(error.code, Some(error.getMessage), { () =>
      error.originalDocument.fold("<unknown>")(
        reactivemongo.bson.BSONDocument.pretty(_))
    })

  // ---

  private[reactivemongo] final class DefaultCommandError(
    val code: Option[Int],
    val errmsg: Option[String],
    showDocument: () => String) extends CommandError {
    override def getMessage = s"CommandError[code=${code.getOrElse("<unknown>")}, errmsg=${errmsg.getOrElse("<unknown>")}, doc: ${showDocument()}]"
  }
}
