package reactivemongo.api.commands

import scala.util.control.NoStackTrace

/** Base definition for all the errors for the command execution errors. */
trait CommandError extends Exception with NoStackTrace {
  /** The error code */
  def code: Option[Int]

  /** The error message */
  def errmsg: Option[String]

  override def getMessage = s"CommandError[code=${code.getOrElse("<unknown>")}, errmsg=${errmsg.getOrElse("<unknown>")}]"
}

object CommandError {
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
      case _                           => None
    }
  }
}
