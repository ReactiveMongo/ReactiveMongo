package reactivemongo.api.commands

/** Error extractor for command results */
object CommandException {
  import reactivemongo.core.errors.DatabaseException

  /**
   * Pattern matching extractor for the error code.
   *
   * {{{
   * import reactivemongo.core.errors.DatabaseException
   * import reactivemongo.api.commands.{ CommandException, WriteResult }
   *
   * def testError(err: DatabaseException): String = err match {
   *   case CommandException.Code(code) => s"hasCode: \\$code"
   *   case _ => "no-code"
   * }
   *
   * def testWriteRes(res: WriteResult): String = res match {
   *   case CommandException.Code(code) => s"onlyIfFailure: \\$code"
   *   case _ => "no-code"
   * }
   * }}}
   *
   * @see [[WriteResult.Code]]
   */
  object Code {
    def unapply(scrutinee: Any): Option[Int] = scrutinee match {
      case res: WriteResult if !res.ok => WriteResult.Code.unapply(res)
      case err: DatabaseException      => err.code
      case _                           => None
    }
  }

  /**
   * Pattern matching extractor for the error message.
   *
   * {{{
   * import reactivemongo.core.errors.DatabaseException
   * import reactivemongo.api.commands.{ CommandException, WriteResult }
   *
   * def testError(err: DatabaseException): String = err match {
   *   case CommandException.Message(msg) => s"hasMessage: \\$msg"
   *   case _ => "no-message"
   * }
   *
   * def testWriteRes(res: WriteResult): String = res match {
   *   case CommandException.Message(msg) => s"onlyIfFailure: \\$msg"
   *   case _ => "no-message"
   * }
   * }}}
   */
  object Message {
    def unapply(scrutinee: Any): Option[String] = scrutinee match {
      case res: WriteResult if !res.ok => WriteResult.Message.unapply(res)
      case err: DatabaseException      => Option(err.getMessage)
      case _                           => None
    }
  }
}
