package reactivemongo.api.commands

import reactivemongo.core.errors.{ CommandException => CmdErr }
import reactivemongo.core.protocol.Response

import reactivemongo.api.SerializationPack

/** An authentication result */
sealed trait AuthenticationResult

/** A successful authentication result. */
sealed trait SuccessfulAuthentication extends AuthenticationResult

/** A silent successful authentication result (MongoDB <= 2.0). */
private[reactivemongo] object SilentSuccessfulAuthentication
    extends SuccessfulAuthentication

/**
 * A verbose successful authentication result (MongoDB >= 2.2).
 *
 * Previous versions of MongoDB only return ok = BSONDouble(1.0).
 *
 * @param db the database name
 * @param user the user name
 * @param readOnly states if the authentication gives us only the right to read from the database.
 */
private[reactivemongo] case class VerboseSuccessfulAuthentication(
    db: String,
    user: String,
    readOnly: Boolean)
    extends SuccessfulAuthentication

/**
 * A failed authentication result
 */
@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
sealed abstract class FailedAuthentication
    extends CmdErr
    with AuthenticationResult
    with scala.util.control.NoStackTrace {

  /** The explanation of the error */
  def message: String = "<error>"

  override def equals(that: Any): Boolean = that match {
    case other: FailedAuthentication =>
      tupled == other.tupled

    case _ =>
      false
  }
}

private[reactivemongo] object FailedAuthentication {

  @SuppressWarnings(Array("VariableShadowing"))
  def apply[P <: SerializationPack](
      _pack: P
    )(msg: String,
      c: Option[Int],
      doc: Option[_pack.Document]
    ) = new FailedAuthentication {
    val code = c
    override val message = msg
    lazy val originalDocument = doc.map(_pack.pretty)
  }
}

private[reactivemongo] object AuthenticationResult {
  import scala.util.control.NonFatal

  def parse[P <: SerializationPack](
      pack: P,
      resp: Response
    )(reader: pack.Reader[AuthenticationResult]
    ): Either[CmdErr, SuccessfulAuthentication] =
    try {
      pack.readAndDeserialize(resp, reader) match {
        case failed: FailedAuthentication =>
          Left(failed)

        case suc: SuccessfulAuthentication =>
          Right(suc)
      }
    } catch {
      case NonFatal(error) =>
        Left(CmdErr(pack)(error.getMessage, None, None))
    }
}
