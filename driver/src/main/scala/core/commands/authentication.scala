package reactivemongo.core.commands

import reactivemongo.api.{ BSONSerializationPack, SerializationPack }

import reactivemongo.bson.{
  BSONDocument,
  BSONInteger,
  BSONNumberLike,
  BSONString
}

import reactivemongo.core.protocol.Response
import reactivemongo.core.errors.{ CommandError, BSONCommandError }

private[core] case class X509Authenticate(user: Option[String])
  extends Command[SuccessfulAuthentication] {

  private val userNameDocument = user.fold(BSONDocument.empty) { name =>
    BSONDocument("user" -> BSONString(name))
  }

  override def makeDocuments = BSONDocument(
    "authenticate" -> BSONInteger(1),
    "mechanism" -> BSONString("MONGODB-X509")) ++ userNameDocument

  override val ResultMaker = X509Authenticate
}

private[core] object X509Authenticate extends BSONCommandResultMaker[SuccessfulAuthentication] {
  def parseResponse(response: Response): Either[CommandError, SuccessfulAuthentication] = apply(response)

  def apply(document: BSONDocument) = {
    CommandError.checkOk(document, Some("authenticate"), (doc, _) => {
      FailedAuthentication(BSONSerializationPack)(
        doc.getAs[BSONString]("errmsg").map(_.value).getOrElse(""),
        doc.getAs[BSONNumberLike]("code").map(_.toInt),
        Some(doc))

    }).toLeft(SilentSuccessfulAuthentication)
  }
}

/** An authentication result */
sealed trait AuthenticationResult

/** A successful authentication result. */
sealed trait SuccessfulAuthentication extends AuthenticationResult

/** A silent successful authentication result (MongoDB <= 2.0).*/
private[reactivemongo] object SilentSuccessfulAuthentication extends SuccessfulAuthentication

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
  readOnly: Boolean) extends SuccessfulAuthentication

/**
 * A failed authentication result
 */
sealed abstract class FailedAuthentication
  extends BSONCommandError with AuthenticationResult {

  private[core] type Pack <: SerializationPack

  private[core] val pack: Pack

  /** The explanation of the error */
  def message: String = "<error>"

  private[reactivemongo] def response: Option[pack.Document]

  @deprecated("Use `response`", "0.19.1")
  private[reactivemongo] lazy val originalDocument: Option[BSONDocument] =
    response.map(pack.bsonValue(_)).collect {
      case doc: BSONDocument => doc
    }

  private[reactivemongo] lazy val tupled = message -> response

  override def hashCode: Int = tupled.hashCode

  override def equals(that: Any): Boolean = that match {
    case other: FailedAuthentication =>
      tupled == other.tupled

    case _ =>
      false
  }
}

private[reactivemongo] object FailedAuthentication {
  type Aux[P <: SerializationPack] = FailedAuthentication { type Pack = P }

  def apply[P <: SerializationPack](_pack: P)(
    msg: String,
    c: Option[Int],
    doc: Option[_pack.Document]): Aux[_pack.type] = new FailedAuthentication {
    type Pack = _pack.type
    val pack: _pack.type = _pack
    val code = c
    override val message = msg
    val response = doc
  }
}

private[reactivemongo] object AuthenticationResult {
  import scala.util.control.NonFatal

  def parse[P <: SerializationPack](
    pack: P,
    resp: Response)(reader: pack.Reader[AuthenticationResult]): Either[CommandError, SuccessfulAuthentication] = try {
    pack.readAndDeserialize(resp, reader) match {
      case failed: FailedAuthentication =>
        Left(failed)

      case suc: SuccessfulAuthentication =>
        Right(suc)
    }
  } catch {
    case NonFatal(error) =>
      Left(CommandError(pack)(error.getMessage, None, None))
  }
}
