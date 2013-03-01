package reactivemongo.core.errors

import reactivemongo.bson._
import DefaultBSONHandlers._

/** A driver error - can be from a MongoDB node or not. */
trait ReactiveMongoError extends Throwable {
  /** explanation message */
  val message: String

  override def getMessage :String = "MongoError['" + message + "']"
}

/** An error thrown by a MongoDB node. */
trait DBError extends ReactiveMongoError {
  /** original document of this error */
  val originalDocument: Option[BSONDocument]

  /** error code */
  val code: Option[Int]

  override def getMessage :String = "MongoError['" + message + "'" + code.map(c => " (code = " + c + ")").getOrElse("") + "]"

  /** Tells if this error is due to a write on a secondary node. */
  lazy val isNotAPrimaryError :Boolean = code.map {
    case 10054 | 10056 | 10058 | 10107 | 13435 | 13436 => true
    case _ => false
  }.getOrElse(false)
}

/** A non recoverable error */
trait NonRecoverableError {
  self :ReactiveMongoError =>
}

object ReactiveMongoError {
  def apply(message: String) :ReactiveMongoError = GenericMongoError(message)

  def apply(doc: BSONDocument) :DBError = new CompleteDBError(doc)
}

/** A generic driver error. */
case class GenericMongoError(
  message: String
) extends ReactiveMongoError

/** A generic error thrown by a MongoDB node. */
case class GenericDBError(
  message: String,
  code: Option[Int]
) extends DBError {
  val originalDocument = None
}

/** An error thrown by a MongoDB node (containing the original document of the error). */
class CompleteDBError(
  doc: BSONDocument
) extends DBError {
  val originalDocument = Some(doc)
  lazy val message = doc.getAs[BSONString]("$err").map(_.value).getOrElse("$err is not present, unknown error")
  lazy val code = doc.getAs[BSONInteger]("code").map(_.value)
}

