/*
 * Copyright 2012-2013 Stephane Godbillon (@sgodbillon) and Zenexity
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivemongo.core.errors

import reactivemongo.api.SerializationPack

import scala.util.control.NoStackTrace

import reactivemongo.bson.{ BSONDocument, BSONInteger, BSONNumberLike }
import reactivemongo.bson.DefaultBSONHandlers._

/** An error that can come from a MongoDB node or not. */
sealed trait ReactiveMongoException extends Exception {
  /** explanation message */
  def message: String

  override def getMessage: String = s"MongoError['$message']"
}

/** An error thrown by a MongoDB node. */
trait DatabaseException extends ReactiveMongoException {
  /** original document of this error */
  private[reactivemongo] def originalDocument: Option[BSONDocument]

  /** error code */
  def code: Option[Int]

  final override def getMessage: String = s"DatabaseException['$message'" + code.map(c => s" (code = $c)").getOrElse("") + "]"

  /** Tells if this error is due to a write on a secondary node. */
  private[reactivemongo] final def isNotAPrimaryError: Boolean = code.map {
    case 10054 | 10056 | 10058 | 10107 | 13435 | 13436 => true
    case _ => false
  }.getOrElse(false)

  /** Tells if this error is related to authentication issues. */
  private[reactivemongo] final def isUnauthorized: Boolean = code.map {
    case 10057 | 15845 | 16550 => true
    case _                     => false
  }.getOrElse(false)

  override def equals(that: Any): Boolean = that match {
    case other: DatabaseException =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString: String = getMessage

  private lazy val tupled = originalDocument -> code
}

private[reactivemongo] object DatabaseException {
  def apply(cause: Throwable): DatabaseException = new Default(cause)

  def apply[P <: SerializationPack](pack: P)(doc: pack.Document): DatabaseException = new DatabaseException {
    private lazy val decoder = pack.newDecoder

    val originalDocument = Some(pack bsonValue doc).collect {
      case doc: BSONDocument => doc
    }

    lazy val message = {
      decoder.string(doc, f"$$err").orElse {
        decoder.string(doc, "errmsg")
      }.getOrElse(
        s"message is not present, unknown error: ${pack pretty doc}")
    }

    lazy val code = decoder.int(doc, "code")
  }

  // ---

  private final class Default(val cause: Throwable) extends DatabaseException {
    type Document = Nothing

    val originalDocument = Option.empty[Nothing]
    val code = Option.empty[Int]
    val message = s"${cause.getClass.getName}: ${cause.getMessage}"
  }
}

/** A driver-specific error */
private[reactivemongo] trait DriverException extends ReactiveMongoException {
  protected def cause: Throwable = null
  override def getCause = cause
}

/** A generic driver error. */
private[reactivemongo] final class GenericDriverException(
  val message: String) extends DriverException with NoStackTrace {

  override def equals(that: Any): Boolean = that match {
    case other: GenericDriverException =>
      (this.message == null && other.message == null) || (
        this.message != null && this.message.equals(other.message))

    case _ =>
      false
  }

  override def hashCode: Int = if (message == null) -1 else message.hashCode

  override def toString = s"GenericDriverException($message)"
}

private[reactivemongo] final class ConnectionNotInitialized(
  val message: String,
  override val cause: Throwable) extends DriverException {

  override lazy val hashCode = (message -> cause).hashCode

  override def equals(that: Any): Boolean = that match {
    case x: ConnectionNotInitialized =>
      (message -> cause) == (x.message -> x.cause)

    case _ => false
  }
}

private[reactivemongo] final class ConnectionException(
  val message: String) extends DriverException {

  override def equals(that: Any): Boolean = that match {
    case other: ConnectionException =>
      (this.message == null && other.message == null) || (
        this.message != null && this.message.equals(other.message))

    case _ =>
      false
  }

  override def hashCode: Int = if (message == null) -1 else message.hashCode

  override def toString = s"ConnectionException($message)"
}

/** A generic error thrown by a MongoDB node. */
private[reactivemongo] final class GenericDatabaseException(
  val message: String,
  val code: Option[Int]) extends DatabaseException {

  val originalDocument = None

  private[core] lazy val tupled = message -> code

  override def equals(that: Any): Boolean = that match {
    case other: GenericDatabaseException =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString = s"GenericDatabaseException($message)"
}

/** An error thrown by a MongoDB node (containing the original document of the error). */
private[reactivemongo] class DetailedDatabaseException(
  doc: BSONDocument) extends DatabaseException with NoStackTrace {

  type Document = BSONDocument

  val originalDocument = Some(doc)

  lazy val message = doc.getAs[String]("$err").orElse {
    doc.getAs[String]("errmsg")
  }.getOrElse(
    s"message is not present, unknown error: ${BSONDocument pretty doc}")

  lazy val code = doc.getAs[BSONInteger]("code").map(_.value)
}

/** A generic command error. TODO: Remove */
private[reactivemongo] trait CommandError extends ReactiveMongoException {
  /** error code */
  val code: Option[Int]

  override def getMessage: String = s"CommandError['$message'" + code.map(c => " (code = " + c + ")").getOrElse("") + "]"
}

private[reactivemongo] object CommandError {
  /**
   * Makes a 'DefaultCommandError'.
   *
   * @param message The error message.
   * @param originalDocument The original document contained in the response.
   * @param code The code of the error, if any.
   */
  def apply(message: String, originalDocument: Option[BSONDocument] = None, code: Option[Int] = None): DefaultCommandError =
    new DefaultCommandError(message, code, originalDocument)

  private[reactivemongo] def apply[P <: SerializationPack](pack: P)(
    _message: String,
    originalDocument: Option[pack.Document],
    _code: Option[Int]): CommandError =
    new CommandError {
      val code = _code
      val message = _message

      override def getMessage: String =
        s"CommandError['$message'" + code.map(c => " (code = " + c + ")").getOrElse("") + "]" +
          originalDocument.map(doc => " with original doc " + pack.pretty(doc)).getOrElse("")
    }

  /**
   * Checks if the given document contains a 'ok' field which value equals 1, and produces a command error if not.
   *
   * @param doc The document of the response.
   * @param name The optional name of the command.
   * @param error A function that takes the document of the response and the optional name of the command as arguments, and produces a command error.
   */
  def checkOk(
    doc: BSONDocument, name: Option[String],
    error: (BSONDocument, Option[String]) => CommandError = (doc, name) => CommandError("command " + name.map(_ + " ").getOrElse("") + "failed because the 'ok' field is missing or equals 0", Some(doc))): Option[CommandError] = {
    doc.getAs[BSONNumberLike]("ok").map(_.toInt).orElse(Some(0)).flatMap {
      case 1 => None
      case _ => Some(error(doc, name))
    }
  }
}

/**
 * A default command error, which may contain the original BSONDocument of the response.
 *
 * @param message The error message.
 * @param code The optional error code.
 * @param originalDocument The original BSONDocument of this error.
 */
private[reactivemongo] final class DefaultCommandError( // TODO: Remove
  val message: String,
  val code: Option[Int],
  val originalDocument: Option[BSONDocument]) extends BSONCommandError

/** A command error that optionally holds the original TraversableBSONDocument; TODO: Remove */
private[reactivemongo] trait BSONCommandError extends CommandError {
  private[reactivemongo] val originalDocument: Option[BSONDocument]

  override def getMessage: String =
    s"BSONCommandError['$message'" + code.map(c => " (code = " + c + ")").getOrElse("") + "]" +
      originalDocument.map(doc => " with original doc " + BSONDocument.pretty(doc)).getOrElse("")
}
