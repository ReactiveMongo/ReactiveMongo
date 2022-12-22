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

import scala.util.control.NoStackTrace

import reactivemongo.api.SerializationPack
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.BSONSerializationPack

/** An error that can come from a MongoDB node or not. */
sealed trait ReactiveMongoException extends Exception {

  /** explanation message */
  def message: String

  override def getMessage: String = s"MongoError['$message']"
}

/** An error thrown by a MongoDB node. */
trait DatabaseException extends ReactiveMongoException {

  /** Representation of the original document of this error */
  private[reactivemongo] def originalDocument: Option[String]

  /** error code */
  def code: Option[Int]

  override def getMessage: String =
    s"DatabaseException['$message'" + code.fold("")(c => s" (code = $c)") + "]"

  /** Tells if this error is due to a write on a secondary node. */
  private[reactivemongo] final def isNotAPrimaryError: Boolean =
    code.fold(false) {
      case 10054 | 10056 | 10058 | 10107 | 13435 | 13436 => true
      case _                                             => false
    }

  /** Tells if this error is related to authentication issues. */
  private[reactivemongo] final def isUnauthorized: Boolean = code.fold(false) {
    case 10057 | 15845 | 16550 => true
    case _                     => false
  }

  override def equals(that: Any): Boolean = that match {
    case other: DatabaseException =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString: String = getMessage

  protected lazy val tupled: Product = originalDocument -> code
}

private[reactivemongo] object DatabaseException {
  def apply(cause: Throwable): DatabaseException = new DefaultException(cause)

  def apply[P <: SerializationPack](
      pack: P
    )(doc: pack.Document
    ): DatabaseException = new DatabaseException with NoStackTrace {
    private lazy val decoder = pack.newDecoder

    lazy val originalDocument = Option(doc).map(pack.pretty(_))

    lazy val message = decoder
      .string(doc, f"$$err")
      .orElse {
        decoder.string(doc, "errmsg")
      }
      .getOrElse(s"message is not present, unknown error: ${pack pretty doc}")

    lazy val code = decoder.int(doc, "code")
  }

  // ---

  private final class DefaultException(
      val cause: Throwable)
      extends DatabaseException
      with NoStackTrace {

    val originalDocument = Option.empty[Nothing]
    val code = Option.empty[Int]
    val message = s"${cause.getClass.getName}: ${cause.getMessage}"
  }
}

/** A driver-specific error */
private[reactivemongo] trait DriverException extends ReactiveMongoException {
  protected def cause: Throwable = null
  @inline override def getCause = cause
}

/** A generic driver error. */
@SuppressWarnings(Array("NullAssignment"))
private[reactivemongo] final class GenericDriverException(
    val message: String,
    override val cause: Throwable = null)
    extends DriverException
    with NoStackTrace {

  override def equals(that: Any): Boolean = that match {
    case other: GenericDriverException =>
      (this.message == null && other.message == null) || (this.message != null && this.message
        .equals(other.message))

    case _ =>
      false
  }

  @SuppressWarnings(Array("NullParameter"))
  override def hashCode: Int = if (message == null) -1 else message.hashCode

  override def toString = s"GenericDriverException($message)"
}

private[reactivemongo] final class ConnectionException(
    val message: String)
    extends DriverException {

  override def equals(that: Any): Boolean = that match {
    case other: ConnectionException =>
      (this.message == null && other.message == null) || (this.message != null && this.message
        .equals(other.message))

    case _ =>
      false
  }

  @SuppressWarnings(Array("NullParameter"))
  override def hashCode: Int = if (message == null) -1 else message.hashCode

  override def toString = s"ConnectionException($message)"
}

/** A generic error thrown by a MongoDB node. */
private[reactivemongo] final class GenericDatabaseException(
    val message: String,
    val code: Option[Int])
    extends DatabaseException {

  val originalDocument = None

  override protected lazy val tupled = message -> code

  override def equals(that: Any): Boolean = that match {
    case other: GenericDatabaseException =>
      this.tupled == other.tupled

    case _ =>
      false
  }
}

/** A generic command error. */
private[reactivemongo] trait CommandException extends DatabaseException {

  /** error code */
  val code: Option[Int]

  override def getMessage: String =
    s"CommandException['$message'" + code.fold("")(" (code = " + _ + ')') + ']'
}

private[reactivemongo] object CommandException {

  @inline def apply(
      message: String,
      originalDocument: Option[BSONDocument] = None,
      code: Option[Int] = None
    ): CommandException =
    CommandException(BSONSerializationPack)(message, originalDocument, code)

  def apply[P <: SerializationPack](
      pack: P
    )(_message: String,
      _originalDocument: Option[pack.Document],
      _code: Option[Int]
    ): CommandException =
    new CommandException {
      lazy val originalDocument = _originalDocument.map(pack.pretty)
      val code = _code
      val message = _message

      override def getMessage: String = s"CommandException['$message'" +
        code.fold("")(" (code = " + _ + ')') + ']' +
        originalDocument.fold("")(" with original document " + _)
    }
}
