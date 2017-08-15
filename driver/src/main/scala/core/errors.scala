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

import reactivemongo.bson._
import DefaultBSONHandlers._

/** An error that can come from a MongoDB node or not. */
trait ReactiveMongoException extends Exception {
  /** explanation message */
  def message: String

  override def getMessage: String = s"MongoError['$message']"
}

object ReactiveMongoException {
  def apply(message: String): ReactiveMongoException =
    GenericDriverException(message)

  def apply(doc: BSONDocument): DatabaseException =
    new DetailedDatabaseException(doc)
}

/** An error thrown by a MongoDB node. */
trait DatabaseException extends ReactiveMongoException {
  /** original document of this error */
  def originalDocument: Option[BSONDocument]

  /** error code */
  def code: Option[Int]

  override def getMessage: String = s"DatabaseException['$message'" + code.map(c => s" (code = $c)").getOrElse("") + "]"

  /** Tells if this error is due to a write on a secondary node. */
  def isNotAPrimaryError: Boolean = code.map {
    case 10054 | 10056 | 10058 | 10107 | 13435 | 13436 => true
    case _ => false
  }.getOrElse(false)

  /** Tells if this error is related to authentication issues. */
  def isUnauthorized: Boolean = code.map {
    case 10057 | 15845 | 16550 => true
    case _                     => false
  }.getOrElse(false)
}

/** A driver-specific error */
trait DriverException extends ReactiveMongoException {
  protected def cause: Throwable = null
  override def getCause = cause
}

/** A generic driver error. */
case class GenericDriverException(
  message: String) extends DriverException with NoStackTrace

sealed class ConnectionNotInitialized(
  val message: String,
  override val cause: Throwable) extends DriverException
  with Product with java.io.Serializable with Serializable with Equals {

  @deprecated(message = "Use constructor with cause", since = "0.12-RC1")
  def this(message: String) = this(message, null)

  override val productPrefix = "ConnectionNotInitialized"

  def productElement(i: Int): Any = i match {
    case 0 => message
    case 1 => cause
    case _ => throw new NoSuchElementException
  }

  override def productIterator: Iterator[Any] = Iterator(message, cause)

  val productArity = 2

  override lazy val hashCode = (message -> cause).hashCode

  override def equals(that: Any): Boolean = that match {
    case x: ConnectionNotInitialized =>
      (message -> cause) == (x.message -> x.cause)

    case _ => false
  }

  def canEqual(other: Any): Boolean = other match {
    case _: ConnectionNotInitialized => true
    case _                           => false
  }

  @deprecated(message = "Use constructor with cause", since = "0.12-RC1")
  def copy(message: String): ConnectionNotInitialized =
    new ConnectionNotInitialized(message, this.cause)
}

object ConnectionNotInitialized {
  @deprecated(message = "Use constructor with cause", since = "0.12-RC1")
  def apply(message: String): ConnectionNotInitialized =
    new ConnectionNotInitialized(message, null)

  @deprecated(message = "Use constructor with cause", since = "0.12-RC1")
  def unapply(instance: ConnectionNotInitialized): Option[String] =
    Some(instance.message)

  def MissingMetadata(cause: Throwable): ConnectionNotInitialized =
    new ConnectionNotInitialized("Connection is missing metadata (like protocol version, etc.) The connection pool is probably being initialized.", cause)

  @deprecated(message = "Use constructor with cause", since = "0.12-RC1")
  def MissingMetadata: ConnectionNotInitialized = MissingMetadata(null)
}

case class ConnectionException(message: String) extends DriverException

/** A generic error thrown by a MongoDB node. */
case class GenericDatabaseException(
  message: String,
  code: Option[Int]) extends DatabaseException {
  val originalDocument = None
}

/** An error thrown by a MongoDB node (containing the original document of the error). */
class DetailedDatabaseException(
  doc: BSONDocument) extends DatabaseException with NoStackTrace {

  val originalDocument = Some(doc)
  lazy val message = doc.getAs[BSONString]("$err").map(_.value).getOrElse("$err is not present, unknown error")
  lazy val code = doc.getAs[BSONInteger]("code").map(_.value)
}
