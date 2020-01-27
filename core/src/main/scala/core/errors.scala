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

import reactivemongo.bson.{ BSONDocument, BSONInteger }
import reactivemongo.bson.DefaultBSONHandlers._

/** An error that can come from a MongoDB node or not. */
trait ReactiveMongoException extends Exception {
  /** explanation message */
  def message: String

  override def getMessage: String = s"MongoError['$message']"
}

object ReactiveMongoException {
  def apply(message: String): ReactiveMongoException =
    GenericDriverException(message)

  @deprecated("Use [[DatabaseException]]", "0.13.0")
  def apply(doc: BSONDocument) = DatabaseException(doc)
}

/** An error thrown by a MongoDB node. */
trait DatabaseException extends ReactiveMongoException {
  /** original document of this error */
  @deprecated("Internal: will be made private", "0.19.0")
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

  override def equals(that: Any): Boolean = that match {
    case other: DatabaseException =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString: String = getMessage

  @com.github.ghik.silencer.silent(".*Internal:\\ will\\ be\\ private.*")
  private lazy val tupled = originalDocument -> code
}

private[reactivemongo] object DatabaseException {
  @deprecated("Will be remove", "0.19.0")
  def apply(doc: BSONDocument): DatabaseException =
    new DetailedDatabaseException(doc)

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
trait DriverException extends ReactiveMongoException {
  protected def cause: Throwable = null
  override def getCause = cause
}

/** A generic driver error. */
class GenericDriverException private[core] (
  val message: String) extends DriverException with NoStackTrace
  with Product1[String] with Serializable {

  @deprecated("No longer a case class", "0.20.3")
  @inline def _1 = message

  @deprecated("No longer a case class", "0.20.3")
  def canEqual(that: Any): Boolean = that match {
    case _: GenericDriverException => true
    case _                         => false
  }

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

object GenericDriverException
  extends scala.runtime.AbstractFunction1[String, GenericDriverException] {

  def apply(message: String): GenericDriverException =
    new GenericDriverException(message)

  def unapply(exception: GenericDriverException): Option[String] =
    Option(exception).collect {
      case x if x.message != null => x.message
    }
}

sealed class ConnectionNotInitialized private[core] (
  val message: String,
  override val cause: Throwable) extends DriverException
  with Product with java.io.Serializable with Serializable with Equals {

  @deprecated("No longer a case class", "0.20.3")
  override val productPrefix = "ConnectionNotInitialized"

  @deprecated("No longer a case class", "0.20.3")
  def productElement(i: Int): Any = i match {
    case 0 => message
    case 1 => cause
    case _ => throw new NoSuchElementException
  }

  @deprecated("No longer a case class", "0.20.3")
  override def productIterator: Iterator[Any] = Iterator(message, cause)

  @deprecated("No longer a case class", "0.20.3")
  val productArity = 2

  override lazy val hashCode = (message -> cause).hashCode

  override def equals(that: Any): Boolean = that match {
    case x: ConnectionNotInitialized =>
      (message -> cause) == (x.message -> x.cause)

    case _ => false
  }

  @deprecated("No longer a case class", "0.20.3")
  def canEqual(other: Any): Boolean = other match {
    case _: ConnectionNotInitialized => true
    case _                           => false
  }
}

object ConnectionNotInitialized {
  def MissingMetadata(cause: Throwable): ConnectionNotInitialized = new ConnectionNotInitialized("Connection is missing metadata (like protocol version, etc.) The connection pool is probably being initialized.", cause)
}

class ConnectionException private[core] (val message: String)
  extends DriverException with Product1[String] with Serializable {

  @deprecated("No longer a case class", "0.20.3")
  @inline def _1 = message

  @deprecated("No longer a case class", "0.20.3")
  def canEqual(that: Any): Boolean = that match {
    case _: ConnectionException => true
    case _                      => false
  }

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

object ConnectionException
  extends scala.runtime.AbstractFunction1[String, ConnectionException] {

  def apply(message: String): ConnectionException =
    new ConnectionException(message)

  def unapply(exception: ConnectionException): Option[String] =
    Option(exception).collect {
      case x if x.message != null => x.message
    }
}

/** A generic error thrown by a MongoDB node. */
class GenericDatabaseException private[core] (
  val message: String,
  val code: Option[Int]) extends DatabaseException
  with Product2[String, Option[Int]] with Serializable {

  val originalDocument = None

  @deprecated("No longer a case class", "0.20.3")
  @inline def _1 = message

  @deprecated("No longer a case class", "0.20.3")
  @inline def _2 = code

  @deprecated("No longer a case class", "0.20.3")
  def canEqual(that: Any): Boolean = that match {
    case _: GenericDatabaseException => true
    case _                           => false
  }

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

object GenericDatabaseException extends scala.runtime.AbstractFunction2[String, Option[Int], GenericDatabaseException] {

  def apply(message: String, code: Option[Int]): GenericDatabaseException =
    new GenericDatabaseException(message, code)

  def unapply(exception: GenericDatabaseException): Option[(String, Option[Int])] = Option(exception).map(_.tupled)
}

/** An error thrown by a MongoDB node (containing the original document of the error). */
@deprecated("Will be remove", "0.19.0")
class DetailedDatabaseException(
  doc: BSONDocument) extends DatabaseException with NoStackTrace {

  type Document = BSONDocument

  val originalDocument = Some(doc)

  lazy val message = doc.getAs[String]("$err").orElse {
    doc.getAs[String]("errmsg")
  }.getOrElse(
    s"message is not present, unknown error: ${BSONDocument pretty doc}")

  lazy val code = doc.getAs[BSONInteger]("code").map(_.value)
}
