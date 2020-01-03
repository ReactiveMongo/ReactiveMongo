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
package reactivemongo.core.commands

import reactivemongo.api.{
  BSONSerializationPack,
  ReadPreference,
  SerializationPack
}

import reactivemongo.bson.{ BSONDocument, BSONNumberLike }
import reactivemongo.core.errors.ReactiveMongoException
import reactivemongo.core.protocol.{ RequestMaker, Query, QueryFlags, Response }
import reactivemongo.core.netty._

@deprecated("consider using reactivemongo.api.commands instead", "0.11.0")
object `package` {}

/**
 * A MongoDB Command.
 *
 * Basically, it's as query that is performed on any db.\$cmd collection
 * and gives back one document as a result.
 *
 * @param Result This command's result type.
 */
trait Command[Result] {
  /**
   * Deserializer for this command's result.
   */
  val ResultMaker: CommandResultMaker[Result]

  /**
   * States if this command can be run on secondaries.
   */
  def slaveOk: Boolean = false

  /**
   * Makes the `BSONDocument` for documents that will be send as body of this command's query.
   */
  def makeDocuments: BSONDocument

  /**
   * Produces a [[reactivemongo.core.commands.MakableCommand]] instance of this command.
   *
   * @param db name of the target database.
   */
  def apply(db: String): MakableCommand = new MakableCommand(db, this)
}

/**
 * Handler for deserializing commands results.
 *
 * @tparam Result The result type of this command.
 */
trait CommandResultMaker[Result] {
  protected type Pack <: SerializationPack

  protected val pack: Pack

  /**
   * Deserializes the given response into an instance of Result.
   */
  def apply(response: Response): Either[CommandError, Result] = {
    lazy val document: pack.Document = response match {
      case Response.CommandError(_, _, _, cause) =>
        cause.originalDocument match {
          case pack.IsDocument(doc) =>
            doc

          case Some(doc: reactivemongo.bson.BSONDocument) =>
            pack.document(doc) // TODO#1.1: Remove after release 1.0

          case _ => throw cause
        }

      case _ => Response.parse[pack.type](pack)(response).next()
    }

    try {
      apply(document)
    } catch {
      case e: CommandError => Left(e)

      case e: Throwable =>
        val error = CommandError(pack)(
          _message = "exception while deserializing this command's result!",
          originalDocument = Some(document),
          _code = None)

        error.initCause(e)
        Left(error)
    }
  }

  /**
   * Deserializes the given document into an instance of Result.
   */
  protected def apply(document: pack.Document): Either[CommandError, Result]
}

trait BSONCommandResultMaker[Result] extends CommandResultMaker[Result] {
  protected type Pack = BSONSerializationPack.type

  protected val pack: Pack = BSONSerializationPack

  final override def apply(response: Response): Either[CommandError, Result] =
    super.apply(response)

  /**
   * Deserializes the given document into an instance of Result.
   */
  def apply(document: BSONDocument): Either[CommandError, Result]
}

/** A generic command error. */
trait CommandError extends ReactiveMongoException {
  /** error code */
  val code: Option[Int]

  override def getMessage: String = s"CommandError['$message'" + code.map(c => " (code = " + c + ")").getOrElse("") + "]"
}

/** A command error that optionally holds the original TraversableBSONDocument */
trait BSONCommandError extends CommandError {
  val originalDocument: Option[BSONDocument]

  override def getMessage: String =
    s"BSONCommandError['$message'" + code.map(c => " (code = " + c + ")").getOrElse("") + "]" +
      originalDocument.map(doc => " with original doc " + BSONDocument.pretty(doc)).getOrElse("")
}

object CommandError {
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
class DefaultCommandError(
  val message: String,
  val code: Option[Int],
  val originalDocument: Option[BSONDocument]) extends BSONCommandError

/**
 * A makable command, that can produce a request maker ready to be sent to a [[reactivemongo.core.actors.MongoDBSystem]] actor.
 *
 * @param db Database name.
 * @param command Subject command.
 */
class MakableCommand(val db: String, val command: Command[_]) {
  /**
   * Produces the `reactivemongo.core.protocol.Query` instance for the given command.
   */
  def makeQuery: Query = Query(if (command.slaveOk) QueryFlags.SlaveOk else 0, db + ".$cmd", 0, 1)

  /**
   * Returns the [[reactivemongo.core.protocol.RequestMaker]] for the given command.
   */
  def maker = RequestMaker(
    makeQuery,
    BufferSequence.single(BSONSerializationPack)(command.makeDocuments))

  /**
   * Returns the [[reactivemongo.core.protocol.RequestMaker]] for the given command, using the given ReadPreference.
   */
  def maker(readPreference: ReadPreference) = {
    val query = makeQuery
    val flags = {
      if (readPreference.slaveOk) query.flags | QueryFlags.SlaveOk
      else query.flags
    }

    RequestMaker(
      query.copy(flags = flags),
      BufferSequence.single(BSONSerializationPack)(command.makeDocuments),
      readPreference)
  }
}
