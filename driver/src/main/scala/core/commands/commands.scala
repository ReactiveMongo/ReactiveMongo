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

import scala.util.{ Try, Failure }

import reactivemongo.api.ReadPreference
import reactivemongo.bson.{
  BSONArray,
  BSONBoolean,
  BSONBooleanLike,
  BSONDocument,
  BSONElement,
  BSONInteger,
  BSONNumberLike,
  BSONReader,
  BSONString,
  BSONValue
}
import reactivemongo.bson.exceptions.DocumentKeyNotFound
import reactivemongo.core.errors.{ DatabaseException, ReactiveMongoException }
import reactivemongo.core.protocol.{ RequestMaker, Query, QueryFlags, Response }
import reactivemongo.core.netty._
import reactivemongo.util.option
import reactivemongo.core.nodeset.NodeStatus

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
  /**
   * Deserializes the given response into an instance of Result.
   */
  def apply(response: Response): Either[CommandError, Result]
}

trait BSONCommandResultMaker[Result] extends CommandResultMaker[Result] {
  final def apply(response: Response): Either[CommandError, Result] = {
    val document = Response.parse(response).next()
    try {
      apply(document)
    } catch {
      case e: CommandError => Left(e)
      case e: Throwable =>
        val error = CommandError("exception while deserializing this command's result!", Some(document))
        error.initCause(e);
        Left(error)
    }
  }

  /**
   * Deserializes the given document into an instance of Result.
   */
  def apply(document: BSONDocument): Either[CommandError, Result]
}

/**
 * A command that targets the ''admin'' database only (administrative commands).
 */
trait AdminCommand[Result] extends Command[Result] {
  /**
   * As and admin command targets only the ''admin'' database, @param db will be ignored.
   * @inheritdoc
   */
  override def apply(db: String): MakableCommand = apply()

  /**
   * Produces a [[reactivemongo.core.commands.MakableCommand]] instance of this command.
   */
  def apply(): MakableCommand = new MakableCommand("admin", this)
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
   * Produces the [[reactivemongo.core.protocol.Query]] instance for the given command.
   */
  def makeQuery: Query = Query(if (command.slaveOk) QueryFlags.SlaveOk else 0, db + ".$cmd", 0, 1)

  /**
   * Returns the [[reactivemongo.core.protocol.RequestMaker]] for the given command.
   */
  def maker = RequestMaker(
    makeQuery,
    BufferSequence.single(command.makeDocuments))

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
      BufferSequence.single(command.makeDocuments), readPreference)
  }
}

@deprecated("consider using `rawCommand` on [[reactivemongo.api.collections.GenericCollection.runner]] instead", "0.11.0")
case class RawCommand(bson: BSONDocument) extends Command[BSONDocument] {
  val makeDocuments = bson

  object ResultMaker extends BSONCommandResultMaker[BSONDocument] {
    def apply(document: BSONDocument) = CommandError.checkOk(document, None).toLeft(document)
  }
}

/**
 * GetLastError Command.
 *
 * This command is used to check the status of the immediately previous operation.
 * It is commonly used to make sure that a write request has been effectively done (so it is also known as ''writeConcern'').
 * This command will return only when the previous operation is complete and matches its parameters
 * (for example, with waitForReplicatedOn set to Some(2), this command will return only when at least two replicas have also run the previous operation).
 *
 * @param j Make sure that the previous operation has been committed into the journal. Journaling must be enabled on the servers.
 * @param w Specify the level of replication for a write operation. Should be a BSONString or BSONInteger. See [[http://docs.mongodb.org/manual/reference/command/getLastError/#dbcmd.getLastError the MongoDB documentation]].
 * @param wtimeout Write propagation timeout (in milliseconds). See [[http://docs.mongodb.org/manual/reference/command/getLastError/#dbcmd.getLastError the MongoDB documentation]].
 * @param fsync Make sure that the previous (write) operation has been written on the disk.
 */
@deprecated("consider using reactivemongo.api.commands.GetLastError instead", "0.11.0")
case class GetLastError(
  j: Boolean = false,
  w: Option[BSONValue] = None,
  wtimeout: Int = 0,
  fsync: Boolean = false) extends Command[LastError] {
  override def makeDocuments =
    BSONDocument(
      "getlasterror" -> BSONInteger(1),
      "w" -> w,
      "wtimeout" -> Option(wtimeout).filter(_ > 0),
      "fsync" -> option(fsync, BSONBoolean(true)),
      "j" -> option(j, BSONBoolean(true)))

  val ResultMaker = LastError
}

/**
 * Result of the [[reactivemongo.core.commands.GetLastError GetLastError]] command.
 *
 * @param ok True if the last operation was successful
 * @param err The err field, if any
 * @param code The error code, if any
 * @param errMsg The message (often regarding an error) if any
 * @param originalDocument The whole map resulting of the deserialization of the response with the [[reactivemongo.bson.DefaultBSONHandlers DefaultBSONHandlers]].
 * @param updated The number of documents affected by last command, 0 if none
 * @param updatedExisting When true, the last update operation resulted in change of existing documents
 */
@deprecated("consider using reactivemongo.api.commands.WriteResult instead", "0.11.0")
case class LastError(
  ok: Boolean,
  err: Option[String],
  code: Option[Int],
  errMsg: Option[String],
  originalDocument: Option[BSONDocument],
  updated: Int,
  updatedExisting: Boolean) extends DatabaseException {
  /** States if the last operation ended up with an error */
  lazy val inError: Boolean = !ok || err.isDefined
  lazy val stringify: String = s"toString [inError: $inError]"

  lazy val message = err.orElse(errMsg).getOrElse("empty lastError message")

  /** Alias for [[reactivemongo.core.commands.LastError#updated updated]] to also support the short MongoDB syntax */
  def n: Int = updated

  /** Returns a `Stream` corresponding to the stream of the [[reactivemongo.core.commands.LastError#originalDocument originalDocument]] if present, otherwise an empty `Stream`. */
  def stream: Stream[Try[BSONElement]] = originalDocument.map(_.stream).getOrElse(Stream.empty[Try[BSONElement]])

  /** Returns a `Stream` for all the elements of the [[reactivemongo.core.commands.LastError#originalDocument originalDocument]] if present, otherwise an empty `Stream`. */
  def elements: Stream[BSONElement] = originalDocument.map(_.elements).getOrElse(Stream.empty[BSONElement])

  /**
   * Returns the [[reactivemongo.bson.BSONValue]] associated with the given `key` of the [[reactivemongo.core.commands.LastError#originalDocument originalDocument]].
   *
   * If the key is not found or the matching value cannot be deserialized, returns `None`.
   */
  def get(key: String): Option[BSONValue] = originalDocument.flatMap(_.get(key))

  /**
   * Returns the [[reactivemongo.bson.BSONValue]] associated with the given `key` of the [[reactivemongo.core.commands.LastError#originalDocument originalDocument]].
   *
   * If the key is not found or the matching value cannot be deserialized, returns a `Failure`.
   * The `Failure` holds a [[reactivemongo.bson.exceptions.DocumentKeyNotFound DocumentKeyNotFound]] if the key could not be found.
   */
  def getTry(key: String): Try[BSONValue] = originalDocument.map(_.getTry(key)).getOrElse(Failure(DocumentKeyNotFound(key)))

  /**
   * Returns the [[reactivemongo.bson.BSONValue]] associated with the given `key` of the [[reactivemongo.core.commands.LastError#originalDocument originalDocument]].
   *
   * If the key could not be found, the resulting option will be `None`.
   * If the matching value could not be deserialized, returns a `Failure`.
   */
  def getUnflattenedTry(key: String): Try[Option[BSONValue]] = originalDocument.map(_.getUnflattenedTry(key)).getOrElse(Failure(DocumentKeyNotFound(key)))

  /**
   * Returns the [[reactivemongo.bson.BSONValue]] associated with the given `key` of the [[reactivemongo.core.commands.LastError#originalDocument originalDocument]],
   * and converts it with the given implicit [[reactivemongo.bson.BSONReader BSONReader]].
   *
   * If there is no matching value, or the value could not be deserialized or converted, returns a `None`.
   */
  def getAs[T](s: String)(implicit reader: BSONReader[_ <: BSONValue, T]): Option[T] = originalDocument.flatMap(_.getAs[T](s))

  /**
   * Returns the [[reactivemongo.bson.BSONValue]] associated with the given `key` of the [[reactivemongo.core.commands.LastError#originalDocument originalDocument]],
   * and converts it with the given implicit [[reactivemongo.bson.BSONReader BSONReader]].
   *
   * If there is no matching value, or the value could not be deserialized or converted, returns a `Failure`.
   * The `Failure` holds a [[reactivemongo.bson.exceptions.DocumentKeyNotFound DocumentKeyNotFound]] if the key could not be found.
   */
  def getAsTry[T](s: String)(implicit reader: BSONReader[_ <: BSONValue, T]): Try[T] = originalDocument.map(_.getAsTry[T](s)).getOrElse(Failure(DocumentKeyNotFound(s)))

  /**
   * Returns the [[reactivemongo.bson.BSONValue]] associated with the given `key` of the [[reactivemongo.core.commands.LastError#originalDocument originalDocument]],
   * and converts it with the given implicit [[reactivemongo.bson.BSONReader BSONReader]].
   *
   * If there is no matching value, returns a `Success` holding `None`.
   * If the value could not be deserialized or converted, returns a `Failure`.
   */
  def getAsUnflattenedTry[T](s: String)(implicit reader: BSONReader[_ <: BSONValue, T]): Try[Option[T]] = originalDocument.map(_.getAsUnflattenedTry[T](s)).getOrElse(Failure(DocumentKeyNotFound(s)))
}

/**
 * Deserializer for [[reactivemongo.core.commands.GetLastError GetLastError]] command result.
 */
@deprecated("consider using reactivemongo.api.commands.LastError instead", "0.11.0")
object LastError extends BSONCommandResultMaker[LastError] {
  def apply(document: BSONDocument) = {
    Right(LastError(
      document.getAs[BSONBooleanLike]("ok").fold(true)(_.toBoolean),
      document.getAs[BSONString]("err").map(_.value),
      document.getAs[BSONInteger]("code").map(_.value),
      document.getAs[BSONString]("errmsg").map(_.value),
      Some(document),
      document.getAs[BSONInteger]("n").map(_.value).getOrElse(0),
      document.getAs[BSONBoolean]("updatedExisting").map(_.value).getOrElse(false)))
  }
  def meaningful(response: Response) = {
    apply(response) match {
      case Left(e) => Left(e)
      case Right(lastError) =>
        if (lastError.inError)
          Left(lastError)
        else Right(lastError)
    }
  }
}

/**
 * The Count command.
 *
 * Returns a document containing the number of documents matching the query.
 * @param collectionName the name of the target collection
 * @param query the document selector
 * @param fields select only the matching fields
 */
@deprecated("consider using reactivemongo.api.commands.Count instead", "0.11.0")
case class Count(
  collectionName: String,
  query: Option[BSONDocument] = None,
  fields: Option[BSONDocument] = None) extends Command[Int] {
  override def makeDocuments =
    BSONDocument(
      "count" -> BSONString(collectionName),
      "query" -> query,
      "fields" -> fields)

  val ResultMaker = Count
}

/**
 * Deserializer for the Count command. Basically returns an Int (number of counted documents)
 */
@deprecated("consider using reactivemongo.api.commands.Count instead", "0.11.0")
object Count extends BSONCommandResultMaker[Int] {
  def apply(document: BSONDocument) =
    CommandError.checkOk(document, Some("count")).toLeft(document.getAs[BSONNumberLike]("n").map(_.toInt).get)
}

/**
 * ReplSetGetStatus Command.
 *
 * Returns the state of the Replica Set from the target server's point of view.
 */
@deprecated(
  "consider using reactivemongo.api.commands.ReplSetGetStatus", "0.11.5")
object ReplStatus extends AdminCommand[Map[String, BSONValue]] {
  override def makeDocuments = BSONDocument("replSetGetStatus" -> BSONInteger(1))

  object ResultMaker extends BSONCommandResultMaker[Map[String, BSONValue]] {
    def apply(document: BSONDocument) = Right(document.toMap)
  }
}

/**
 * ServerStatus Command.
 *
 * Gets the detailed status of the target server.
 */
@deprecated(
  "consider using reactivemongo.api.commands.ServerStatus", "0.11.5")
object Status extends AdminCommand[Map[String, BSONValue]] {
  override def makeDocuments = BSONDocument("serverStatus" -> BSONInteger(1))

  object ResultMaker extends BSONCommandResultMaker[Map[String, BSONValue]] {
    def apply(document: BSONDocument) = Right(document.toMap)
  }
}

/**
 * IsMaster Command.
 *
 * States if the target server is a primary.
 * This command also gives some useful information, like the other nodes in the replica set.
 */
@deprecated("consider using reactivemongo.api.commands.IsMasterCommand instead", "0.11.0")
object IsMaster extends AdminCommand[IsMasterResponse] {
  def makeDocuments = BSONDocument("isMaster" -> BSONInteger(1))

  object ResultMaker extends BSONCommandResultMaker[IsMasterResponse] {
    def apply(document: BSONDocument) = {
      CommandError.checkOk(document, Some("isMaster")).toLeft(IsMasterResponse(
        document.getAs[BSONBoolean]("ismaster").map(_.value).getOrElse(false),
        document.getAs[BSONBoolean]("secondary").map(_.value).getOrElse(false),
        document.getAs[BSONInteger]("maxBsonObjectSize").map(_.value).getOrElse(16777216), // TODO: default value should be 4M
        document.getAs[BSONString]("setName").map(_.value),
        document.getAs[BSONArray]("hosts").map(
          _.values.map(_.asInstanceOf[BSONString].value).toList),
        document.getAs[BSONString]("me").map(_.value),
        document.getAs[BSONDocument]("tags")))
    }
  }
}

/**
 * Deserialized IsMaster command response.
 *
 * @param isMaster states if the server is a primary
 * @param secondary states if the server is a secondary
 * @param maxBsonObjectSize the maximum document size allowed by the server
 * @param setName the name of the replica set, if any
 * @param hosts the names (''servername'':''port'') of the other nodes in the replica set, if any
 * @param me the name (''servername'':''port'') of the answering server
 */
@deprecated("consider using reactivemongo.api.commands.IsMasterCommand instead", "0.11.0")
case class IsMasterResponse(
  isMaster: Boolean,
  secondary: Boolean,
  maxBsonObjectSize: Int,
  setName: Option[String],
  hosts: Option[Seq[String]],
  me: Option[String],
  tags: Option[BSONDocument]) {
  /** the resolved [[reactivemongo.core.nodeset.NodeStatus]] of the answering server */
  val status: NodeStatus = if (isMaster) NodeStatus.Primary else if (secondary) NodeStatus.Secondary else NodeStatus.NonQueryableUnknownStatus
}

/** A modify operation, part of a FindAndModify command */
sealed trait Modify {
  protected[commands] def toDocument: BSONDocument
}

/**
 * Update (part of a FindAndModify command).
 *
 * @param update the modifier document.
 * @param fetchNewObject the command result must be the new object instead of the old one.
 */
@deprecated("consider using reactivemongo.api.commands.FindAndModifyCommand instead", "0.11.0")
case class Update(update: BSONDocument, fetchNewObject: Boolean) extends Modify {
  override def toDocument = BSONDocument(
    "update" -> update,
    "new" -> BSONBoolean(fetchNewObject))
}

/** Remove (part of a FindAndModify command). */
@deprecated("consider using reactivemongo.api.commands.FindAndModifyCommand instead", "0.11.0")
object Remove extends Modify {
  override def toDocument = BSONDocument("remove" -> BSONBoolean(true))
}

/**
 * FindAndModify command.
 *
 * This command allows to perform a modify operation (update/remove) matching a query, without the extra requests.
 * It returns the old document by default.
 *
 * @param collection the target collection name
 * @param query the filter for this command
 * @param modify the [[reactivemongo.core.commands.Modify]] operation to do
 * @param upsert states if a new document should be inserted if no match
 * @param sort the sort document
 * @param fields retrieve only a subset of the returned document
 */
@deprecated("consider using reactivemongo.api.commands.FindAndModifyCommand instead", "0.11.0")
case class FindAndModify(
  collection: String,
  query: BSONDocument,
  modify: Modify,
  upsert: Boolean = false,
  sort: Option[BSONDocument] = None,
  fields: Option[BSONDocument] = None) extends Command[Option[BSONDocument]] {
  override def makeDocuments =
    BSONDocument(
      "findAndModify" -> BSONString(collection),
      "query" -> query,
      "sort" -> sort,
      "fields" -> fields,
      "upsert" -> option(upsert, BSONBoolean(true))) ++ modify.toDocument

  val ResultMaker = FindAndModify
}

/** FindAndModify command deserializer */
@deprecated("consider using reactivemongo.api.commands.FindAndModifyCommand instead", "0.11.0")
object FindAndModify extends BSONCommandResultMaker[Option[BSONDocument]] {
  def apply(document: BSONDocument) =
    CommandError.checkOk(document, Some("findAndModify")).toLeft(document.getAs[BSONDocument]("value"))
}

@deprecated("consider using reactivemongo.api.commands.DropIndexes instead", "0.11.0")
case class DeleteIndex(
  collection: String,
  index: String) extends Command[Int] {
  override def makeDocuments = BSONDocument(
    "deleteIndexes" -> BSONString(collection),
    "index" -> BSONString(index))

  object ResultMaker extends BSONCommandResultMaker[Int] {
    def apply(document: BSONDocument) =
      CommandError.checkOk(document, Some("deleteIndexes")).toLeft(document.getAs[BSONNumberLike]("nIndexesWas").map(_.toInt).get)
  }
}
