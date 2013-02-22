package reactivemongo.core.commands

import reactivemongo.bson._
import DefaultBSONHandlers._
import reactivemongo.core.errors._
import reactivemongo.core.protocol.{RequestMaker, Query, QueryFlags, Response}
import reactivemongo.core.protocol.NodeState
import reactivemongo.core.protocol.NodeState._
import reactivemongo.core.netty._
import reactivemongo.utils._

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
  val ResultMaker :CommandResultMaker[Result]

  /**
   * States if this command can be run on secondaries.
   */
  def slaveOk :Boolean = false

  /**
   * Makes the `BSONDocument` for documents that will be send as body of this command's query.
   */
  def makeDocuments :BSONDocument

  /**
   * Produces a [[reactivemongo.core.commands.MakableCommand]] instance of this command.
   *
   * @param db name of the target database.
   */
  def apply(db: String) :MakableCommand = new MakableCommand(db, this)
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
  def apply(response: Response) :Either[CommandError, Result]
}

trait BSONCommandResultMaker[Result] extends CommandResultMaker[Result] {
  final def apply(response: Response) :Either[CommandError, Result] = {
    val document = Response.parse(response).next()
    try {
      apply(document)
    } catch {
      case e :CommandError => Left(e)
      case e: Throwable =>
        val error = CommandError("exception while deserializing this command's result!", Some(document))
        error.initCause(e);
        Left(error)
    }
  }

  /**
   * Deserializes the given document into an instance of Result.
   */
  def apply(document: BSONDocument) :Either[CommandError, Result]
}

/**
 * A command that targets the ''admin'' database only (administrative commands).
 */
trait AdminCommand[Result] extends Command[Result] {
  /**
   * As and admin command targets only the ''admin'' database, @param db will be ignored.
   * @inheritdoc
   */
  override def apply(db: String) :MakableCommand = apply()

  /**
   * Produces a [[reactivemongo.core.commands.MakableCommand]] instance of this command.
   */
  def apply() :MakableCommand = new MakableCommand("admin", this)
}

/** A generic command error. */
trait CommandError extends ReactiveMongoError {
  /** error code */
  val code: Option[Int]

  override def getMessage :String = "CommandError['" + message + "'" + code.map(c => " (code = " + c + ")").getOrElse("") + "]"
}

/** A command error that optionally holds the original TraversableBSONDocument */
trait BSONCommandError extends CommandError {
  val originalDocument: Option[BSONDocument]

  override def getMessage :String =
    "BSONCommandError['" + message + "'" + code.map(c => " (code = " + c + ")").getOrElse("") + "]" +
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
  def apply(message: String, originalDocument: Option[BSONDocument] = None, code: Option[Int] = None) :DefaultCommandError =
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
    error: (BSONDocument, Option[String]) => CommandError = (doc, name) => CommandError("command " + name.map(_ + " ").getOrElse("") + "failed because the 'ok' field is missing or equals 0", Some(doc))
  ) :Option[CommandError] = {
    doc.getAs[BSONDouble]("ok").map(_.value).orElse(Some(0)).flatMap {
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
  val originalDocument: Option[BSONDocument]
) extends BSONCommandError

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
  def makeQuery :Query = Query(if(command.slaveOk) QueryFlags.SlaveOk else 0, db + ".$cmd", 0, 1)
  /**
   * Returns the [[reactivemongo.core.protocol.RequestMaker]] for the given command.
   */
  def maker = RequestMaker(makeQuery, BufferSequence(command.makeDocuments.makeBuffer))
}

case class RawCommand(bson: BSONDocument) extends Command[BSONDocument] {
  def makeDocuments = bson

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
 * @param awaitJournalCommit Make sure that the previous operation has been committed into the journal. Journaling must be enabled on the servers.
 * @param waitForReplicatedOn Make sure that the previous (write) operation has been run on at least ''n'' replicas, with ''n'' = waitReplicatedOn.get
 * @param fsync Make sure that the previous (write) operation has been written on the disk.
 */
case class GetLastError(
  awaitJournalCommit: Boolean = false,
  waitForReplicatedOn: Option[Int] = None,
  fsync: Boolean = false
) extends Command[LastError] {
  override def makeDocuments =
    BSONDocument(
      "getlasterror"        -> BSONInteger(1),
      "waitForReplicatedOn" -> waitForReplicatedOn.map(w => BSONInteger(w)),
      "fsync"               -> option(fsync, BSONBoolean(true)),
      "j"                   -> option(awaitJournalCommit, BSONBoolean(true))
    )

  val ResultMaker = LastError
}

/**
 * Result of the [[reactivemongo.core.commands.GetLastError]] command.
 *
 * @param ok true if the last operation was successful
 * @param err the err field, if any
 * @param code the error code, if any
 * @param errMsg the message (often regarding an error) if any
 * @param original the whole map resulting of the deserialization of the response with the [[reactivemongo.bson.handlers.DefaultBSONHandlers]].
 */
case class LastError(
  ok: Boolean,
  err: Option[String],
  code: Option[Int],
  errMsg: Option[String],
  originalDocument: Option[BSONDocument]
) extends DBError {
  /** states if the last operation ended up with an error */
  lazy val inError :Boolean = !ok || err.isDefined
  lazy val stringify :String = toString + " [inError: " + inError + "]"

  lazy val message = err.orElse(errMsg).getOrElse("empty lastError message")
}

/**
 * Deserializer for [[reactivemongo.core.commands.GetLastError]] command result.
 */
object LastError extends BSONCommandResultMaker[LastError] {
  def apply(document: BSONDocument) = {
    Right(LastError(
      document.getAs[BSONDouble]("ok").map(_.value == 1).getOrElse(true),
      document.getAs[BSONString]("err").map(_.value),
      document.getAs[BSONInteger]("code").map(_.value),
      document.getAs[BSONString]("errmsg").map(_.value),
      Some(document)
    ))
  }
  def meaningful(response: Response) = {
    apply(response) match {
      case Left(e) => Left(e)
      case Right(lastError) =>
        if(lastError.inError)
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
case class Count(
  collectionName: String,
  query: Option[BSONDocument] = None,
  fields: Option[BSONDocument] = None
) extends Command[Int] {
  override def makeDocuments =
    BSONDocument(
      "count"  -> BSONString(collectionName),
      "query"  -> query,
      "fields" -> fields
    )

  val ResultMaker = Count
}

/**
 * Deserializer for the Count command. Basically returns an Int (number of counted documents)
 */
object Count extends BSONCommandResultMaker[Int] {
  def apply(document: BSONDocument) =
    CommandError.checkOk(document, Some("count")).toLeft(document.getAs[BSONDouble]("n").map(_.value.toInt).get)
}

/**
 * ReplSetGetStatus Command.
 *
 * Returns the state of the Replica Set from the target server's point of view.
 */
object ReplStatus extends AdminCommand[Map[String, BSONValue]] {
  override def makeDocuments = BSONDocument("replSetGetStatus" -> BSONInteger(1))

  object ResultMaker extends BSONCommandResultMaker[Map[String, BSONValue]] {
    def apply(document: BSONDocument) = Right(document.elements.toMap)
  }
}

/**
 * ServerStatus Command.
 *
 * Gets the detailed status of the target server.
 */
object Status extends AdminCommand[Map[String, BSONValue]] {
  override def makeDocuments = BSONDocument("serverStatus" -> BSONInteger(1))

  object ResultMaker extends BSONCommandResultMaker[Map[String, BSONValue]] {
    def apply(document: BSONDocument) = Right(document.elements.toMap)
  }
}

/**
 * Getnonce Command.
 *
 * Gets a nonce for authentication token.
 */
object Getnonce extends Command[String] {
  override def makeDocuments = BSONDocument("getnonce" -> BSONInteger(1))

  object ResultMaker extends BSONCommandResultMaker[String] {
    def apply(document: BSONDocument) = {
      CommandError.checkOk(document, Some("getnonce")).toLeft(document.getAs[BSONString]("nonce").get.value)
    }
  }
}

/**
 * Authenticate Command.
 *
 * @param user username
 * @param password user's password
 * @param nonce the previous nonce given by the server
 */
case class Authenticate(user: String, password: String, nonce: String) extends Command[SuccessfulAuthentication] {
  import Converters._
  /** the computed digest of the password */
  lazy val pwdDigest = md5Hex(user + ":mongo:" + password)
  /** the digest of the tuple (''nonce'', ''user'', ''pwdDigest'') */
  lazy val key = md5Hex(nonce + user + pwdDigest)

  override def makeDocuments = BSONDocument("authenticate" -> BSONInteger(1), "user" -> BSONString(user), "nonce" -> BSONString(nonce), "key" -> BSONString(key))

  val ResultMaker = Authenticate
}

/** Authentication command's response deserializer. */
object Authenticate extends BSONCommandResultMaker[SuccessfulAuthentication] {
  def apply(document: BSONDocument) = {
    CommandError.checkOk(document, Some("authenticate"), (doc, name) => {
      FailedAuthentication(doc.getAs[BSONString]("errmsg").map(_.value).getOrElse(""), Some(doc))
    }).toLeft(document.get("dbname") match {
      case Some(BSONString(dbname)) => VerboseSuccessfulAuthentication(
        dbname,
        document.get("user").get.asInstanceOf[BSONString].value,
        document.get("readOnly").flatMap {
          case BSONBoolean(value) => Some(value)
          case _ => Some(false)
        }.get
      )
      case _ => SilentSuccessfulAuthentication
    })
  }
}

/** an authentication result */
sealed trait AuthenticationResult

/** A successful authentication result. */
sealed trait SuccessfulAuthentication extends AuthenticationResult

/** A silent successful authentication result (MongoDB <= 2.0).*/
object SilentSuccessfulAuthentication extends SuccessfulAuthentication

/**
 * A verbose successful authentication result (MongoDB >= 2.2).
 *
 * Previous versions of MongoDB only return ok = BSONDouble(1.0).
 *
 * @param db database name
 * @param user username
 * @param readOnly states if the authentication gives us only the right to read from the database.
 */
case class VerboseSuccessfulAuthentication(
  db: String,
  user: String,
  readOnly: Boolean
) extends SuccessfulAuthentication

/**
 * A failed authentication result
 *
 * @param message the explanation of the error.
 */
case class FailedAuthentication(
  message: String,
  originalDocument :Option[BSONDocument]
) extends BSONCommandError with AuthenticationResult {
  val code = None
}

/**
 * IsMaster Command.
 *
 * States if the target server is a primary.
 * This command also gives some useful information, like the other nodes in the replica set.
 */
object IsMaster extends AdminCommand[IsMasterResponse] {
  def makeDocuments = BSONDocument("isMaster" -> BSONInteger(1))

  object ResultMaker extends BSONCommandResultMaker[IsMasterResponse] {
    def apply(document: BSONDocument) = {
      CommandError.checkOk(document, Some("isMaster")).toLeft(IsMasterResponse(
        document.getAs[BSONBoolean]("ismaster").map(_.value).getOrElse(false),
        document.getAs[BSONBoolean]("secondary").map(_.value).getOrElse(false),
        document.getAs[BSONInteger]("maxBsonObjectSize").map(_.value).getOrElse(16777216),
        document.getAs[BSONString]("setName").map(_.value),
        document.getAs[BSONArray]("hosts").map(
          _.values.map(_.asInstanceOf[BSONString].value).toList
        ),
        document.getAs[BSONString]("me").map(_.value)
      ))
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
case class IsMasterResponse(
  isMaster: Boolean,
  secondary: Boolean,
  maxBsonObjectSize: Int,
  setName: Option[String],
  hosts: Option[List[String]],
  me: Option[String]
) {
  /** the resolved [[reactivemongo.core.protocol.NodeState]] of the answering server */
  lazy val state :NodeState = if(isMaster) PRIMARY else if(secondary) SECONDARY else UNKNOWN
}

/** A modify operation, part of a FindAndModify command */
sealed trait Modify {
  protected[commands] def toDocument :BSONDocument
}

/**
 * Update (part of a FindAndModify command).
 *
 * @param update the modifier document.
 * @param fetchNewObject the command result must be the new object instead of the old one.
 */
case class Update(update: BSONDocument, fetchNewObject: Boolean) extends Modify {
  override def toDocument = BSONDocument(
    "update" -> update,
    "new"    -> BSONBoolean(fetchNewObject)
  )
}

/** Remove (part of a FindAndModify command). */
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
case class FindAndModify(
  collection: String,
  query: BSONDocument,
  modify: Modify,
  upsert: Boolean = false,
  sort: Option[BSONDocument] = None,
  fields: Option[BSONDocument] = None
) extends Command[Option[BSONDocument]] {
  override def makeDocuments =
    BSONDocument(
      "findAndModify" -> BSONString(collection),
      "query" -> query,
      "sort" -> sort,
      "fields" -> fields,
      "upsert" -> option(upsert, BSONBoolean(true))) ++ modify.toDocument

  val ResultMaker = FindAndModify
}

/**
 * FindAndModify command deserializer
 * @todo [[reactivemongo.bson.handlers.BSONReader]][T] typeclass
 */
object FindAndModify extends BSONCommandResultMaker[Option[BSONDocument]] {
  def apply(document: BSONDocument) =
    CommandError.checkOk(document, Some("findAndModify")).toLeft(document.getAs[BSONDocument]("value"))
}

case class DeleteIndex(
  collection: String,
  index: String
) extends Command[Int] {
  override def makeDocuments = BSONDocument(
    "deleteIndexes" -> BSONString(collection),
    "index" -> BSONString(index))

  object ResultMaker extends BSONCommandResultMaker[Int] {
    def apply(document: BSONDocument) =
      CommandError.checkOk(document, Some("deleteIndexes")).toLeft(document.getAs[BSONDouble]("nIndexWas").map(_.value.toInt).get)
  }
}