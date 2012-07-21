package org.asyncmongo.protocol.commands

import org.asyncmongo.bson._
import org.asyncmongo.handlers.DefaultBSONHandlers
import org.asyncmongo.protocol.{RequestMaker, Query, QueryFlags, Response}
import org.asyncmongo.protocol.NodeState
import org.asyncmongo.protocol.NodeState._
import org.asyncmongo.utils._

/**
 * A MongoDB Command.
 *
 * Basically, it's as query that is performed on any db.\$cmd collection
 * and gives back one document as a result.
 */
trait Command {
  /**
   * This command's result type.
   */
  type Result

  /**
   * Deserializer for this command's result.
   */
  val ResultMaker :CommandResultMaker[Result]

  /**
   * States if this command can be run on secondaries.
   */
  val slaveOk :Boolean = false

  /**
   * Makes the [[org.asyncmongo.bson.Bson]] for documents that will be send as body of this command's query.
   */
  def makeDocuments :Bson

  /**
   * Produces a [[org.asyncmongo.protocol.commands.MakableCommand]] instance of this command.
   *
   * @param db name of the target database.
   */
  def apply(db: String) :MakableCommand = new MakableCommand(db, this)
}

/**
 * Handler for deserializing commands results.
 *
 * @tparam Result the result type of this command.
 */
trait CommandResultMaker[Result] {
  /**
   * Deserializes the given response into an instance of Result.
   */
  def apply(response: Response) :Result
}

/**
 * A command that targets the ''admin'' database only (administrative commands).
 */
trait AdminCommand extends Command {
  /**
   * As and admin command targets only the ''admin'' database, @param db will be ignored.
   * @inheritdoc
   */
  override def apply(db: String) :MakableCommand = apply()

  /**
   * Produces a [[org.asyncmongo.protocol.commands.MakableCommand]] instance of this command.
   */
  def apply() :MakableCommand = new MakableCommand("admin", this)
}

/**
 * A makable command, that can produce a request maker ready to be sent to a [[org.asyncmongo.actors.MongoDBSystem]] actor.
 *
 * @param db Database name.
 * @param command Subject command.
 */
class MakableCommand(val db: String, val command: Command) {
  /**
   * Produces the [[org.asyncmongo.protocol.Query]] instance for the given command.
   */
  def makeQuery :Query = Query(if(command.slaveOk) QueryFlags.SlaveOk else 0, db + ".$cmd", 0, 1)
  /**
   * Returns the [[org.asyncmongo.protocol.RequestMaker]] for the given command.
   */
  def maker = RequestMaker(makeQuery, command.makeDocuments.makeBuffer)
}

case class RawCommand(bson: Bson) extends Command {
  def makeDocuments = bson

  type Result = Map[String, BSONElement]

  object ResultMaker extends CommandResultMaker[Result] {
    def apply(response: Response) = DefaultBSONHandlers.parse(response).next().mapped
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
) extends Command {
  override def makeDocuments = {
    val bson = Bson("getlasterror" -> BSONInteger(1))
    if(awaitJournalCommit) {
      bson.write("j" -> BSONBoolean(true))
    }
    if(waitForReplicatedOn.isDefined) {
      bson.write("w" -> BSONInteger(waitForReplicatedOn.get))
    }
    if(fsync) {
      bson.write("fsync" -> BSONBoolean(true))
    }
    bson
  }

  type Result = LastError

  val ResultMaker = LastError
}

/**
 * Result of the [[org.asyncmongo.protocol.commands.GetLastError]] command.
 *
 * @param ok true if the last operation was successful
 * @param err the err field, if any
 * @param code the error code, if any
 * @param message the message (often regarding an error) if any
 * @param original the whole map resulting of the deserialization of the response with the [[org.asyncmongo.handlers.DefaultBSONHandlers]].
 */
case class LastError(
  ok: Boolean,
  err: Option[String],
  code: Option[Int],
  message: Option[String],
  original: Map[String, BSONElement]
) {
  /** states if the last operation ended up with an error */
  lazy val inError :Boolean = !ok || err.isDefined
  lazy val stringify :String = toString + " [inError: " + inError + "]"
}

/**
 * Deserializer for [[org.asyncmongo.protocol.commands.GetLastError]] command result.
 */
object LastError extends CommandResultMaker[LastError] {
  def apply(response: Response) = {
    val mapped = DefaultBSONHandlers.parse(response).next().mapped
    LastError(
      mapped.get("ok").flatMap {
        case d: BSONDouble => Some(d == 1)
        case _ => None
      }.getOrElse(true),
      mapped.get("err").flatMap {
        case s: BSONString => Some(s.value)
        case _ => None
      },
      mapped.get("code").flatMap {
        case i: BSONInteger => Some(i.value)
        case _ => None
      },
      mapped.get("errmsg").flatMap {
        case s: BSONString => Some(s.value)
        case _ => None
      },
      mapped
    )
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
  query: Option[Bson] = None,
  fields: Option[Bson] = None
) extends Command {
  override def makeDocuments = {
    val bson = Bson("count" -> BSONString(collectionName))
    if(query.isDefined)
      bson.write("query" -> BSONDocument(query.get.makeBuffer))
    if(fields.isDefined)
      bson.write("fields" -> BSONDocument(fields.get.makeBuffer))
    bson
  }

  type Result = Int
  val ResultMaker = Count
}

/**
 * Deserializer for the Count command. Basically returns an Int (number of counted documents)
 */
object Count extends CommandResultMaker[Int] {
  def apply(response: Response) = DefaultBSONHandlers.parse(response).next().find(_.name == "n").get match {
    case ReadBSONElement(_, BSONDouble(n)) => n.toInt
    case _ => 0
  }
}

/**
 * ReplSetGetStatus Command.
 *
 * Returns the state of the Replica Set from the target server's point of view.
 */
object ReplStatus extends AdminCommand {
  override def makeDocuments = Bson("replSetGetStatus" -> BSONInteger(1))

  type Result = Map[String, BSONElement]

  object ResultMaker extends CommandResultMaker[Result] {
    def apply(response: Response) = DefaultBSONHandlers.parse(response).next().mapped
  }
}

/**
 * ServerStatus Command.
 *
 * Gets the detailed status of the target server.
 */
object Status extends AdminCommand {
  override def makeDocuments = Bson("serverStatus" -> BSONInteger(1))

  type Result = Map[String, BSONElement]

  object ResultMaker extends CommandResultMaker[Result] {
    def apply(response: Response) = DefaultBSONHandlers.parse(response).next().mapped
  }
}

/**
 * Getnonce Command.
 *
 * Gets a nonce for authentication token.
 */
object Getnonce extends Command {
  override def makeDocuments = Bson("getnonce" -> BSONInteger(1))

  type Result = GetnonceResult

  object ResultMaker extends CommandResultMaker[Result] {
    def apply(response: Response) = GetnonceResult(DefaultBSONHandlers.parse(response).next().mapped.get("nonce").get.value.asInstanceOf[BSONString].value)
  }
}

/** Getnonce Command's result (holding a string representation of the nonce) */
case class GetnonceResult(nonce: String)

/**
 * Authenticate Command.
 *
 * @param user username
 * @param password user's password
 * @param nonce the previous nonce given by the server
 */
case class Authenticate(user: String, password: String, nonce: String) extends Command {
  import Converters._
  /** the computed digest of the password */
  lazy val pwdDigest = md5Hex(user + ":mongo:" + password)
  /** the digest of the tuple (''nonce'', ''user'', ''pwdDigest'') */
  lazy val key = md5Hex(nonce + user + pwdDigest)

  override def makeDocuments = Bson("authenticate" -> BSONInteger(1), "user" -> BSONString(user), "nonce" -> BSONString(nonce), "key" -> BSONString(key))

  type Result = Map[String, BSONElement]

  object ResultMaker extends CommandResultMaker[Result] {
    def apply(response: Response) = DefaultBSONHandlers.parse(response).next().mapped
  }
}

/** Authentication command's response deserializer. */
object Authenticate extends CommandResultMaker[AuthenticationResult] {
  def apply(response: Response) = {
    val mapped = DefaultBSONHandlers.parse(response).next().mapped
    if(mapped.get("ok").get.value.asInstanceOf[BSONDouble].value == 0)
      FailedAuthentication(mapped.get("errmsg").map(_.value.asInstanceOf[BSONString].value).getOrElse(""))
    else SuccessfulAuthentication(
        mapped.get("dbname").get.value.asInstanceOf[BSONString].value,
        mapped.get("user").get.value.asInstanceOf[BSONString].value,
        mapped.get("readOnly").flatMap {
          case ReadBSONElement(_, BSONBoolean(value)) => Some(value)
          case _ => Some(false)
        }.get
    )
  }
}

/** an authentication result */
sealed trait AuthenticationResult

/**
 * A failed authentication result
 *
 * @param message the explanation of the error.
 */
case class FailedAuthentication(
  message: String
) extends Exception with AuthenticationResult {
  override def getMessage() = message
}

/**
 * A successful authentication result.
 *
 * @param db database name
 * @param user username
 * @param readOnly states if the authentication gives us only the right to read from the database.
 */
case class SuccessfulAuthentication(
  db: String,
  user: String,
  readOnly: Boolean
) extends AuthenticationResult

/**
 * IsMaster Command.
 *
 * States if the target server is a primary.
 * This command also gives some useful information, like the other nodes in the replica set.
 */
object IsMaster extends AdminCommand {
  def makeDocuments = Bson("isMaster" -> BSONInteger(1))

  type Result = IsMasterResponse

  object ResultMaker extends CommandResultMaker[Result] {
    def apply(response: Response) = {
      val mapped = DefaultBSONHandlers.parse(response).next().mapped
      IsMasterResponse(
        mapped.get("ismaster").flatMap {
          case ReadBSONElement(_, BSONBoolean(b)) => Some(b)
          case _ => None
        }.getOrElse(false),
        mapped.get("secondary").flatMap {
          case ReadBSONElement(_, BSONBoolean(b)) => Some(b)
          case _ => None
        }.getOrElse(false),
        mapped.get("maxBsonObjectSize").flatMap {
          case ReadBSONElement(_, BSONInteger(i)) => Some(i)
          case _ => None
        }.getOrElse(16777216),
        mapped.get("setName").flatMap {
          case ReadBSONElement(_, BSONString(name)) => Some(name)
          case _ => None
        },
        mapped.get("hosts").flatMap {
          case ReadBSONElement(_, BSONArray(buffer)) => Some((for(e <- DefaultBSONIterator(buffer)) yield {
            e.value.asInstanceOf[BSONString].value
          }).toList)
          case _ => None
        },
        mapped.get("me").flatMap {
          case ReadBSONElement(_, BSONString(name)) => Some(name)
          case _ => None
        }
      )
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
  /** the resolved [[org.asyncmongo.protocol.NodeState]] of the answering server */
  lazy val state :NodeState = if(isMaster) PRIMARY else if(secondary) SECONDARY else UNKNOWN
}

/** A modify operation, part of a FindAndModify command */
sealed trait Modify {
  protected[commands] def alter(bson: Bson) :Bson
}

/**
 * Update (part of a FindAndModify command).
 *
 * @param update the modifier document.
 * @param fetchNewObject the command result must be the new object instead of the old one.
 */
case class Update(update: Bson, fetchNewObject: Boolean) extends Modify {
  override def alter(bson: Bson) = bson
      .write("update" -> BSONDocument(update.makeBuffer))
      .write("new" -> BSONBoolean(fetchNewObject))
}

/** Remove (part of a FindAndModify command). */
object Remove extends Modify {
  override def alter(bson: Bson) = bson.write("remove" -> BSONBoolean(true))
}

/**
 * FindAndModify command.
 *
 * This command allows to perform a modify operation (update/remove) matching a query, without the extra requests.
 * It returns the old document by default.
 *
 * @param collection the target collection name
 * @param query the filter for this command
 * @param modify the [[org.asyncmongo.protocol.commands.Modify]] operation to do
 * @param upsert states if a new document should be inserted if no match
 * @param sort the sort document
 * @param fields retrieve only a subset of the returned document
 */
case class FindAndModify(
  collection: String,
  query: Bson,
  modify: Modify,
  upsert: Boolean = false,
  sort: Option[Bson] = None,
  fields: Option[Bson] = None
) extends Command {
  override def makeDocuments: Bson = {
    val bson = Bson("findAndModify" -> BSONString(collection), "query" -> BSONDocument(query.makeBuffer))
    if(sort.isDefined)
      bson.write("sort" -> BSONDocument(sort.get.makeBuffer))
    if(fields.isDefined)
      bson.write("fields" -> BSONDocument(fields.get.makeBuffer))
    if(upsert)
      bson.write("upsert" -> BSONBoolean(true))
    modify.alter(bson)
  }

  type Result = Option[BSONDocument]

  val ResultMaker = FindAndModify
}

/**
 * FindAndModify command deserializer
 * @todo [[org.asyncmongo.handlers.BSONReader[T]]] typeclass
 */
object FindAndModify extends CommandResultMaker[Option[BSONDocument]] {
  def apply(response: Response) =
    DefaultBSONHandlers.parse(response).next().mapped.get("value") flatMap { element =>
      element.value match {
        case doc: BSONDocument => Some(doc)
        case _ => None
      }
    }
}