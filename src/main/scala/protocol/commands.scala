package org.asyncmongo.protocol.commands

import org.asyncmongo.bson._
import org.asyncmongo.handlers.DefaultBSONHandlers
import org.asyncmongo.protocol.{RequestMaker, Query, QueryFlags, Response}
import org.asyncmongo.protocol.NodeState
import org.asyncmongo.protocol.NodeState._
import org.asyncmongo.utils._

trait Command {
  type Result

  val ResultMaker :CommandResultMaker[Result]

  val slaveOk :Boolean = false
  def makeDocuments :Bson

  def apply(db: String) :MakableCommand = MakableCommand(db, this)
}

trait CommandResultMaker[Result] {
  def apply(response: Response) :Result
}

trait AdminCommand extends Command {
  override def apply(db: String) :MakableCommand = apply()
  def apply() :MakableCommand = MakableCommand("admin", this)
}

case class RawCommand(bson: Bson) extends Command {
  def makeDocuments = bson

  type Result = Map[String, BSONElement]

  object ResultMaker extends CommandResultMaker[Result] {
    def apply(response: Response) = DefaultBSONHandlers.parse(response).next().mapped
  }
}

case class MakableCommand(db: String, command: Command) {
  def makeQuery :Query = Query(if(command.slaveOk) QueryFlags.SlaveOk else 0, db + ".$cmd", 0, 1)
  def maker = RequestMaker(makeQuery, command.makeDocuments.getBuffer)
}

case class GetLastError(
  awaitJournalCommit: Boolean = false,
  waitForReplicatedOn: Option[Int] = None,
  fsync: Boolean = false
) extends Command {
  override def makeDocuments = {
    val bson = Bson(BSONInteger("getlasterror", 1))
    if(awaitJournalCommit) {
      bson.write(BSONBoolean("j", true))
    }
    if(waitForReplicatedOn.isDefined) {
      bson.write(BSONInteger("w", waitForReplicatedOn.get))
    }
    if(fsync) {
      bson.write(BSONBoolean("fsync", true))
    }
    bson
  }

  type Result = LastError

  val ResultMaker = LastError
}

case class LastError(
  ok: Boolean,
  err: Option[String],
  code: Option[Int],
  message: Option[String],
  original: Map[String, BSONElement]
) {
  lazy val inError :Boolean = !ok || err.isDefined
  lazy val stringify :String = toString + " [inError: " + inError + "]"
}

object LastError extends CommandResultMaker[LastError] {
  def apply(response: Response) = {
    val mapped = DefaultBSONHandlers.parse(response).next().mapped
    LastError(
      mapped.get("ok").flatMap {
        case d: BSONDouble => Some(true)
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

case class Count(
  collectionName: String,
  query: Option[Bson] = None,
  fields: Option[Bson] = None
) extends Command {
  override def makeDocuments = {
    val bson = Bson(BSONString("count", collectionName))
    if(query.isDefined)
      bson.write(BSONDocument("query", query.get.getBuffer))
    if(fields.isDefined)
      bson.write(BSONDocument("fields", fields.get.getBuffer))
    bson
  }

  type Result = Int
  val ResultMaker = Count
}

object Count extends CommandResultMaker[Int] {
  def apply(response: Response) = DefaultBSONHandlers.parse(response).next().find(_.name == "n").get match {
    case BSONDouble(_, n) => n.toInt
    case _ => 0
  }
}

object ReplStatus extends AdminCommand {
  override def makeDocuments = Bson(BSONInteger("replSetGetStatus", 1))

  type Result = Map[String, BSONElement]

  object ResultMaker extends CommandResultMaker[Result] {
    def apply(response: Response) = DefaultBSONHandlers.parse(response).next().mapped
  }
}

object Status extends AdminCommand {
  override def makeDocuments = Bson(BSONInteger("serverStatus", 1))

  type Result = Map[String, BSONElement]

  object ResultMaker extends CommandResultMaker[Result] {
    def apply(response: Response) = DefaultBSONHandlers.parse(response).next().mapped
  }
}

object Getnonce extends Command {
  override def makeDocuments = Bson(BSONInteger("getnonce", 1))

  type Result = GetnonceResult

  object ResultMaker extends CommandResultMaker[Result] {
    def apply(response: Response) = GetnonceResult(DefaultBSONHandlers.parse(response).next().mapped.get("nonce").get.asInstanceOf[BSONString].value)
  }
}

case class GetnonceResult(nonce: String)

case class Authenticate(user: String, password: String, nonce: String) extends Command {
  import Converters._
  lazy val pwdDigest = md5Hex(user + ":mongo:" + password)
  lazy val key = md5Hex(nonce + user + pwdDigest)

  override def makeDocuments = Bson(BSONInteger("authenticate", 1)).write(BSONString("user", user)).write(BSONString("nonce", nonce)).write(BSONString("key", key))

  type Result = Map[String, BSONElement]

  object ResultMaker extends CommandResultMaker[Result] {
    def apply(response: Response) = DefaultBSONHandlers.parse(response).next().mapped
  }
}

object Authenticate extends CommandResultMaker[AuthenticationResult] {
  def apply(response: Response) = {
    val mapped = DefaultBSONHandlers.parse(response).next().mapped
    println("\n" + mapped + "\n" + mapped.get("ok") + " => " + mapped.get("ok").get.asInstanceOf[BSONDouble].value == 0)
    if(mapped.get("ok").get.asInstanceOf[BSONDouble].value == 0)
      FailedAuthentication(mapped.get("errmsg").map(_.asInstanceOf[BSONString].value).getOrElse(""))
    else SuccessfulAuthentication(
        mapped.get("dbname").get.asInstanceOf[BSONString].value,
        mapped.get("user").get.asInstanceOf[BSONString].value,
        mapped.get("readOnly").flatMap {
          case BSONBoolean(_, value) => Some(value)
          case _ => Some(false)
        }.get
    )
  }
}

sealed trait AuthenticationResult

case class FailedAuthentication(
  message: String
) extends Exception with AuthenticationResult {
  override def getMessage() = message
}

case class SuccessfulAuthentication(
  db: String,
  user: String,
  readOnly: Boolean
) extends AuthenticationResult

object IsMaster extends AdminCommand {
  def makeDocuments = Bson(BSONInteger("isMaster", 1))

  type Result = IsMasterResponse

  object ResultMaker extends CommandResultMaker[Result] {
    def apply(response: Response) = {
      val mapped = DefaultBSONHandlers.parse(response).next().mapped
      IsMasterResponse(
        mapped.get("ismaster").flatMap {
          case BSONBoolean(_, b) => Some(b)
          case _ => None
        }.getOrElse(false),
        mapped.get("secondary").flatMap {
          case BSONBoolean(_, b) => Some(b)
          case _ => None
        }.getOrElse(false),
        mapped.get("maxBsonObjectSize").flatMap {
          case BSONInteger(_, i) => Some(i)
          case _ => None
        }.getOrElse(16777216),
        mapped.get("setName").flatMap {
          case BSONString(_, name) => Some(name)
          case _ => None
        },
        mapped.get("hosts").flatMap {
          case BSONArray(_, buffer) => Some((for(e <- DefaultBSONIterator(buffer)) yield {
            e.asInstanceOf[BSONString].value
          }).toList)
          case _ => None
        },
        mapped.get("me").flatMap {
          case BSONString(_, name) => Some(name)
          case _ => None
        }
      )
    }
  }
}

case class IsMasterResponse(
  isMaster: Boolean,
  secondary: Boolean,
  maxBsonObjectSize: Int,
  setName: Option[String],
  hosts: Option[List[String]],
  me: Option[String]
) {
  lazy val state :NodeState = if(isMaster) PRIMARY else if(secondary) SECONDARY else UNKNOWN
}

/*
{ findAndModify: "collection", query: {processed:false}, update: {$set: {processed:true}}, new: true}
{ findAndModify: "collection", query: {processed:false}, remove: true, sort: {priority:-1}}
*/
sealed trait Modify {
  protected[commands] def alter(bson: Bson) :Bson
}
case class Update(update: Bson, fetchNewObject: Boolean) extends Modify {
  override def alter(bson: Bson) = bson
      .write(BSONDocument("update", update.getBuffer))
      .write(BSONBoolean("new", fetchNewObject))
}
object Remove extends Modify {
  override def alter(bson: Bson) = bson.write(BSONBoolean("remove", true))
}
/*query  a filter for the query  {}
sort   if multiple docs match, choose the first one in the specified sort order as the object to manipulate  {}
remove   set to a true to remove the object before returning 
N/A
update   a modifier object   N/A
new  set to true if you want to return the modified object rather than the original. Ignored for remove.   false
fields   see Retrieving a Subset of Fields (1.5.0+)  All fields
upsert   create object if it doesn't exist; a query must be supplied! examples (1.5.4+)  false*/
case class FindAndModify(
  collection: String,
  query: Bson,
  modify: Modify,
  upsert: Boolean = false,
  sort: Option[Bson] = None,
  fields: Option[Bson] = None
) extends Command {
  override def makeDocuments: Bson = {
    val bson = Bson(BSONString("findAndModify", collection)).write(BSONDocument("query", query.getBuffer))
    if(sort.isDefined)
      bson.write(BSONDocument("sort", sort.get.getBuffer))
    if(fields.isDefined)
      bson.write(BSONDocument("fields", fields.get.getBuffer))
    if(upsert)
      bson.write(BSONBoolean("upsert", true))
    modify.alter(bson)
  }

  type Result = Option[BSONDocument]

  val ResultMaker = FindAndModify
}

object FindAndModify extends CommandResultMaker[Option[BSONDocument]] {
  def apply(response: Response) = DefaultBSONHandlers.parse(response).next().mapped.get("value") flatMap {
    case doc: BSONDocument => Some(doc)
    case _ => None
  }
}