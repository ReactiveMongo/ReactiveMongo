package org.asyncmongo.protocol.messages

import org.asyncmongo.bson._
import org.asyncmongo.handlers.DefaultBSONHandlers
import org.asyncmongo.protocol.{RequestMaker, Query, Response}
import org.asyncmongo.protocol.NodeState
import org.asyncmongo.protocol.NodeState._
import org.asyncmongo.utils._

trait Command {
  val db: String
  def makeQuery :Query = Query(0, db + ".$cmd", 0, 1)
  def makeDocuments :Bson
  def maker = RequestMaker(makeQuery, makeDocuments.getBuffer)
}

trait CommandResult[A] {
  def apply(response: Response) :A
}

case class GetLastError(
  db: String,
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

object LastError extends CommandResult[LastError] {
  def apply(response: Response) :LastError = {
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
  db: String,
  collectionName: String,
  query: Option[Bson] = None,
  fields: Option[Bson] = None
) extends Command {
  def makeDocuments = {
    val bson = Bson(BSONString("count", collectionName))
    if(query.isDefined)
      bson.write(BSONDocument("query", query.get.getBuffer))
    if(fields.isDefined)
      bson.write(BSONDocument("fields", fields.get.getBuffer))
    bson
  }
}

object ReplStatus extends Command {
  val db = "admin"
  def makeDocuments = Bson(BSONInteger("replSetGetStatus", 1))
}

case class Status(db: String) extends Command {
  def makeDocuments = Bson(BSONInteger("serverStatus", 1))
}

case class Getnonce(db: String) extends Command {
  def makeDocuments = Bson(BSONInteger("getnonce", 1))
}

case class GetnonceResult(nonce: String)

object GetnonceResult extends CommandResult[GetnonceResult]{
  def apply(response: Response) = GetnonceResult(DefaultBSONHandlers.parse(response).next().mapped.get("nonce").get.asInstanceOf[BSONString].value)
}

case class Authenticate(db: String, user: String, password: String, nonce: String) extends Command {
  import Converters._
  lazy val pwdDigest = md5Hex(user + ":mongo:" + password)
  lazy val key = md5Hex(nonce + user + pwdDigest)

  def makeDocuments = Bson(BSONInteger("authenticate", 1)).write(BSONString("user", user)).write(BSONString("nonce", nonce)).write(BSONString("key", key))
}

case class IsMaster(db: String = "admin") extends Command {
  def makeDocuments = Bson(BSONInteger("isMaster", 1))
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

object IsMasterResponse extends CommandResult[IsMasterResponse] {
  def apply(response: Response) :IsMasterResponse = {
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