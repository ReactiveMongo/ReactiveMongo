package org.asyncmongo.protocol.messages

import org.asyncmongo.utils._
import org.asyncmongo.bson.Bson
import org.asyncmongo.protocol.{WritableMessage, Query}

case class GetLastError(
  awaitJournalCommit: Boolean = false,
  waitForReplicatedOn: Option[Int] = None,
  fsync: Boolean = false
) {
  def makeWritableMessage(db: String, requestID: Int) :WritableMessage[Query] = {
    val bson = new Bson
    bson.writeElement("getlasterror", 1)
    if(awaitJournalCommit) {
      bson.writeElement("j", true)
    }
    if(waitForReplicatedOn.isDefined) {
      bson.writeElement("w", waitForReplicatedOn.get)
    }
    if(fsync) {
      bson.writeElement("fsync", true)
    }
    WritableMessage(requestID, 0, Query(0, db + ".$cmd", 0, 1), bson.getBuffer)
  }
}

trait Message {
  val db: String
  def makeQuery :Query = Query(0, db + ".$cmd", 0, 1)
  def makeDocuments :Bson
  def makeWritableMessage :WritableMessage[Query] = makeWritableMessage(randomInt)
  def makeWritableMessage(requestID: Int) :WritableMessage[Query] = WritableMessage(requestID, 0, makeQuery, makeDocuments.getBuffer)
}

case class Count(
  db: String,
  collectionName: String,
  query: Option[Bson] = None,
  fields: Option[Bson] = None
) extends Message {
  def makeDocuments = {
    val bson = new Bson
    bson.writeElement("count", collectionName)
    bson.writeElement("query", query.getOrElse(new Bson))
    bson.writeElement("fields", fields.getOrElse(new Bson))
    bson
  }
}

object IsMaster {
  def makeWritableMessage(db: String) :WritableMessage[Query] = {
    val bson = new Bson
    bson.writeElement("isMaster", 1)
    WritableMessage(Query(0, db + ".$cmd", 0, 1), bson.getBuffer)
  }
}

object ReplStatus {
  def makeWritableMessage(db: String) :WritableMessage[Query] = {
    val bson = new Bson
    bson.writeElement("replSetGetStatus", 1)
    WritableMessage(Query(0, "admin.$cmd", 0, 1), bson.getBuffer)
  }
}

object Status {
  def makeWritableMessage(db: String) :WritableMessage[Query] = {
    val bson = new Bson
    bson.writeElement("serverStatus", 1)
    WritableMessage(Query(0, db + ".$cmd", 0, 1), bson.getBuffer)
  }
}