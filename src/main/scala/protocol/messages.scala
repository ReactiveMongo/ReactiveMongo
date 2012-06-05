package org.asyncmongo.protocol.messages

import org.asyncmongo.utils._
import org.asyncmongo.bson._
import org.asyncmongo.protocol.{WritableMessage, Query}

case class GetLastError(
  awaitJournalCommit: Boolean = false,
  waitForReplicatedOn: Option[Int] = None,
  fsync: Boolean = false
) {
  def makeWritableMessage(db: String, requestID: Int) :WritableMessage[Query] = {
    val bson = new Bson
    bson.write(BSONInteger("getlasterror", 1))
    if(awaitJournalCommit) {
      bson.write(BSONBoolean("j", true))
    }
    if(waitForReplicatedOn.isDefined) {
      bson.write(BSONInteger("w", waitForReplicatedOn.get))
    }
    if(fsync) {
      bson.write(BSONBoolean("fsync", true))
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
    bson.write(BSONString("count", collectionName))
    if(query.isDefined)
      bson.write(BSONDocument("query", query.get.getBuffer))
    if(fields.isDefined)
      bson.write(BSONDocument("fields", fields.get.getBuffer))
    bson
  }
}

object IsMaster {
  def makeWritableMessage(db: String) :WritableMessage[Query] = {
    val bson = new Bson
    bson.write(BSONInteger("isMaster", 1))
    WritableMessage(Query(0, db + ".$cmd", 0, 1), bson.getBuffer)
  }
}

object ReplStatus {
  def makeWritableMessage(db: String) :WritableMessage[Query] = {
    val bson = new Bson
    bson.write(BSONInteger("replSetGetStatus", 1))
    WritableMessage(Query(0, "admin.$cmd", 0, 1), bson.getBuffer)
  }
}

object Status {
  def makeWritableMessage(db: String) :WritableMessage[Query] = {
    val bson = new Bson
    bson.write(BSONInteger("serverStatus", 1))
    WritableMessage(Query(0, db + ".$cmd", 0, 1), bson.getBuffer)
  }
}