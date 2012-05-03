package org.asyncmongo.protocol.messages

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

object Count {
  def makeWritableMessage(db: String, fullCollectionName: String, query: Option[Bson], fields: Option[Bson], requestID: Int) :WritableMessage[Query] = {
    val bson = new Bson
    bson.writeElement("count", fullCollectionName.span(_ != '.')._2.tail)
    bson.writeElement("query", query.getOrElse(new Bson))
    bson.writeElement("fields", fields.getOrElse(new Bson))
    WritableMessage(requestID, 0, Query(0, db + ".$cmd", 0, 1), bson.getBuffer)
  }
}

object IsMaster {
  def makeWritableMessage(db: String, requestID: Int) :WritableMessage[Query] = {
    val bson = new Bson
    bson.writeElement("isMaster", 1)
    WritableMessage(requestID, 0, Query(0, db + ".$cmd", 0, 1), bson.getBuffer)
  }
}