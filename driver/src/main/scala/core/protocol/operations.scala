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
package reactivemongo.core.protocol

import shaded.netty.buffer.ByteBuf

import BufferAccessors._

/** A Mongo Wire Protocol operation */
sealed trait Op {
  /** The operation code */
  val code: Int
}

/**
 * A Mongo Wire Protocol request operation.
 *
 * Actually, all operations excepted Reply are requests.
 */
sealed trait RequestOp extends Op with ChannelBufferWritable {
  /** States if this request expects a response. */
  val expectsResponse: Boolean = false

  /** States if this request has to be run on a primary. */
  val requiresPrimary: Boolean = false
}

/** A request that needs to know the full collection name. */
sealed trait CollectionAwareRequestOp extends RequestOp {
  /** The full collection name (''<dbname.collectionname>'') */
  val fullCollectionName: String

  /** Database and collection name */
  lazy val (db: String, collectionName: String) =
    fullCollectionName.span(_ != '.')
}

/** A request that will perform a write on the database */
sealed trait WriteRequestOp extends CollectionAwareRequestOp

/**
 * Reply operation.
 *
 * @param flags The flags of this response.
 * @param cursorID The cursor id. Strictly positive if a cursor has been created server side, 0 if none or exhausted.
 * @param startingFrom The index the returned documents start from.
 * @param numberReturned The number of documents that are present in this reply.
 */
case class Reply(
  flags: Int,
  cursorID: Long,
  startingFrom: Int,
  numberReturned: Int) extends Op {
  val code = Reply.code

  /** States whether the cursor given in the request was found */
  lazy val cursorNotFound = (flags & 0x01) != 0

  /** States if the request encountered an error */
  lazy val queryFailure = (flags & 0x02) != 0

  /** States if the answering server supports the AwaitData query option */
  lazy val awaitCapable = (flags & 0x08) != 0

  private def str(b: Boolean, s: String) = if (b) s else ""

  /** States if this reply is in error */
  lazy val inError = cursorNotFound || queryFailure

  lazy val stringify = toString + " [" + str(cursorNotFound, "CursorNotFound;") + str(queryFailure, "QueryFailure;") + str(awaitCapable, "AwaitCapable") + "]"
}

object Reply extends ChannelBufferReadable[Reply] {
  /** OP_REPLY = 1 */
  val code = 1

  /** Once the [[Reply]] is parsed, the buffer can immediately be released. */
  def readFrom(buffer: ByteBuf): Reply = Reply(
    buffer.readIntLE,
    buffer.readLongLE,
    buffer.readIntLE,
    buffer.readIntLE)

}

/**
 * Update operation.
 *
 * @param flags Operation flags.
 */
case class Update(
  fullCollectionName: String,
  flags: Int) extends WriteRequestOp {
  val code = 2001
  val writeTo = writeTupleToBuffer3((0, fullCollectionName, flags)) _
  val size = 4 /* int32 = ZERO */ + 4 + fullCollectionName.length + 1
  override val requiresPrimary = true
}

object UpdateFlags {
  /** If set, the database will insert the supplied object into the collection if no matching document is found. */
  val Upsert = 0x01

  /** If set, the database will update all matching objects in the collection. Otherwise only updates first matching doc. */
  val MultiUpdate = 0x02
}

/**
 * Insert operation.
 *
 * @param flags Operation flags.
 */
case class Insert(
  flags: Int,
  fullCollectionName: String) extends WriteRequestOp {
  val code = 2002
  val writeTo = writeTupleToBuffer2((flags, fullCollectionName)) _
  val size = 4 + fullCollectionName.length + 1
  override val requiresPrimary = true
}

/**
 * Query operation.
 *
 * @param flags the operation flags
 * @param fullCollectionName the full name of the queried collection
 * @param numberToSkip the number of documents to skip in the response.
 * @param numberToReturn The number of documents to return in the response. 0 means the server will choose.
 */
case class Query(
  flags: Int,
  fullCollectionName: String,
  numberToSkip: Int,
  numberToReturn: Int) extends CollectionAwareRequestOp {
  override val expectsResponse = true
  val code = 2004
  val size = 4 + fullCollectionName.length + 1 + 4 + 4

  val writeTo: ByteBuf => Unit = writeTupleToBuffer4(
    (flags, fullCollectionName, numberToSkip, numberToReturn)) _
}

/**
 * Query flags.
 */
object QueryFlags {
  /** Makes the cursor not to close after all the data is consumed. */
  val TailableCursor = 0x02

  /** The query is might be run on a secondary. */
  val SlaveOk = 0x04

  /** OplogReplay */
  val OplogReplay = 0x08

  /** The cursor will not expire automatically */
  val NoCursorTimeout = 0x10

  /**
   * Block a little while waiting for more data
   * instead of returning immediately if no data.
   * Use along with TailableCursor.
   */
  val AwaitData = 0x20

  /** Exhaust */
  val Exhaust = 0x40

  /**
   * The response can be partial;
   * If a shard is down, no error will be thrown.
   */
  val Partial = 0x80
}

/**
 * GetMore operation.
 *
 * Allows to get more data from a cursor.
 * @param numberToReturn number of documents to return in the response. 0 means the server will choose.
 * @param cursorId id of the cursor.
 */
case class GetMore(
  fullCollectionName: String,
  numberToReturn: Int,
  cursorID: Long) extends CollectionAwareRequestOp {
  override val expectsResponse = true
  val code = 2005

  val writeTo =
    writeTupleToBuffer4((0, fullCollectionName, numberToReturn, cursorID)) _

  val size = 4 /* int32 ZERO */ + fullCollectionName.length + 1 + 4 + 8
}

/**
 * Delete operation.
 *
 * @param flags operation flags.
 */
case class Delete(
  fullCollectionName: String,
  flags: Int) extends WriteRequestOp {
  val code = 2006
  val writeTo = writeTupleToBuffer3((0, fullCollectionName, flags)) _
  val size = 4 /* int32 ZERO */ + fullCollectionName.length + 1 + 4
  override val requiresPrimary = true
}

/**
 * KillCursors operation.
 *
 * @param cursorIDs ids of the cursors to kill. Should not be empty.
 */
case class KillCursors(cursorIDs: Set[Long]) extends RequestOp {
  val code = 2007

  val writeTo: ByteBuf => Unit = { buffer: ByteBuf =>
    buffer writeIntLE 0
    buffer writeIntLE cursorIDs.size
    for (cursorID <- cursorIDs) {
      buffer writeLongLE cursorID
    }
  }

  val size = 4 /* int32 ZERO */ + 4 + cursorIDs.size * 8
}
