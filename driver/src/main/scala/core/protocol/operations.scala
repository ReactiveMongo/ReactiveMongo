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

import shaded.netty.buffer._

/**
 * Helper methods to write tuples of supported types into a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
 */
private[protocol] object BufferAccessors {
  /**
   * Typeclass for types that can be written into a [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]]
   * via writeTupleToBufferN methods.
   *
   * @tparam T type to be written via BufferAccessors.writeTupleToBufferN(...) methods.
   */
  sealed trait BufferInteroperable[T] {
    def apply(buffer: ChannelBuffer, t: T): Unit
  }

  implicit object IntChannelInteroperable extends BufferInteroperable[Int] {
    def apply(buffer: ChannelBuffer, i: Int) = buffer writeInt i
  }

  implicit object LongChannelInteroperable extends BufferInteroperable[Long] {
    def apply(buffer: ChannelBuffer, l: Long) = buffer writeLong l
  }

  implicit object StringChannelInteroperable
    extends BufferInteroperable[String] {

    private def writeCString(buffer: ChannelBuffer, s: String): ChannelBuffer = {
      val bytes = s.getBytes("utf-8")
      buffer writeBytes bytes
      buffer writeByte 0
      buffer
    }

    def apply(buffer: ChannelBuffer, s: String) = {
      writeCString(buffer, s); ()
    }
  }

  /**
   * Write the given tuple into the given [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
   *
   * @tparam A type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   * @tparam B type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   */
  def writeTupleToBuffer2[A, B](t: (A, B))(buffer: ChannelBuffer)(implicit i1: BufferInteroperable[A], i2: BufferInteroperable[B]): Unit = {
    i1(buffer, t._1)
    i2(buffer, t._2)
  }

  /**
   * Write the given tuple into the given [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
   *
   * @tparam A type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   * @tparam B type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   * @tparam C type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   */
  def writeTupleToBuffer3[A, B, C](t: (A, B, C))(buffer: ChannelBuffer)(implicit i1: BufferInteroperable[A], i2: BufferInteroperable[B], i3: BufferInteroperable[C]): Unit = {
    i1(buffer, t._1)
    i2(buffer, t._2)
    i3(buffer, t._3)
  }

  /**
   * Write the given tuple into the given [[http://static.netty.io/3.5/api/org/jboss/netty/buffer/ChannelBuffer.html ChannelBuffer]].
   *
   * @tparam A type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   * @tparam B type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   * @tparam C type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   * @tparam D type that have an implicit typeclass [[reactivemongo.core.protocol.BufferAccessors.BufferInteroperable]].
   */
  def writeTupleToBuffer4[A, B, C, D](t: (A, B, C, D))(buffer: ChannelBuffer)(implicit i1: BufferInteroperable[A], i2: BufferInteroperable[B], i3: BufferInteroperable[C], i4: BufferInteroperable[D]): Unit = {
    i1(buffer, t._1)
    i2(buffer, t._2)
    i3(buffer, t._3)
    i4(buffer, t._4)
  }
}

import BufferAccessors._

/** A Mongo Wire Protocol operation */
sealed trait Op {
  /** operation code */
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
  lazy val (db: String, collectionName: String) = fullCollectionName.span(_ != '.')
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
  override val code = 1

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
  def readFrom(buffer: ChannelBuffer) = Reply(
    buffer.readInt,
    buffer.readLong,
    buffer.readInt,
    buffer.readInt)
}

/**
 * Update operation.
 *
 * @param flags Operation flags.
 */
case class Update(
  fullCollectionName: String,
  flags: Int) extends WriteRequestOp {
  override val code = 2001
  override val writeTo = writeTupleToBuffer3((0, fullCollectionName, flags)) _
  override def size = 4 /* int32 = ZERO */ + 4 + fullCollectionName.length + 1
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
  override val code = 2002
  override val writeTo = writeTupleToBuffer2((flags, fullCollectionName)) _
  override def size = 4 + fullCollectionName.length + 1
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
  override val code = 2004
  override val writeTo = writeTupleToBuffer4((flags, fullCollectionName, numberToSkip, numberToReturn)) _
  override def size = 4 + fullCollectionName.length + 1 + 4 + 4
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
   * Block a little while waiting for more data instead of returning immediately if no data.
   * Use along with TailableCursor.
   */
  val AwaitData = 0x20

  /** Exhaust */
  val Exhaust = 0x40

  /** The response can be partial - if a shard is down, no error will be thrown. */
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
  override val code = 2005
  override val writeTo = writeTupleToBuffer4((0, fullCollectionName, numberToReturn, cursorID)) _
  override def size = 4 /* int32 ZERO */ + fullCollectionName.length + 1 + 4 + 8
}

/**
 * Delete operation.
 *
 * @param flags operation flags.
 */
case class Delete(
  fullCollectionName: String,
  flags: Int) extends WriteRequestOp {
  override val code = 2006
  override val writeTo = writeTupleToBuffer3((0, fullCollectionName, flags)) _
  override def size = 4 /* int32 ZERO */ + fullCollectionName.length + 1 + 4
  override val requiresPrimary = true
}

/**
 * KillCursors operation.
 *
 * @param cursorIDs ids of the cursors to kill. Should not be empty.
 */
case class KillCursors(
  cursorIDs: Set[Long]) extends RequestOp {
  override val code = 2007
  override val writeTo = { buffer: ChannelBuffer =>
    buffer writeInt 0
    buffer writeInt cursorIDs.size
    for (cursorID <- cursorIDs) {
      buffer writeLong cursorID
    }
  }
  override def size = 4 /* int32 ZERO */ + 4 + cursorIDs.size * 8
}
