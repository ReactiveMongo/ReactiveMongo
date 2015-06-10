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

import java.nio.ByteOrder

import akka.util.{ByteString, ByteStringBuilder}
import reactivemongo.bson.buffer.ReadableBuffer

private[core] object ByteStringBuilderHelper {
  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN

  sealed trait TypeWriter[T] {
    def apply(builder: ByteStringBuilder, t:T): Unit
  }

  implicit object IntWriter extends TypeWriter[Int] {
    override def apply(builder: ByteStringBuilder, t:Int): Unit = builder.putInt(t)
  }

  implicit object LongWriter extends TypeWriter[Long] {
    override def apply(builder: ByteStringBuilder, t: Long): Unit = builder.putLong(t)
  }

  implicit object StringWriter extends TypeWriter[String] {
    override def apply(builder: ByteStringBuilder, t: String): Unit = {
      val bytes = t.getBytes("utf-8")
      builder.putBytes(bytes)
      builder.putByte(0)
    }
  }

  def write[A](a: A)(builder: ByteStringBuilder)(implicit w1: TypeWriter[A]) : Unit = w1(builder, a)

  def write[A,B](a: A, b: B)(builder: ByteStringBuilder)(implicit w1: TypeWriter[A], w2: TypeWriter[B]) : Unit = {
    w1(builder, a);
    w2(builder, b);
  }


  def write[A,B,C](a: A, b: B, c: C)(builder: ByteStringBuilder)(implicit w1: TypeWriter[A], w2: TypeWriter[B], w3: TypeWriter[C]) : Unit = {
    w1(builder, a);
    w2(builder, b);
    w3(builder, c);
  }

  def write[A,B,C,E](a: A, b: B, c: C, e: E)(builder: ByteStringBuilder)(implicit w1: TypeWriter[A], w2: TypeWriter[B], w3: TypeWriter[C], w4: TypeWriter[E]) : Unit = {
    w1(builder, a);
    w2(builder, b);
    w3(builder, c);
    w4(builder, e);
  }
}

import ByteStringBuilderHelper._

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
sealed trait RequestOp extends Op with ByteStringBuffer {
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

  val size = 4 + 8 + 4 + 4
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

object Reply extends BufferReadable[Reply] with ReadableFrom[ByteString, Reply] {
  val size = 4 + 8 + 4 + 4


  override def readFrom(buffer: ReadableBuffer) = Reply(
    buffer.readInt,
    buffer.readLong,
    buffer.readInt,
    buffer.readInt
  )

  override def readFrom(buffer: ByteString): Reply = {
    val iterator = buffer.iterator
    Reply(
      iterator.getInt,
      iterator.getLong,
      iterator.getInt,
      iterator.getInt
    )
  }
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
  override def size = 4 /* int32 = ZERO */ + 4 + fullCollectionName.length + 1
  override val requiresPrimary = true

  override val append = write(0, fullCollectionName, flags) _
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
  override def size = 4 + fullCollectionName.length + 1
  override val requiresPrimary = true

  override def append = write(flags, fullCollectionName) _
}

/**
 * Query operation.
 *
 * @param flags Operation flags.
 * @param numberToSkip number of documents to skip in the response.
 * @param numberToReturn number of documents to return in the response. 0 means the server will choose.
 */
case class Query(
    flags: Int,
    fullCollectionName: String,
    numberToSkip: Int,
    numberToReturn: Int) extends CollectionAwareRequestOp {
  override val expectsResponse = true
  override val code = 2004
  override def size = 4 + fullCollectionName.length + 1 + 4 + 4

  override val append = write(flags, fullCollectionName, numberToSkip, numberToReturn) _
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
  override def size = 4 /* int32 ZERO */ + fullCollectionName.length + 1 + 4 + 8

  override def append = write(0, fullCollectionName, numberToReturn, cursorID)
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
  override def size = 4 /* int32 ZERO */ + fullCollectionName.length + 1 + 4
  override val requiresPrimary = true

  override def append = write(0, fullCollectionName, flags)
}

/**
 * KillCursors operation.
 *
 * @param cursorIDs ids of the cursors to kill. Should not be empty.
 */
case class KillCursors(
    cursorIDs: Set[Long]) extends RequestOp {
  override val code = 2007
  override def size = 4 /* int32 ZERO */ + 4 + cursorIDs.size * 8

  override def append = { builder: ByteStringBuilder =>
    write(0, cursorIDs.size)(builder)
    for (cursorID <- cursorIDs)
      write(cursorID)(builder)
  }
}