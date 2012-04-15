package org.asyncmongo.protocol

import org.jboss.netty.channel._
import org.jboss.netty.buffer._

import org.asyncmongo.utils.BufferAccessors._

sealed trait Op {
  val code :Int
}

sealed trait WritableOp extends Op with ChannelBufferWritable

sealed trait AwaitingResponse extends WritableOp

case class Reply(
  flags: Int,
  cursorID: Long,
  startingFrom: Int,
  numberReturned: Int
) extends Op {
  override val code = 1
}

object Reply extends ChannelBufferReadable[Reply] {
  def readFrom(buffer: ChannelBuffer) = Reply(
    buffer.readInt,
    buffer.readLong,
    buffer.readInt,
    buffer.readInt
  )
}

case class Update(
  fullCollectionName: String,
  flags: Int
) extends WritableOp {
  override val code = 2001
  override val writeTo = writeTupleToBuffer3( (0, fullCollectionName, flags) ) _
  override def size = 4 /* int32 = ZERO */ + 4 + fullCollectionName.length + 1
}

case class Insert(
  flags: Int,
  fullCollectionName: String
) extends WritableOp {
  override val code = 2002
  override val writeTo = writeTupleToBuffer2( (flags, fullCollectionName) ) _
  override def size = 4 + fullCollectionName.length + 1
}

case class Query(
  flags: Int,
  fullCollectionName: String,
  numberToSkip: Int,
  numberToReturn: Int
) extends AwaitingResponse {
  override val code = 2004
  override val writeTo = writeTupleToBuffer4( (flags, fullCollectionName, numberToSkip, numberToReturn) ) _
  override def size = 4 + fullCollectionName.length + 1 + 4 + 4
}

case class GetMore(
  fullCollectionName: String,
  numberToReturn: Int,
  cursorID: Long
) extends AwaitingResponse {
  override val code = 2005
  override val writeTo = writeTupleToBuffer4( (0, fullCollectionName, numberToReturn, cursorID) ) _
  override def size = 4 /* int32 ZERO */ + fullCollectionName.length + 1 + 4 + 8
}

case class Delete(
  fullCollectionName: String,
  flags: Int
) extends WritableOp {
  override val code = 2006
  override val writeTo = writeTupleToBuffer3( (0, fullCollectionName, flags) ) _
  override def size = 4 /* int32 ZERO */ + fullCollectionName.length + 1 + 4
}

case class KillCursors(
  cursorIDs: Set[Long]
) extends WritableOp {
  override val code = 2007
  override val writeTo = { buffer: ChannelBuffer =>
    buffer writeInt cursorIDs.size
    for(cursorID <- cursorIDs) {
      buffer writeLong cursorID
    }
  }
  override def size = 4 /* int32 ZERO */ + 4 + cursorIDs.size * 8
}