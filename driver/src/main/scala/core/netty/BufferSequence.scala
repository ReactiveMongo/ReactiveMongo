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
package reactivemongo.core.netty

import reactivemongo.io.netty.buffer.{ ByteBuf, Unpooled }

import reactivemongo.bson.{ BSONDocument => LegacyDoc }

private[reactivemongo] case class BufferSequence(
  private val head: ByteBuf,
  private val tail: ByteBuf*) {

  def merged: ByteBuf = mergedBuffer.duplicate()

  private lazy val mergedBuffer =
    Unpooled.wrappedBuffer((head +: tail): _*)
}

private[reactivemongo] object BufferSequence extends BufferSequenceCompat {
  /** Returns an empty buffer sequence. */
  val empty: BufferSequence = BufferSequence(Unpooled.EMPTY_BUFFER)

  /** Returns a new channel buffer with the give `document` written on. */
  def single(document: LegacyDoc): BufferSequence = // TODO: Remove
    BufferSequence(ChannelBufferWritableBuffer single document)
}
