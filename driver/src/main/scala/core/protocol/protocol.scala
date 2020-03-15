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

import reactivemongo.io.netty.buffer.ByteBuf
import reactivemongo.io.netty.channel.{ ChannelHandlerContext, ChannelId }

import reactivemongo.core.netty.BufferSequence

import reactivemongo.api.commands.GetLastError

import reactivemongo.api.ReadPreference

// concrete classes

/**
 * A helper to build requests.
 *
 * @param op write operation.
 * @param documents body of this request, a [[http://netty.io/4.1/api/io/netty/buffer/ByteBuf.html ByteBuf]] containing 0, 1, or many documents.
 * @param channelIdHint a hint for sending this request on a particular channel.
 */
private[reactivemongo] case class RequestMaker(
  op: RequestOp,
  documents: BufferSequence = BufferSequence.empty,
  readPreference: ReadPreference = ReadPreference.primary,
  channelIdHint: Option[ChannelId] = None) {

  def apply(@deprecatedName(Symbol("id")) requestID: Int) = Request(
    requestID, 0, op, documents, readPreference, channelIdHint)
}

// protocol handlers for netty.
private[reactivemongo] class RequestEncoder
  extends reactivemongo.io.netty.handler.codec.MessageToByteEncoder[Request] {
  def encode(
    ctx: ChannelHandlerContext,
    message: Request,
    buffer: ByteBuf): Unit = {

    message writeTo buffer

    ()
  }
}
