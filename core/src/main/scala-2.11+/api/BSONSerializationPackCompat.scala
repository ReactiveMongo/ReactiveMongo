package reactivemongo.api

import reactivemongo.bson.buffer.DefaultBufferHandler

import reactivemongo.api.bson.buffer.WritableBuffer

private[api] trait BSONSerializationPackCompat {
  _: BSONSerializationPack.type =>

  private[reactivemongo] def writeToBuffer(
    buffer: WritableBuffer,
    document: Document): WritableBuffer = {
    val buf = new reactivemongo.bson.buffer.ArrayBSONBuffer()

    DefaultBufferHandler.writeDocument(document, buf)

    buffer.writeBytes(buf.array)
  }
}
