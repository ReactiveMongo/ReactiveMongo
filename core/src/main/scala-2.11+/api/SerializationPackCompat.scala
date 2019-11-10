package reactivemongo.api

import reactivemongo.api.bson.buffer.WritableBuffer

private[api] trait SerializationPackCompat { _: SerializationPack =>
  private[reactivemongo] def writeToBuffer(
    buffer: WritableBuffer,
    document: Document): WritableBuffer

}
