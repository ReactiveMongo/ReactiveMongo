package reactivemongo.core.netty

import reactivemongo.api.SerializationPack

private[netty] trait BufferSequenceCompat {
  private[reactivemongo] def single[P <: SerializationPack](pack: P)(document: pack.Document): BufferSequence = {
    val head = reactivemongo.api.bson.buffer.WritableBuffer.empty

    pack.writeToBuffer(head, document)

    BufferSequence(head.buffer)
  }
}
