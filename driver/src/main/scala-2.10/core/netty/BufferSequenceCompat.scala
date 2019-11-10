package reactivemongo.core.netty

import reactivemongo.api.SerializationPack

private[netty] trait BufferSequenceCompat {
  private[reactivemongo] def single[P <: SerializationPack](pack: P)(document: pack.Document): BufferSequence = {
    val head = new reactivemongo.bson.buffer.ArrayBSONBuffer()

    pack.writeToBuffer(head, document)

    BufferSequence(head.buffer)
  }
}
