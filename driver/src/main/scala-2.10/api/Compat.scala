package reactivemongo.api

import reactivemongo.api.{ BSONSerializationPack => SerPack }

import reactivemongo.api.commands.{ CommandCodecs, WriteResult }

private[reactivemongo] object Compat {
  @inline def defaultCollectionProducer =
    reactivemongo.api.collections.bson.BSONCollectionProducer

  type SerializationPack = SerPack.type

  @inline def internalSerializationPack: this.SerializationPack = SerPack

  lazy implicit val unitBoxReader =
    CommandCodecs.unitBoxReader(internalSerializationPack)

  lazy implicit val writeResultReader = {
    val p = internalSerializationPack
    val r = CommandCodecs.defaultWriteResultReader(p)

    p.reader[WriteResult] {
      p.deserialize(_, r)
    }
  }
}
