package reactivemongo.api

import reactivemongo.api.{ BSONSerializationPack => SerPack }

import reactivemongo.api.commands.{ CommandCodecs, WriteResult }

@deprecated("Upgrade to 2.11+", "0.19.0")
private[reactivemongo] object Compat {
  type SerializationPack = SerPack.type

  type DefaultCollection = collections.GenericCollection[SerializationPack] with CollectionMetaCommands

  @deprecated("Upgrade to 2.11+", "0.19.0")
  @inline def defaultCollectionProducer: CollectionProducer[DefaultCollection] =
    reactivemongo.api.collections.bson.BSONCollectionProducer

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
