package reactivemongo.api

import reactivemongo.api.{ BSONSerializationPack => SerPack }

import reactivemongo.api.commands.{ CommandCodecs, WriteResult }

@deprecated("Upgrade to 2.11+", "0.19.0")
private[reactivemongo] object Serialization {
  type Pack = SerPack.type

  type DefaultCollection = collections.GenericCollection[this.Pack] with CollectionMetaCommands

  @deprecated("Upgrade to 2.11+", "0.19.0")
  @inline def defaultCollectionProducer: CollectionProducer[DefaultCollection] =
    reactivemongo.api.collections.bson.BSONCollectionProducer

  @inline def internalSerializationPack: this.Pack = SerPack

  lazy implicit val unitBoxReader =
    CommandCodecs.unitBoxReader(internalSerializationPack)

  lazy implicit val writeResultReader =
    CommandCodecs.writeResultReader[WriteResult, this.Pack](
      internalSerializationPack)
}
