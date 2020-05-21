package reactivemongo.api

import reactivemongo.api.commands.{ CommandCodecs, WriteResult }

import reactivemongo.api.bson.collection.{
  BSONSerializationPack => SerPack
}

private[reactivemongo] object Serialization {
  type Pack = SerPack.type

  type DefaultCollection = reactivemongo.api.bson.collection.BSONCollection

  val defaultCollectionProducer =
    reactivemongo.api.bson.collection.BSONCollectionProducer

  @inline def internalSerializationPack: this.Pack = SerPack

  lazy implicit val unitReader =
    CommandCodecs.unitReader(internalSerializationPack)

  lazy implicit val writeResultReader =
    CommandCodecs.writeResultReader[WriteResult, this.Pack](
      internalSerializationPack)
}
