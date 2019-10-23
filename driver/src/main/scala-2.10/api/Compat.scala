package reactivemongo.api

private[reactivemongo] object Compat {
  @inline def defaultCollectionProducer =
    reactivemongo.api.collections.bson.BSONCollectionProducer

  @inline def internalSerializationPack =
    reactivemongo.api.BSONSerializationPack
}
