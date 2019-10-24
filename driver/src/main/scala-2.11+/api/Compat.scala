package reactivemongo.api

private[reactivemongo] object Compat {
  @inline def defaultCollectionProducer =
    reactivemongo.api.bson.collection.CollectionProducer

  @inline def internalSerializationPack =
    reactivemongo.api.bson.collection.BSONSerializationPack

}
