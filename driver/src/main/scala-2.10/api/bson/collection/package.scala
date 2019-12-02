package reactivemongo.api.bson.collection

@deprecated("Will be replace by actual new BSON API", "0.19.3")
object `package` {
  type BSONCollection = reactivemongo.api.collections.bson.BSONCollection

  type BSONSerializationPack = reactivemongo.api.BSONSerializationPack.type

  @inline def BSONSerializationPack: BSONSerializationPack =
    reactivemongo.api.BSONSerializationPack
}
