package reactivemongo.api.bson.collection

import reactivemongo.api.{ CollectionMetaCommands, DB, FailoverStrategy }

import reactivemongo.api.collections.{
  GenericCollection,
  GenericCollectionProducer
}

/** Integration of `reactivemongo.api.bson` for collection operations. */
object `package` {
  private type Pack = BSONSerializationPack.type

  /**
   * Collection type using package `reactivemongo.api.bson` for serialization
   */
  type BSONCollection = GenericCollection[this.Pack] with CollectionMetaCommands

  /**
   * Instance of collection producer for `BSONCollection`
   */
  implicit object BSONCollectionProducer
      extends GenericCollectionProducer[this.Pack, this.BSONCollection] {
    val pack = BSONSerializationPack

    def apply(
        db: DB,
        name: String,
        failoverStrategy: FailoverStrategy
      ): BSONCollection =
      new CollectionImpl(db, name, failoverStrategy, db.defaultReadPreference)
  }
}
