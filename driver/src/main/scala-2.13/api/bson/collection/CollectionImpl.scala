package reactivemongo.api.bson.collection

import reactivemongo.api.{
  CollectionMetaCommands,
  DB,
  FailoverStrategy,
  ReadPreference,
  Serialization
}

import reactivemongo.api.collections.GenericCollection

/**
 * A Collection that interacts with the BSON library.
 */
private[reactivemongo] final class CollectionImpl(
  val db: DB,
  val name: String,
  val failoverStrategy: FailoverStrategy,
  override val readPreference: ReadPreference) extends GenericCollection[Serialization.Pack] with CollectionMetaCommands { self =>

  val pack: Serialization.Pack = Serialization.internalSerializationPack

  def withReadPreference(pref: ReadPreference): Serialization.DefaultCollection = new CollectionImpl(db, name, failoverStrategy, pref)
}
