package reactivemongo.api

import reactivemongo_internal.bson.{ BSONSerializationPack => SerPack }

import reactivemongo.api.commands.{ CommandCodecs, WriteResult }

import reactivemongo.api.collections.GenericCollectionProducer

private[reactivemongo] object Compat {
  type SerializationPack = SerPack

  type DefaultCollection = collections.GenericCollection[SerPack] with CollectionMetaCommands

  object defaultCollectionProducer extends GenericCollectionProducer[SerPack, DefaultCollection] {
    def apply(db: DB, name: String, failoverStrategy: FailoverStrategy) = new BSONCollection(db, name, failoverStrategy, db.defaultReadPreference)
  }

  @inline def internalSerializationPack: this.SerializationPack = SerPack

  lazy implicit val unitBoxReader =
    CommandCodecs.unitBoxReader(internalSerializationPack)

  lazy implicit val writeResultReader = {
    val p = internalSerializationPack
    val r = CommandCodecs.defaultWriteResultReader(p)

    p.reader[WriteResult] { doc =>
      p.deserialize(doc, r)
    }
  }
}
