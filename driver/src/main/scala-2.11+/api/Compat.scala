package reactivemongo.api

import reactivemongo.api.commands.{ CommandCodecs, WriteResult }

import reactivemongo.api.collections.GenericCollectionProducer

import reactivemongo.api.bson.collection.{
  BSONCollection => BSONColl,
  BSONSerializationPack => SerPack
}

private[reactivemongo] object Compat {
  type SerializationPack = SerPack.type

  type DefaultCollection = collections.GenericCollection[SerializationPack] with CollectionMetaCommands

  object defaultCollectionProducer extends GenericCollectionProducer[SerializationPack, DefaultCollection] {
    def apply(db: DB, name: String, failoverStrategy: FailoverStrategy) =
      new BSONColl(db, name, failoverStrategy, db.defaultReadPreference)
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
