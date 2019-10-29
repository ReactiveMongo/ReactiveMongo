package reactivemongo.api

import reactivemongo_internal.bson.{ BSONSerializationPack => SerPack }

import reactivemongo.api.commands.{ CommandCodecs, WriteResult }

private[reactivemongo] object Compat {
  @inline def defaultCollectionProducer =
    reactivemongo_internal.bson.CollectionProducer

  type SerializationPack = SerPack

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
