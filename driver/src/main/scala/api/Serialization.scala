package reactivemongo.api

import reactivemongo.api.bson.collection.{ BSONSerializationPack => SerPack }
import reactivemongo.api.commands.{ CommandCodecs, WriteResult }

private[reactivemongo] object Serialization {
  type Pack = SerPack.type

  type DefaultCollection = reactivemongo.api.bson.collection.BSONCollection

  val defaultCollectionProducer =
    reactivemongo.api.bson.collection.BSONCollectionProducer

  @inline def internalSerializationPack: this.Pack = SerPack

  lazy implicit val unitReader: SerPack.Reader[Unit] =
    CommandCodecs.unitReader(internalSerializationPack)

  lazy implicit val writeResultReader: SerPack.Reader[WriteResult] =
    CommandCodecs.writeResultReader[WriteResult, this.Pack](
      internalSerializationPack
    )
}
