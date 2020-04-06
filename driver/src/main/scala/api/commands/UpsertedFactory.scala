package reactivemongo.api.commands

import reactivemongo.api.{ PackSupport, SerializationPack }

private[reactivemongo] trait UpsertedFactory[P <: SerializationPack] {
  _: PackSupport[P] =>

  final class Upserted private (
    val index: Int,
    val _id: pack.Value) {

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"Upserted($index, ${_id})"

    private[commands] lazy val tupled = index -> _id
  }

  object Upserted {
    private[reactivemongo] def apply(index: Int, _id: pack.Value): Upserted =
      new Upserted(index, _id)

    def unapply(upserted: Upserted): Option[(Int, Any)] =
      Option(upserted).map { u => u.index -> u._id }

    private[api] def readUpserted(decoder: SerializationPack.Decoder[pack.type]): pack.Document => Upserted = { document =>
      (for {
        index <- decoder.int(document, "index")
        id <- decoder.get(document, "_id")
      } yield Upserted(index, id)).get
    }

    private[reactivemongo] implicit val reader: pack.Reader[Upserted] = {
      val decoder = pack.newDecoder

      pack.reader[Upserted](readUpserted(decoder))
    }
  }
}
