package reactivemongo.api.commands

import reactivemongo.api.{ PackSupport, SerializationPack }

trait UpsertedFactory[P <: SerializationPack] {
  _: PackSupport[P] =>

  /** An upserted element */
  final class Upserted private (
    _index: Int,
    __id: pack.Value) {

    /** The index of the upserted element */
    @inline def index: Int = _index

    /** The id of the upserted element */
    @SuppressWarnings(Array("MethodNames"))
    @inline def _id: pack.Value = __id

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

    private[api] def readUpserted(decoder: SerializationPack.Decoder[pack.type]): pack.Document => Option[Upserted] = { document =>
      (for {
        index <- decoder.int(document, "index")
        id <- decoder.get(document, "_id")
      } yield Upserted(index, id))
    }

    private[reactivemongo] implicit val reader: pack.Reader[Upserted] = {
      val decoder = pack.newDecoder

      pack.readerOpt[Upserted](readUpserted(decoder))
    }
  }
}
