package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

sealed trait Upserted {
  type Pack <: SerializationPack

  def index: Int

  def _id: Pack#Value

  override def equals(that: Any): Boolean = that match {
    case other: Upserted =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString = s"Upserted($index, ${_id})"

  private[commands] lazy val tupled = index -> _id
}

object Upserted {
  private[reactivemongo] type Aux[P] = Upserted { type Pack = P }

  private[reactivemongo] def init[P <: SerializationPack](
    _index: Int,
    id: P#Value): Upserted.Aux[P] = new Upserted {
    type Pack = P
    override val index = _index
    override val _id = id
  }

  def unapply(upserted: Upserted): Option[(Int, Any)] =
    Option(upserted).map { u => u.index -> u._id }
}
