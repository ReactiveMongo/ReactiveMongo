package reactivemongo.api.commands

import reactivemongo.bson.BSONValue

import reactivemongo.api.SerializationPack

abstract class Upserted extends Product with Serializable {
  type Pack <: SerializationPack

  def index: Int = throw new UnsupportedOperationException

  def _id: Pack#Value = throw new UnsupportedOperationException

  val productArity = 2

  def productElement(n: Int): Any = n match {
    case 0 => index
    case _ => _id
  }

  def canEqual(that: Any): Boolean = that match {
    case _: Upserted => true
    case _           => false
  }

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

object Upserted
  extends scala.runtime.AbstractFunction2[Int, BSONValue, Upserted] {

  private[reactivemongo] type Aux[P] = Upserted { type Pack = P }

  @deprecated("Use with serialization pack type parameter", "0.19.0")
  def apply(index: Int, _id: BSONValue): Upserted =
    init[reactivemongo.api.BSONSerializationPack.type](index, _id)

  private[reactivemongo] def init[P <: SerializationPack](
    _index: Int,
    id: P#Value): Upserted.Aux[P] = new Upserted {
    type Pack = P
    override val index = _index
    override val _id = id
  }

  @deprecated("No longer a case class", "0.19.0")
  def unapply(that: Upserted): Option[(Int, Any)] = Option(that).map(_.tupled)
}
