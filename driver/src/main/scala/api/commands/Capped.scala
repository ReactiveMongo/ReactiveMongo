package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

@deprecated("Internal: will be made private", "0.16.0")
class Capped(
  val size: Long,
  val max: Option[Int] = None) extends Product with Serializable {

  def canEqual(that: Any): Boolean = that match {
    case _: Capped => true
    case _         => false
  }

  val productArity = 2

  def productElement(n: Int): Any = n match {
    case 0 => size
    case _ => max
  }

  override def equals(that: Any): Boolean = that match {
    case other: Capped =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  override def toString: String = s"Capped($size, $max)"

  private[commands] lazy val tupled = size -> max
}

object Capped
  extends scala.runtime.AbstractFunction2[Long, Option[Int], Capped] {

  @inline def apply(size: Long, max: Option[Int]): Capped = new Capped(size, max)

  private[api] def writeTo[P <: SerializationPack](pack: P)(builder: SerializationPack.Builder[pack.type], append: pack.ElementProducer => Any)(capped: Capped): Unit = {
    import builder.{ elementProducer => element }

    append(element("size", builder.long(capped.size)))

    capped.max.foreach { max =>
      append(element("max", builder.int(max)))
    }
  }
}
