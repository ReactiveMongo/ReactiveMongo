package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

private[reactivemongo] final class Capped(
    val size: Long,
    val max: Option[Int] = None) {

  override def equals(that: Any): Boolean = that match {
    case other: Capped =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  override def toString: String = s"Capped($size, $max)"

  private[commands] lazy val tupled = size -> max
}

private[reactivemongo] object Capped {

  @SuppressWarnings(Array("UnusedMethodParameter"))
  private[api] def writeTo[P <: SerializationPack](
      pack: P
    )(builder: SerializationPack.Builder[pack.type],
      append: pack.ElementProducer => Any
    )(capped: Capped
    ): Unit = {
    import builder.{ elementProducer => element }

    append(element("size", builder.long(capped.size)))

    capped.max.foreach { max => append(element("max", builder.int(max))) }
  }
}
