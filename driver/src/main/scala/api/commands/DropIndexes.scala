package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

@deprecated("Internal: will be made private", "0.16.0")
class DropIndexes(
  val index: String) extends Product with Serializable
  with CollectionCommand with CommandWithResult[DropIndexesResult] {

  def canEqual(that: Any): Boolean = that match {
    case _: DropIndexes => true
    case _              => false
  }

  val productArity = 1

  def productElement(n: Int): Any = index

  override def equals(that: Any): Boolean = that match {
    case other: DropIndexes =>
      index == other.index

    case _ =>
      false
  }

  override def hashCode: Int = index.hashCode

  override def toString: String = s"DropIndexes($index)"
}

@deprecated("Internal: will be made private", "0.16.0")
case class DropIndexesResult(value: Int) extends BoxedAnyVal[Int]

@deprecated("Internal: will be made private", "0.16.0")
object DropIndexes
  extends scala.runtime.AbstractFunction1[String, DropIndexes] {

  @inline def apply(index: String): DropIndexes = new DropIndexes(index)

  def unapply(other: DropIndexes): Option[String] = Option(other).map(_.index)

  private[api] def writer[P <: SerializationPack](pack: P): pack.Writer[ResolvedCollectionCommand[DropIndexes]] = {
    val builder = pack.newBuilder

    import builder.{ elementProducer => element, string }

    pack.writer[ResolvedCollectionCommand[DropIndexes]] { drop =>
      builder.document(Seq(
        element("dropIndexes", string(drop.collection)),
        element("index", string(drop.command.index))))
    }
  }

  private[api] def reader[P <: SerializationPack](pack: P): pack.Reader[DropIndexesResult] = {
    val decoder = pack.newDecoder

    CommandCodecs.dealingWithGenericCommandErrorsReader[pack.type, DropIndexesResult](pack) { doc =>
      DropIndexesResult(decoder.int(doc, "nIndexesWas").getOrElse(0))
    }
  }
}
