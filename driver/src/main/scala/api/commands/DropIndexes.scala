package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

private[reactivemongo] final class DropIndexes(
  val index: String) extends CollectionCommand with CommandWithResult[DropIndexesResult] {

  override def equals(that: Any): Boolean = that match {
    case other: DropIndexes =>
      index == other.index

    case _ =>
      false
  }

  override def hashCode: Int = index.hashCode

  override def toString: String = s"DropIndexes($index)"
}

private[reactivemongo] final class DropIndexesResult(
  val value: Int) extends AnyVal

private[reactivemongo] object DropIndexes {
  @inline def apply(index: String): DropIndexes = new DropIndexes(index)

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
      new DropIndexesResult(decoder.int(doc, "nIndexesWas").getOrElse(0))
    }
  }
}
