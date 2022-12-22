package reactivemongo.api.commands

import reactivemongo.api.SerializationPack
import reactivemongo.api.indexes.{ Index, IndexesManager, NSIndex }

/**
 * Creates the given indexes on the specified collection.
 *
 * @param db the database name
 * @param indexes the indexes to be created
 */
private[reactivemongo] final class CreateIndexes(
    val db: String,
    val indexes: List[Index])
    extends CollectionCommand
    with CommandWithResult[WriteResult] {

  val commandKind = CommandKind.CreateIndexes

  override def equals(that: Any): Boolean = that match {
    case other: CreateIndexes =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString: String = s"CreateIndexes($db, $indexes)"

  private[commands] def tupled = db -> indexes
}

private[reactivemongo] object CreateIndexes {

  private[api] def writer[P <: SerializationPack](
      pack: P
    ): pack.Writer[ResolvedCollectionCommand[Command[P]]] = {
    val builder = pack.newBuilder
    val nsIndexWriter = IndexesManager.nsIndexWriter[P](pack)

    import builder.{ elementProducer => element }

    pack.writer[ResolvedCollectionCommand[Command[P]]] { create =>
      val indexes = create.command.indexes.map { i =>
        val nsi = NSIndex[P](create.command.db + "." + create.collection, i)

        pack.serialize(nsi, nsIndexWriter)
      }

      val elements = Seq.newBuilder[pack.ElementProducer]

      elements += element("createIndexes", builder.string(create.collection))

      if (indexes.nonEmpty) {
        elements += element("indexes", builder.array(indexes))
      }

      builder.document(elements.result())
    }
  }

  // ---

  private[api] class Command[P <: SerializationPack](
      val db: String,
      val indexes: List[Index.Aux[P]])
      extends CollectionCommand
      with CommandWithResult[WriteResult] {

    val commandKind = CommandKind.CreateIndexes

    override def equals(that: Any): Boolean = that match {
      case other: Command[_] =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString: String = s"CreateIndexes($db, $indexes)"

    private[commands] def tupled = db -> indexes
  }
}
