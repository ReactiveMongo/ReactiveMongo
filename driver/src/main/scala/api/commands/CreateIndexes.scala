package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

import reactivemongo.api.indexes.{ Index, IndexesManager, NSIndex }

/**
 * Creates the given indexes on the specified collection.
 *
 * @param db the database name
 * @param indexes the indexes to be created
 */
@deprecated("Internal: will be made private", "0.16.0")
class CreateIndexes(val db: String, val indexes: List[Index])
  extends Product with Serializable // TODO: Remove
  with CollectionCommand with CommandWithResult[WriteResult] {

  val productArity = 2

  def productElement(n: Int): Any = n match {
    case 0 => db
    case _ => indexes
  }

  def canEqual(that: Any): Boolean = that match {
    case _: CreateIndexes => true
    case _                => false
  }

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

@deprecated("Internal: will be made private", "0.16.0")
object CreateIndexes extends scala.runtime.AbstractFunction2[String, List[Index], CreateIndexes] {

  @inline def apply(db: String, indexes: List[Index]): CreateIndexes = new CreateIndexes(db, indexes)

  private[api] def writer[P <: SerializationPack](pack: P): pack.Writer[ResolvedCollectionCommand[Command[P]]] = {
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

      indexes match {
        case head :: tail =>
          elements += element("indexes", builder.array(head, tail))

        case _ =>
      }

      builder.document(elements.result())
    }
  }

  // ---

  private[api] class Command[P <: SerializationPack](
    val db: String,
    val indexes: List[Index.Aux[P]]) extends CollectionCommand with CommandWithResult[WriteResult] {

    override def equals(that: Any): Boolean = that match {
      case other: Command[P] =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString: String = s"CreateIndexes($db, $indexes)"

    private[commands] def tupled = db -> indexes
  }
}
