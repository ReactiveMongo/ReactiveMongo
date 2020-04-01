package reactivemongo.api.commands

import reactivemongo.core.errors.GenericDriverException

import reactivemongo.api.SerializationPack

import reactivemongo.api.indexes.Index

/**
 * Lists the indexes of the specified collection.
 *
 * @param db the database name
 */
private[reactivemongo] class ListIndexes(val db: String)
  extends CollectionCommand with CommandWithResult[List[Index]] {

  override def equals(that: Any): Boolean = that match {
    case other: ListIndexes =>
      this.db == other.db

    case _ =>
      false
  }

  override def hashCode: Int = db.hashCode

  override def toString: String = s"ListIndexes($db)"
}

private[reactivemongo] object ListIndexes {
  private[api] def writer[P <: SerializationPack](pack: P): pack.Writer[ResolvedCollectionCommand[Command[P]]] = {
    val builder = pack.newBuilder

    pack.writer[ResolvedCollectionCommand[Command[P]]] { list =>
      builder.document(Seq(builder.elementProducer(
        "listIndexes", builder.string(list.collection))))
    }
  }

  private[api] def reader[P <: SerializationPack](pack: P)(implicit r: pack.Reader[Index.Aux[P]]): pack.Reader[List[Index.Aux[P]]] = {
    val decoder = pack.newDecoder

    @annotation.tailrec
    def readBatch(batch: List[pack.Document], indexes: List[Index.Aux[P]]): List[Index.Aux[P]] = batch match {
      case d :: ds =>
        readBatch(ds, pack.deserialize(d, r) :: indexes)

      case _ => indexes
    }

    //import BSONCommonWriteCommandsImplicits.DefaultWriteResultReader

    CommandCodecs.dealingWithGenericCommandErrorsReader[P, List[Index.Aux[P]]](pack) { doc =>
      decoder.child(doc, "cursor").map {
        decoder.children(_, "firstBatch")
      }.fold[List[Index.Aux[P]]](throw new GenericDriverException(
        "the cursor and firstBatch must be defined"))(readBatch(_, Nil))
    }
  }

  // ---

  private[api] final class Command[P <: SerializationPack](val db: String)
    extends CollectionCommand with CommandWithResult[List[Index.Aux[P]]] {

    override def equals(that: Any): Boolean = that match {
      case other: Command[P] =>
        this.db == other.db

      case _ =>
        false
    }

    override def hashCode: Int = db.hashCode

    override def toString: String = s"ListIndexes($db)"
  }
}
