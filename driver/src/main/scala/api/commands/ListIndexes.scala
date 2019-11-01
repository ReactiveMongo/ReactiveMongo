package reactivemongo.api.commands

import reactivemongo.core.errors.GenericDriverException

import reactivemongo.api.SerializationPack

import reactivemongo.api.indexes.Index

/**
 * Lists the indexes of the specified collection.
 *
 * @param db the database name
 */
@deprecated("Internal: will be made private", "0.16.0")
class ListIndexes(val db: String)
  extends Product with Serializable
  with CollectionCommand with CommandWithResult[List[Index]] {

  val productArity = 1

  def productElement(n: Int): Any = db

  def canEqual(that: Any): Boolean = that match {
    case _: ListIndexes => true
    case _              => false
  }

  override def equals(that: Any): Boolean = that match {
    case other: ListIndexes =>
      this.db == other.db

    case _ =>
      false
  }

  override def hashCode: Int = db.hashCode

  override def toString: String = s"ListIndexes($db)"
}

@deprecated("Internal: will be made private", "0.16.0")
object ListIndexes
  extends scala.runtime.AbstractFunction1[String, ListIndexes] {

  @inline def apply(db: String): ListIndexes = new ListIndexes(db)

  private[api] def writer[P <: SerializationPack](pack: P): pack.Writer[ResolvedCollectionCommand[ListIndexes]] = {
    val builder = pack.newBuilder

    pack.writer[ResolvedCollectionCommand[ListIndexes]] { list =>
      builder.document(Seq(builder.elementProducer(
        "listIndexes", builder.string(list.collection))))
    }
  }

  private[api] def reader[P <: SerializationPack](pack: P)(implicit r: pack.Reader[Index]): pack.Reader[List[Index]] = {
    val decoder = pack.newDecoder

    @annotation.tailrec
    def readBatch(batch: List[pack.Document], indexes: List[Index]): List[Index] = batch match {
      case d :: ds =>
        readBatch(ds, pack.deserialize(d, r) :: indexes)

      case _ => indexes
    }

    //import BSONCommonWriteCommandsImplicits.DefaultWriteResultReader

    CommandCodecs.dealingWithGenericCommandErrorsReader[pack.type, List[Index]](pack) { doc =>
      decoder.child(doc, "cursor").map {
        decoder.children(_, "firstBatch")
      }.fold[List[Index]](throw GenericDriverException(
        "the cursor and firstBatch must be defined"))(readBatch(_, Nil))
    }
  }
}
