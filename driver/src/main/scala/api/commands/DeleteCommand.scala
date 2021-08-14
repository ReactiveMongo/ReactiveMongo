package reactivemongo.api.commands

import reactivemongo.api.{
  Collation,
  PackSupport,
  SerializationPack,
  Session,
  WriteConcern
}

/**
 * Implements the [[https://docs.mongodb.com/manual/reference/command/delete/ delete]] command.
 */
private[reactivemongo] trait DeleteCommand[P <: SerializationPack] { self: PackSupport[P] =>

  private[api] final class Delete(
    val deletes: Seq[DeleteElement],
    val ordered: Boolean,
    val writeConcern: WriteConcern) extends CollectionCommand with CommandWithResult[DeleteResult] {
    val commandKind = CommandKind.Delete
  }

  /** Delete command element */
  final class DeleteElement private[api] (
    _q: pack.Document,
    _limit: Int,
    _collation: Option[Collation]) {

    /** The query that matches documents to delete */
    @inline def q: pack.Document = _q

    /** The number of matching documents to delete */
    @inline def limit: Int = _limit

    /** The collation to use for the operation */
    @inline def collation: Option[Collation] = _collation

    override def toString = s"DeleteElement${tupled.toString}"

    override def hashCode: Int = tupled.hashCode

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    private lazy val tupled = (_q, _limit, _collation)
  }

  /** Result for a delete command */
  final type DeleteResult = DefaultWriteResult

  protected final type DeleteCmd = ResolvedCollectionCommand[Delete]

  private[reactivemongo] def session(): Option[Session]

  protected final implicit lazy val deleteWriter: pack.Writer[DeleteCmd] =
    deleteWriter(self.session())

  protected final def deleteWriter(
    session: Option[Session]): pack.Writer[DeleteCmd] = {
    val builder = pack.newBuilder

    import builder.{ elementProducer => element }

    val writeWriteConcern = CommandCodecs.writeWriteConcern(builder)
    val writeSession = CommandCodecs.writeSession(builder)

    pack.writer[DeleteCmd] { delete =>
      val elements = Seq.newBuilder[pack.ElementProducer]

      elements ++= Seq(
        element("delete", builder.string(delete.collection)),
        element("ordered", builder.boolean(delete.command.ordered)),
        element("writeConcern", writeWriteConcern(delete.command.writeConcern)))

      session.foreach { s =>
        elements ++= writeSession(s)
      }

      delete.command.deletes.headOption.foreach { first =>
        elements += element("deletes", builder.array(
          writeElement(builder, first) +:
            delete.command.deletes.map(writeElement(builder, _))))
      }

      builder.document(elements.result())
    }
  }

  private[api] def writeElement(
    builder: SerializationPack.Builder[pack.type],
    e: DeleteElement): pack.Document = {

    import builder.{ elementProducer => element }

    val elements = Seq.newBuilder[pack.ElementProducer]

    elements ++= Seq(
      element("q", e.q),
      element("limit", builder.int(e.limit)))

    e.collation.foreach { c =>
      elements += element(
        "collation",
        Collation.serializeWith(pack, c)(builder))
    }

    builder.document(elements.result())
  }
}
