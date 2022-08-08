package reactivemongo.api.commands

import reactivemongo.api.{
  PackSupport,
  SerializationPack,
  Session,
  WriteConcern
}

/**
 * Implements the [[https://docs.mongodb.com/manual/reference/command/insert/ insert]] command.
 */
private[reactivemongo] trait InsertCommand[P <: SerializationPack] {
  self: PackSupport[P] =>

  /**
   * @param head the first mandatory document
   * @param tail maybe other documents
   */
  private[reactivemongo] final class Insert(
      val head: pack.Document,
      val tail: Seq[pack.Document],
      val ordered: Boolean,
      val writeConcern: WriteConcern,
      val bypassDocumentValidation: Boolean)
      extends CollectionCommand
      with CommandWithResult[InsertResult] {

    val commandKind = CommandKind.Insert

    private[commands] lazy val tupled =
      Tuple5(head, tail, ordered, writeConcern, bypassDocumentValidation)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        other.tupled == this.tupled

      case _ => false
    }

    override def hashCode: Int = tupled.hashCode

    @inline override lazy val toString: String = {
      val docs = (head +: tail).map(pack.pretty)

      s"""Insert(${docs.mkString(
          "[",
          ", ",
          "]"
        )}, ${ordered.toString}, ${writeConcern.toString}, ${bypassDocumentValidation.toString})"""
    }
  }

  private[reactivemongo] type InsertResult =
    DefaultWriteResult // for simplified imports

  private[reactivemongo] final type InsertCmd =
    ResolvedCollectionCommand[Insert]

  private[reactivemongo] def session(): Option[Session]

  implicit private[reactivemongo] final lazy val insertWriter: pack.Writer[InsertCmd] =
    insertWriter(self.session())

  private[reactivemongo] final def insertWriter(
      session: Option[Session]
    ): pack.Writer[InsertCmd] = {

    val builder = pack.newBuilder
    val writeWriteConcern = CommandCodecs.writeWriteConcern(pack)
    val writeSession = CommandCodecs.writeSession(builder)

    import builder.{ elementProducer => element }

    pack.writer[InsertCmd] { insert =>
      import insert.command

      val documents = builder.array(command.head +: command.tail)
      val ordered = builder.boolean(command.ordered)
      val elements = Seq.newBuilder[pack.ElementProducer]

      elements ++= Seq[pack.ElementProducer](
        element("insert", builder.string(insert.collection)),
        element("ordered", ordered),
        element("documents", documents),
        element(
          "bypassDocumentValidation",
          builder.boolean(command.bypassDocumentValidation)
        )
      )

      session.foreach { s => elements ++= writeSession(s) }

      if (!session.exists(_.transaction.isSuccess)) {
        // writeConcern is not allowed within a multi-statement transaction
        // code=72

        elements += element(
          "writeConcern",
          writeWriteConcern(command.writeConcern)
        )
      }

      builder.document(elements.result())
    }
  }
}
