package reactivemongo.api.commands

import reactivemongo.api.{ SerializationPack, Session, WriteConcern }

/**
 * Implements the [[https://docs.mongodb.com/manual/reference/command/insert/ insert]] command.
 */
private[reactivemongo] trait InsertCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  /**
   * @param head the first mandatory document
   * @param tail maybe other documents
   */
  final class Insert(
    val head: pack.Document,
    val tail: Seq[pack.Document],
    val ordered: Boolean,
    val writeConcern: WriteConcern,
    val bypassDocumentValidation: Boolean) extends CollectionCommand with CommandWithResult[InsertResult] {

    private[commands] lazy val tupled =
      Tuple5(head, tail, ordered, writeConcern, bypassDocumentValidation)

    override def equals(that: Any): Boolean = that match {
      case other: Insert =>
        other.tupled == this.tupled

      case _ => false
    }

    override def hashCode: Int = tupled.hashCode
  }

  type InsertResult = DefaultWriteResult // for simplified imports
}

@deprecated("Will be removed", "0.19.8")
private[reactivemongo] object InsertCommand {
  // TODO#1.1: Remove when BSONInsertCommand is removed
  def writer[P <: SerializationPack with Singleton](pack: P)(
    context: InsertCommand[pack.type]): Option[Session] => ResolvedCollectionCommand[context.Insert] => pack.Document = {
    val builder = pack.newBuilder
    val writeWriteConcern = CommandCodecs.writeWriteConcern(pack)
    val writeSession = CommandCodecs.writeSession(builder)

    { session: Option[Session] =>
      import builder.{ elementProducer => element }

      { insert =>
        import insert.command

        val documents = builder.array(command.head, command.tail)
        val ordered = builder.boolean(command.ordered)
        val elements = Seq.newBuilder[pack.ElementProducer]

        elements ++= Seq[pack.ElementProducer](
          element("insert", builder.string(insert.collection)),
          element("ordered", ordered),
          element("documents", documents))

        session.foreach { s =>
          elements ++= writeSession(s)
        }

        if (!session.exists(_.transaction.isSuccess)) {
          // writeConcern is not allowed within a multi-statement transaction
          // code=72

          elements += element(
            "writeConcern", writeWriteConcern(command.writeConcern))
        }

        builder.document(elements.result())
      }
    }
  }
}
