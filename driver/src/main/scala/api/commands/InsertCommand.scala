package reactivemongo.api.commands

import reactivemongo.api.{ SerializationPack, Session }

/**
 * Implements the [[https://docs.mongodb.com/manual/reference/command/insert/ insert]] command.
 */
trait InsertCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  /**
   * @param head the first mandatory document
   * @param tail maybe other documents
   */
  case class Insert( // TODO: bypassDocumentValidation: bool
    head: pack.Document,
    tail: Seq[pack.Document],
    ordered: Boolean,
    writeConcern: WriteConcern) extends CollectionCommand with CommandWithResult[InsertResult] with Mongo26WriteCommand

  type InsertResult = DefaultWriteResult // for simplified imports

  @deprecated("Useless, will be removed", "0.16.0")
  object Insert {
  }
}

private[reactivemongo] object InsertCommand {
  // TODO: Unit test
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
          element("writeConcern", writeWriteConcern(command.writeConcern)),
          element("documents", documents))

        session.foreach { writeSession(elements)(_) }

        builder.document(elements.result())
      }
    }
  }
}
