package reactivemongo.api.commands

import reactivemongo.api.{ SerializationPack, Session }

/**
 * Implements the [[https://docs.mongodb.com/manual/reference/command/insert/ insert]] command.
 */
@deprecated("Use the new insert operation", "0.16.0")
trait InsertCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  /**
   * @param head the first mandatory document
   * @param tail maybe other documents
   */
  case class Insert(
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
  // TODO: Move in InsertOps as internal function
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
