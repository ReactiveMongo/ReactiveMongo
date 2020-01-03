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
  sealed class Insert(
    val head: pack.Document,
    val tail: Seq[pack.Document],
    val ordered: Boolean,
    val writeConcern: WriteConcern,
    val bypassDocumentValidation: Boolean) extends CollectionCommand with CommandWithResult[InsertResult] with Mongo26WriteCommand with Serializable with Product {

    @deprecated("Will be removed", "0.19.8")
    def this(
      head: pack.Document,
      tail: Seq[pack.Document],
      ordered: Boolean,
      writeConcern: WriteConcern) =
      this(head, tail, ordered, writeConcern, false)

    @deprecated("No longer a case class", "0.19.8")
    val productArity = 4

    @deprecated("No longer a case class", "0.19.8")
    def productElement(n: Int): Any = n match {
      case 0 => head
      case 1 => tail
      case 2 => ordered
      case 3 => writeConcern
      case _ => bypassDocumentValidation
    }

    def canEqual(that: Any): Boolean = that match {
      case _: Insert => true
      case _         => false
    }

    private[commands] lazy val tupled =
      Tuple4(head, tail, ordered, writeConcern)

    // TODO#1.1: All fields after release
    override def equals(that: Any): Boolean = that match {
      case other: Insert =>
        other.tupled == this.tupled

      case _ => false
    }

    // TODO#1.1: All fields after release
    override def hashCode: Int = tupled.hashCode
  }

  object Insert {
    @deprecated("Use factory with bypassDocumentValidation", "0.19.8")
    def apply(
      head: pack.Document,
      tail: Seq[pack.Document],
      ordered: Boolean,
      writeConcern: WriteConcern): Insert =
      apply(head, tail, ordered, writeConcern, false)

    def apply(
      head: pack.Document,
      tail: Seq[pack.Document],
      ordered: Boolean,
      writeConcern: WriteConcern,
      bypassDocumentValidation: Boolean): Insert =
      new Insert(head, tail, ordered, writeConcern, bypassDocumentValidation)

    @deprecated("No longer a case class", "0.19.8")
    def unapply(that: Any): Option[(pack.Document, Seq[pack.Document], Boolean, WriteConcern)] = that match {
      case other: Insert => Option(other).map(_.tupled)
      case _             => None
    }
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
