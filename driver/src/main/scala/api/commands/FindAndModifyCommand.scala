package reactivemongo.api.commands

import reactivemongo.api.{
  Collation,
  PackSupport,
  SerializationPack,
  Session,
  WriteConcern
}

import reactivemongo.core.protocol.MongoWireVersion

private[api] trait FindAndModifyCommand[P <: SerializationPack] {
  _: PackSupport[P] =>

  private[reactivemongo] final class FindAndModify(
    val query: pack.Document,
    val modifier: FindAndModifyOp,
    val sort: Option[pack.Document],
    val fields: Option[pack.Document],
    val bypassDocumentValidation: Boolean,
    val writeConcern: WriteConcern,
    val maxTimeMS: Option[Int],
    val collation: Option[Collation],
    val arrayFilters: Seq[pack.Document]) extends CollectionCommand
    with CommandWithPack[P]
    with CommandWithResult[FindAndModifyResult]

  protected[reactivemongo] final type FindAndModifyCmd = ResolvedCollectionCommand[FindAndModify]

  /** A modify operation, part of a FindAndModify command */
  sealed trait FindAndModifyOp

  /**
   * Update (part of a FindAndModify command).
   *
   * @param update the modifier document.
   * @param fetchNewObject the command result must be the new object instead of the old one.
   * @param upsert if true, creates a new document if no document matches the query, or if documents match the query, findAndModify performs an update
   */
  final class FindAndUpdateOp private[api] (
    val update: pack.Document,
    val fetchNewObject: Boolean,
    val upsert: Boolean)
    extends FindAndModifyOp {
    private[api] lazy val tupled = Tuple3(update, fetchNewObject, upsert)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"FindAndUpdate${tupled.toString}"
  }

  /** Remove (part of a FindAndModify command). */
  object FindAndRemoveOp extends FindAndModifyOp

  /**
   * @param upserted the value of the upserted ID
   */
  final class FindAndUpdateLastError private[api] (
    val updatedExisting: Boolean,
    val upserted: Option[pack.Value],
    val n: Int,
    val err: Option[String]) {

    override def equals(that: Any): Boolean = that match {
      case other: this.type => other.tupled == tupled
      case _                => false
    }

    override def hashCode: Int = tupled.hashCode

    private[commands] lazy val tupled =
      Tuple4(updatedExisting, upserted, n, err)
  }

  final class FindAndModifyResult private[api] (
    val lastError: Option[FindAndUpdateLastError],
    val value: Option[pack.Document]) {

    def result[T](implicit reader: pack.Reader[T]): Option[T] =
      value.map(pack.deserialize(_, reader))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        (lastError -> value) == (other.lastError -> other.value)

      case _ => false
    }

    override def hashCode: Int = (lastError -> value).hashCode
  }

  private[reactivemongo] def session(): Option[Session]

  protected def maxWireVersion: MongoWireVersion

  implicit private[reactivemongo] lazy val findAndModifyWriter: pack.Writer[FindAndModifyCmd] = {
    val builder = pack.newBuilder
    val writeWriteConcern = CommandCodecs.writeWriteConcern(builder)

    val sessionElmts: Seq[pack.ElementProducer] =
      session().fold(Seq.empty[pack.ElementProducer])(
        CommandCodecs.writeSession(builder))

    pack.writer[FindAndModifyCmd] { cmd =>
      import builder.{
        array,
        boolean,
        elementProducer => element,
        int,
        string
      }
      import cmd.command

      val elements = Seq.newBuilder[pack.ElementProducer]

      elements ++= Seq(
        element("findAndModify", string(cmd.collection)),
        element("query", command.query),
        element("bypassDocumentValidation", boolean(
          command.bypassDocumentValidation)))

      if (maxWireVersion.compareTo(MongoWireVersion.V40) >= 0 &&
        !session().exists(_.transaction.isSuccess)) {

        elements += element(
          "writeConcern", writeWriteConcern(command.writeConcern))
      }

      elements ++= sessionElmts

      command.fields.foreach { f =>
        elements += element("fields", f)
      }

      if (command.arrayFilters.nonEmpty) {
        elements += element("arrayFilters", array(command.arrayFilters))
      }

      command.modifier match {
        case op: FindAndUpdateOp =>
          elements ++= Seq(
            element("upsert", boolean(op.upsert)),
            element("update", op.update),
            element("new", boolean(op.fetchNewObject)))

        case _ =>
          elements += element("remove", boolean(true))
      }

      command.sort.foreach { s =>
        elements += element("sort", s)
      }

      command.maxTimeMS.foreach { ms =>
        elements += element("maxTimeMS", int(ms))
      }

      command.collation.foreach { c =>
        elements += element(
          "collation", Collation.serializeWith(pack, c)(builder))
      }

      builder.document(elements.result())
    }
  }

  final private[reactivemongo] implicit lazy val findAndModifyReader: pack.Reader[FindAndModifyResult] = {
    val decoder: SerializationPack.Decoder[pack.type] = pack.newDecoder

    CommandCodecs.dealingWithGenericCommandExceptionsReader(pack) { result =>
      new FindAndModifyResult(
        decoder.child(result, "lastErrorObject").map { doc =>
          new FindAndUpdateLastError(
            decoder.booleanLike(
              doc, "updatedExisting").getOrElse(false),
            decoder.get(doc, "upserted"),
            decoder.int(doc, "n").getOrElse(0),
            decoder.string(doc, "err"))
        },
        decoder.child(result, "value"))
    }
  }
}
