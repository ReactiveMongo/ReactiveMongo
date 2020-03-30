package reactivemongo.api.commands

import reactivemongo.api.{ Collation, SerializationPack, Session, WriteConcern }

import reactivemongo.core.protocol.MongoWireVersion

private[api] trait FindAndModifyCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] { self =>
  import pack._

  private[reactivemongo] final class FindAndModify(
    val query: Document,
    val modifier: FindAndModifyCommand.ModifyOp,
    val sort: Option[Document],
    val fields: Option[Document],
    val bypassDocumentValidation: Boolean,
    val writeConcern: WriteConcern,
    val maxTimeMS: Option[Int],
    val collation: Option[Collation],
    val arrayFilters: Seq[Document]) extends CollectionCommand
    with CommandWithPack[P]
    with CommandWithResult[FindAndModifyCommand.Result[pack.type]] {

    @inline def upsert = modifier.upsert
  }

  /** A modify operation, part of a FindAndModify command */
  sealed trait Modify extends FindAndModifyCommand.ModifyOp {
    /** Only for the `Update` modifier */
    def upsert = false
  }

  /**
   * Update (part of a FindAndModify command).
   *
   * @param update the modifier document.
   * @param fetchNewObject the command result must be the new object instead of the old one.
   * @param upsert if true, creates a new document if no document matches the query, or if documents match the query, findAndModify performs an update
   */
  final class Update private[api] (
    val update: Document,
    val fetchNewObject: Boolean,
    override val upsert: Boolean)
    extends Modify with FindAndModifyCommand.UpdateOp[pack.type] {
    val pack: self.pack.type = self.pack

    private[api] lazy val tupled = Tuple3(update, fetchNewObject, upsert)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"Update${tupled.toString}"
  }

  private[api] object Update {
    def unapply(update: Update): Option[(Document, Boolean, Boolean)] = Option(update).map(_.tupled)
  }

  /** Remove (part of a FindAndModify command). */
  object Remove extends Modify with FindAndModifyCommand.RemoveOp {
    override val upsert = false
  }
}

object FindAndModifyCommand {
  /** A modify operation, part of a FindAndModify command */
  sealed trait ModifyOp {
    /** Only for the `Update` modifier */
    def upsert: Boolean
  }

  /**
   * Update (part of a FindAndModify command).
   *
   * @param update the modifier document.
   * @param fetchNewObject the command result must be the new object instead of the old one.
   * @param upsert if true, creates a new document if no document matches the query, or if documents match the query, findAndModify performs an update
   */
  sealed trait UpdateOp[P <: SerializationPack with Singleton]
    extends ModifyOp {

    val pack: P

    def update: pack.Document

    def fetchNewObject: Boolean
  }

  /** Remove (part of a FindAndModify command). */
  sealed trait RemoveOp extends ModifyOp {
    val upsert = false
  }

  sealed abstract class UpdateLastError {
    private[api] type Pack <: SerializationPack
    private[api] val pack: Pack

    // TODO#1.1: Refactor
    lazy val upsertedId: Option[Any] = upserted.map { v => (v: Any) }
    def updatedExisting: Boolean = false
    def n: Int = 0
    def err: Option[String] = None

    /** Value of the upserted ID */
    private[api] def upserted: Option[pack.Value] = None

    // TODO#1.1: All fields after release
    override def equals(that: Any): Boolean = that match {
      case other: this.type => other.tupled == tupled
      case _                => false
    }

    // TODO#1.1: All fields after release
    override def hashCode: Int = tupled.hashCode

    private[commands] lazy val tupled =
      Tuple4(updatedExisting, upsertedId, n, err)
  }

  object UpdateLastError {
    private[api] type Aux[P] = UpdateLastError { type Pack = P }

    def apply[P <: SerializationPack](_pack: P)(
      updatedExisting: Boolean,
      upsertedId: Option[_pack.Value],
      n: Int,
      err: Option[String]): UpdateLastError.Aux[_pack.type] = {
      def ue = updatedExisting
      lazy val ui = upsertedId
      def _n = n
      def e = err

      new UpdateLastError {
        type Pack = _pack.type
        override val pack: Pack = _pack
        override val updatedExisting = ue
        override val upserted = ui
        override val n = _n
        override val err = e
      }
    }
  }

  sealed trait Result[P <: SerializationPack with Singleton] {
    val pack: P

    def lastError: Option[UpdateLastError]

    def value: Option[pack.Document]

    final def result[T](implicit reader: pack.Reader[T]): Option[T] =
      value.map(pack.deserialize(_, reader))

    private type R = Result[pack.type]

    override def equals(that: Any): Boolean = that match {
      case other: R =>
        (lastError -> value) == (other.lastError -> other.value)

      case _ => false
    }

    override def hashCode: Int = (lastError -> value).hashCode
  }

  private[reactivemongo] object Result {

    def apply[P <: SerializationPack with Singleton](_pack: P)(
      _lastError: Option[UpdateLastError],
      _value: Option[_pack.Document]): Result[_pack.type] =
      new Result[_pack.type] {
        val pack: _pack.type = _pack
        val lastError = _lastError
        val value = _value
      }
  }

  private[reactivemongo] def writer[P <: SerializationPack with Singleton](pack: P)(
    wireVer: MongoWireVersion,
    context: FindAndModifyCommand[pack.type]): Option[Session] => ResolvedCollectionCommand[context.FindAndModify] => pack.Document = {
    val builder = pack.newBuilder
    val writeWriteConcern = CommandCodecs.writeWriteConcern(builder)

    { session =>
      val sessionElmts: Seq[pack.ElementProducer] =
        session.fold(Seq.empty[pack.ElementProducer])(
          CommandCodecs.writeSession(builder))

      { cmd: ResolvedCollectionCommand[context.FindAndModify] =>

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

        if (wireVer.compareTo(MongoWireVersion.V40) >= 0 &&
          !session.exists(_.transaction.isSuccess)) {

          elements += element(
            "writeConcern", writeWriteConcern(command.writeConcern))
        }

        elements ++= sessionElmts

        command.fields.foreach { f =>
          elements += element("fields", f)
        }

        command.arrayFilters.headOption.foreach { f =>
          elements += element("arrayFilters", array(f, command.arrayFilters.tail))
        }

        command.modifier match {
          case op: UpdateOp[pack.type] @unchecked =>
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
  }
}
