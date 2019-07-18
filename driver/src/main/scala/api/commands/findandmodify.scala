package reactivemongo.api.commands

import reactivemongo.api.{ SerializationPack, Session }

import reactivemongo.core.protocol.MongoWireVersion

@deprecated("Internal: will be made private", "0.16.0")
trait FindAndModifyCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] { self =>
  import pack._

  class FindAndModify(
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
    with CommandWithResult[FindAndModifyCommand.Result[pack.type]]
    with Product with Serializable {

    def upsert = modify.upsert

    @deprecated("Use `modifier`", "0.18.0")
    lazy val modify: Modify = {
      type UpdateOp = FindAndModifyCommand.UpdateOp[pack.type]

      modifier match {
        case u: UpdateOp => Update(
          update = u.update,
          fetchNewObject = u.fetchNewObject,
          upsert = u.upsert)

        case _ =>
          Remove
      }
    }

    def canEqual(that: Any): Boolean = that match {
      case _: FindAndModify => true
      case _                => false
    }

    @deprecated("Will no longer be a product", "0.18.0")
    val productArity = 9

    @deprecated("Will no longer be a product", "0.18.0")
    def productElement(n: Int): Any = (n: @annotation.switch) match {
      case 0 => query
      case 1 => modifier
      case 2 => sort
      case 3 => fields
      case 4 => bypassDocumentValidation
      case 5 => writeConcern
      case 6 => maxTimeMS
      case 7 => collation
      case _ => arrayFilters
    }
  }

  object FindAndModify {
    @deprecated("Use other `apply`", "0.14.0")
    def apply(query: ImplicitlyDocumentProducer, modify: Modify, sort: Option[ImplicitlyDocumentProducer] = None, fields: Option[ImplicitlyDocumentProducer] = None): FindAndModify =
      new FindAndModify(
        query.produce,
        modify,
        sort.map(_.produce),
        fields.map(_.produce),
        bypassDocumentValidation = false,
        writeConcern = GetLastError.Default,
        maxTimeMS = Option.empty,
        collation = Option.empty,
        arrayFilters = Seq.empty)

    @deprecated("Use other `apply`", "0.18.0")
    def apply(
      query: ImplicitlyDocumentProducer,
      modify: Modify,
      sort: Option[ImplicitlyDocumentProducer],
      fields: Option[ImplicitlyDocumentProducer],
      bypassDocumentValidation: Boolean,
      writeConcern: WriteConcern,
      maxTimeMS: Option[Int],
      collation: Option[Collation],
      arrayFilters: Seq[ImplicitlyDocumentProducer]): FindAndModify =
      new FindAndModify(
        query.produce,
        modify,
        sort.map(_.produce),
        fields.map(_.produce),
        bypassDocumentValidation,
        writeConcern,
        maxTimeMS,
        collation,
        arrayFilters.map(_.produce))

    def apply(
      query: ImplicitlyDocumentProducer,
      modify: FindAndModifyCommand.ModifyOp,
      sort: Option[ImplicitlyDocumentProducer],
      fields: Option[ImplicitlyDocumentProducer],
      bypassDocumentValidation: Boolean,
      writeConcern: WriteConcern,
      maxTimeMS: Option[Int],
      collation: Option[Collation],
      arrayFilters: Seq[ImplicitlyDocumentProducer]): FindAndModify =
      new FindAndModify(
        query.produce,
        modify,
        sort.map(_.produce),
        fields.map(_.produce),
        bypassDocumentValidation,
        writeConcern,
        maxTimeMS,
        collation,
        arrayFilters.map(_.produce))
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
  case class Update(
    update: Document,
    fetchNewObject: Boolean,
    override val upsert: Boolean) extends Modify with FindAndModifyCommand.UpdateOp[pack.type] {
    val pack: self.pack.type = self.pack
  }

  object Update {
    def apply(update: ImplicitlyDocumentProducer, fetchNewObject: Boolean = false, upsert: Boolean = false): Update = Update(update.produce, fetchNewObject, upsert)
  }

  /** Remove (part of a FindAndModify command). */
  object Remove extends Modify with FindAndModifyCommand.RemoveOp {
    override val upsert = false
  }

  @deprecated("Use `FindAndModifyCommand.UpdateLastError`", "0.18.0")
  type UpdateLastError = FindAndModifyCommand.UpdateLastError

  @deprecated("Use `FindAndModifyCommand.FindAndModifyResult`", "0.18.0")
  case class FindAndModifyResult(
    lastError: Option[UpdateLastError],
    value: Option[pack.Document]) extends FindAndModifyCommand.Result[pack.type] {
    val pack: self.pack.type = self.pack
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
  trait UpdateOp[P <: SerializationPack with Singleton] extends ModifyOp {
    val pack: P

    def update: pack.Document

    def fetchNewObject: Boolean
  }

  /** Remove (part of a FindAndModify command). */
  trait RemoveOp extends ModifyOp {
    val upsert = false
  }

  case class UpdateLastError(
    updatedExisting: Boolean,
    upsertedId: Option[Any], // TODO. It is the id of the upserted value
    n: Int,
    err: Option[String])

  trait Result[P <: SerializationPack with Singleton] extends Serializable {
    val pack: P

    def lastError: Option[UpdateLastError]

    def value: Option[pack.Document]

    def result[T](implicit reader: pack.Reader[T]): Option[T] =
      value.map(pack.deserialize(_, reader))

    private type R = Result[pack.type]

    override def equals(that: Any): Boolean = that match {
      case other: R =>
        (lastError -> value) == (other.lastError -> other.value)

      case _ => false
    }

    override def hashCode: Int = (lastError -> value).hashCode
  }

  import reactivemongo.bson.BSONDocument
  import reactivemongo.api.BSONSerializationPack

  private[reactivemongo] object Result extends scala.runtime.AbstractFunction2[Option[UpdateLastError], Option[BSONDocument], Result[BSONSerializationPack.type]] {

    def apply[P <: SerializationPack with Singleton](_pack: P)(
      _lastError: Option[UpdateLastError],
      _value: Option[_pack.Document]): Result[_pack.type] =
      new Result[_pack.type] {
        val pack: _pack.type = _pack
        val lastError = _lastError
        val value = _value
      }

    @deprecated("Will be removed", "0.18.0")
    @inline def apply(
      lastError: Option[UpdateLastError],
      value: Option[BSONDocument]): Result[BSONSerializationPack.type] =
      apply(BSONSerializationPack)(lastError, value)

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
          !session.exists(_.transaction.isDefined)) {

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

        command.modify match {
          case context.Update(document, fetchNewObject, upsert) =>
            elements ++= Seq(
              element("upsert", boolean(upsert)),
              element("update", document),
              element("new", boolean(fetchNewObject)))

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
