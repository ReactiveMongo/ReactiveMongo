package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

trait FindAndModifyCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  import pack._

  case class FindAndModify(
    query: Document,
    modify: Modify,
    sort: Option[Document],
    fields: Option[Document],
    bypassDocumentValidation: Boolean,
    writeConcern: WriteConcern,
    maxTimeMS: Option[Int],
    collation: Option[Collation],
    arrayFilters: Seq[Document]) extends CollectionCommand with CommandWithPack[P] with CommandWithResult[FindAndModifyResult] {
    def upsert = modify.upsert
  }

  object FindAndModify {
    @deprecated("Use other `apply`", "0.14.0")
    def apply(query: ImplicitlyDocumentProducer, modify: Modify, sort: Option[ImplicitlyDocumentProducer] = None, fields: Option[ImplicitlyDocumentProducer] = None): FindAndModify =
      FindAndModify(
        query.produce,
        modify,
        sort.map(_.produce),
        fields.map(_.produce),
        bypassDocumentValidation = false,
        writeConcern = GetLastError.Default,
        maxTimeMS = Option.empty,
        collation = Option.empty,
        arrayFilters = Seq.empty)

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
      FindAndModify(
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
  sealed trait Modify {
    /** Only for the [[Update]] modifier */
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
    override val upsert: Boolean) extends Modify

  object Update {
    def apply(update: ImplicitlyDocumentProducer, fetchNewObject: Boolean = false, upsert: Boolean = false): Update = Update(update.produce, fetchNewObject, upsert)
  }

  /** Remove (part of a FindAndModify command). */
  object Remove extends Modify

  case class UpdateLastError(
    updatedExisting: Boolean,
    upsertedId: Option[Any], // TODO. It is the id of the upserted value
    n: Int,
    err: Option[String])

  case class FindAndModifyResult(
    lastError: Option[UpdateLastError],
    value: Option[Document]) {
    def result[R](implicit reader: Reader[R]): Option[R] =
      value.map(pack.deserialize(_, reader))
  }

  def serialize(
    cmd: ResolvedCollectionCommand[FindAndModify]): pack.Document = {
    val builder = pack.newBuilder

    import builder.{
      array,
      boolean,
      elementProducer => element,
      int,
      string
    }
    import cmd.command

    val elements = Seq.newBuilder[pack.ElementProducer]
    val writeWriteConcern = CommandCodecs.writeWriteConcern[pack.type](builder)

    elements ++= Seq(
      element("findAndModify", string(cmd.collection)),
      element("query", command.query),
      element("bypassDocumentValidation", boolean(
        command.bypassDocumentValidation)),
      element("writeConcern", writeWriteConcern(command.writeConcern)))

    command.fields.foreach { f =>
      elements += element("fields", f)
    }

    command.arrayFilters.headOption.foreach { f =>
      elements += element("arrayFilters", array(f, command.arrayFilters.tail))
    }

    command.modify match {
      case Update(document, fetchNewObject, upsert) =>
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
