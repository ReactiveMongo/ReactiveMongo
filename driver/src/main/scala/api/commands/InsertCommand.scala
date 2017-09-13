package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

/**
 * Implements the [[https://docs.mongodb.com/manual/reference/command/insert/ insert]] command.
 */
trait InsertCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  case class Insert( // TODO: bypassDocumentValidation: bool
    documents: Seq[P#Document],
    ordered: Boolean,
    writeConcern: WriteConcern) extends CollectionCommand with CommandWithResult[InsertResult] with Mongo26WriteCommand

  type InsertResult = DefaultWriteResult // for simplified imports

  object Insert {
    def apply(firstDoc: ImplicitlyDocumentProducer, otherDocs: ImplicitlyDocumentProducer*): Insert = apply()(firstDoc, otherDocs: _*)

    def apply(ordered: Boolean = true, writeConcern: WriteConcern = WriteConcern.Default)(firstDoc: ImplicitlyDocumentProducer, otherDocs: ImplicitlyDocumentProducer*): Insert = new Insert(firstDoc.produce #:: otherDocs.toStream.map(_.produce), ordered, writeConcern)
  }
}
