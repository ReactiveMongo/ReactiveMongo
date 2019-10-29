package reactivemongo.api

import scala.util.{ Failure, Try }

import reactivemongo.api.bson.{
  BSONDocument,
  BSONDocumentReader,
  BSONDocumentWriter
}

import reactivemongo.api.collections.GenericCollection

import reactivemongo.api.commands.{
  CountCommand => CC,
  DeleteCommand => DC,
  InsertCommand => IC,
  ResolvedCollectionCommand,
  UpdateCommand => UC
}

/**
 * A Collection that interacts with the BSON library.
 */
private[reactivemongo] final class BSONCollection(
  val db: DB,
  val name: String,
  val failoverStrategy: FailoverStrategy,
  override val readPreference: ReadPreference) extends GenericCollection[Compat.SerializationPack] with CollectionMetaCommands { self =>

  val pack: Compat.SerializationPack = Compat.internalSerializationPack

  def withReadPreference(pref: ReadPreference): BSONCollection =
    new BSONCollection(db, name, failoverStrategy, pref)

  object BatchCommands
    extends reactivemongo.api.collections.BatchCommands[pack.type] { commands =>

    val pack: self.pack.type = self.pack

    object CountCommand extends CC[pack.type] {
      val pack = commands.pack
    }

    private def deprecated[T]: Try[T] =
      Failure(new UnsupportedOperationException("Deprecated"))

    object CountWriter extends BSONDocumentWriter[ResolvedCollectionCommand[CountCommand.Count]] {
      def writeTry(cmd: ResolvedCollectionCommand[CountCommand.Count]) = deprecated
    }

    object CountResultReader
      extends BSONDocumentReader[CountCommand.CountResult] {
      def readDocument(doc: BSONDocument) = deprecated
    }

    object InsertCommand extends IC[pack.type] {
      val pack = commands.pack
    }

    object InsertWriter extends BSONDocumentWriter[ResolvedCollectionCommand[InsertCommand.Insert]] {
      def writeTry(cmd: ResolvedCollectionCommand[InsertCommand.Insert]) = deprecated
    }

    object UpdateCommand extends UC[pack.type] {
      val pack = commands.pack
    }

    object UpdateWriter extends BSONDocumentWriter[ResolvedCollectionCommand[UpdateCommand.Update]] {
      def writeTry(cmd: ResolvedCollectionCommand[UpdateCommand.Update]) = deprecated
    }

    object UpdateReader extends BSONDocumentReader[UpdateCommand.UpdateResult] {
      def readDocument(doc: BSONDocument) = deprecated
    }

    object DeleteCommand extends DC[pack.type] {
      val pack = commands.pack
    }

    object DeleteWriter extends BSONDocumentWriter[ResolvedCollectionCommand[DeleteCommand.Delete]] {
      def writeTry(cmd: ResolvedCollectionCommand[DeleteCommand.Delete]) = deprecated
    }

    import reactivemongo.api.commands.{
      AggregationFramework => AggFramework,
      FindAndModifyCommand => FindAndModifyCmd
    }

    object FindAndModifyCommand
      extends FindAndModifyCmd[pack.type] {
      val pack = commands.pack
    }

    object FindAndModifyWriter extends BSONDocumentWriter[ResolvedCollectionCommand[FindAndModifyCommand.FindAndModify]] {
      def writeTry(cmd: ResolvedCollectionCommand[FindAndModifyCommand.FindAndModify]) = deprecated
    }

    object FindAndModifyReader
      extends BSONDocumentReader[FindAndModifyCommand.FindAndModifyResult] {
      def readDocument(doc: BSONDocument) = deprecated
    }

    object AggregationFramework extends AggFramework[pack.type] {
      val pack = commands.pack
    }

    object AggregateWriter extends BSONDocumentWriter[ResolvedCollectionCommand[AggregationFramework.Aggregate]] {
      def writeTry(cmd: ResolvedCollectionCommand[AggregationFramework.Aggregate]) = deprecated
    }

    object AggregateReader extends BSONDocumentReader[AggregationFramework.AggregationResult] {
      def readDocument(doc: BSONDocument) = deprecated
    }
  }
}
