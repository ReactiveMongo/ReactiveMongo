package reactivemongo.api.collections

import reactivemongo.api.SerializationPack

@deprecated("Will be removed", "0.16.0")
trait BatchCommands[P <: SerializationPack] {
  import reactivemongo.api.commands.{
    AggregationFramework => AC,
    DistinctCommand => DistC,
    InsertCommand => IC,
    UpdateCommand => UC,
    DeleteCommand => DC,
    ResolvedCollectionCommand,
    FindAndModifyCommand => FMC
  }

  val pack: P

  val DistinctCommand: DistC[pack.type]
  def DistinctWriter: pack.Writer[ResolvedCollectionCommand[DistinctCommand.Distinct]]
  def DistinctResultReader: pack.Reader[DistinctCommand.DistinctResult]

  val InsertCommand: IC[pack.type]
  def InsertWriter: pack.Writer[ResolvedCollectionCommand[InsertCommand.Insert]]

  val UpdateCommand: UC[pack.type]
  def UpdateWriter: pack.Writer[ResolvedCollectionCommand[UpdateCommand.Update]]
  def UpdateReader: pack.Reader[UpdateCommand.UpdateResult]

  val DeleteCommand: DC[pack.type]

  @deprecated("Will use internal writer", "0.13.1")
  def DeleteWriter: pack.Writer[ResolvedCollectionCommand[DeleteCommand.Delete]]

  val FindAndModifyCommand: FMC[pack.type]
  @deprecated("Will use internal writer", "0.14.0")
  def FindAndModifyWriter: pack.Writer[ResolvedCollectionCommand[FindAndModifyCommand.FindAndModify]]
  implicit def FindAndModifyReader: pack.Reader[FindAndModifyCommand.FindAndModifyResult]

  val AggregationFramework: AC[pack.type]
  def AggregateWriter: pack.Writer[ResolvedCollectionCommand[AggregationFramework.Aggregate]]
  def AggregateReader: pack.Reader[AggregationFramework.AggregationResult]
}
