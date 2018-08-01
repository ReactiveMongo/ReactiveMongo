package reactivemongo.api.collections

import reactivemongo.api.SerializationPack

trait BatchCommands[P <: SerializationPack] {
  import reactivemongo.api.commands.{
    AggregationFramework => AC,
    CountCommand => CC,
    DistinctCommand => DistC,
    InsertCommand => IC,
    UpdateCommand => UC,
    DeleteCommand => DC,
    DefaultWriteResult,
    ResolvedCollectionCommand,
    FindAndModifyCommand => FMC
  }

  val pack: P

  @deprecated("Will be removed", "0.16.0") val CountCommand: CC[pack.type]
  @deprecated("Will be removed", "0.16.0") implicit def CountWriter: pack.Writer[ResolvedCollectionCommand[CountCommand.Count]]
  @deprecated("Will be removed", "0.16.0") implicit def CountResultReader: pack.Reader[CountCommand.CountResult]

  val DistinctCommand: DistC[pack.type]
  implicit def DistinctWriter: pack.Writer[ResolvedCollectionCommand[DistinctCommand.Distinct]]
  implicit def DistinctResultReader: pack.Reader[DistinctCommand.DistinctResult]

  @deprecated("Will be removed", "0.16.0") val InsertCommand: IC[pack.type]
  @deprecated("Will be removed", "0.16.0") def InsertWriter: pack.Writer[ResolvedCollectionCommand[InsertCommand.Insert]]

  @deprecated("Will be removed", "0.16.0") val UpdateCommand: UC[pack.type]
  @deprecated("Will be removed", "0.16.0") def UpdateWriter: pack.Writer[ResolvedCollectionCommand[UpdateCommand.Update]]
  @deprecated("Will be removed", "0.16.0") def UpdateReader: pack.Reader[UpdateCommand.UpdateResult]

  @deprecated("Will be removed", "0.16.0") val DeleteCommand: DC[pack.type]

  @deprecated("Will use internal writer", "0.13.1")
  def DeleteWriter: pack.Writer[ResolvedCollectionCommand[DeleteCommand.Delete]]

  val FindAndModifyCommand: FMC[pack.type]
  @deprecated("Will use internal writer", "0.14.0")
  def FindAndModifyWriter: pack.Writer[ResolvedCollectionCommand[FindAndModifyCommand.FindAndModify]]
  implicit def FindAndModifyReader: pack.Reader[FindAndModifyCommand.FindAndModifyResult]

  val AggregationFramework: AC[pack.type]
  implicit def AggregateWriter: pack.Writer[ResolvedCollectionCommand[AggregationFramework.Aggregate]]
  implicit def AggregateReader: pack.Reader[AggregationFramework.AggregationResult]

  @deprecated("Use internal reader", "0.13.1")
  def DefaultWriteResultReader: pack.Reader[DefaultWriteResult]
}
