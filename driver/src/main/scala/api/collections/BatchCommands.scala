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
    //LastError,
    ResolvedCollectionCommand,
    FindAndModifyCommand => FMC
  }

  val pack: P

  val CountCommand: CC[pack.type]
  implicit def CountWriter: pack.Writer[ResolvedCollectionCommand[CountCommand.Count]]
  implicit def CountResultReader: pack.Reader[CountCommand.CountResult]

  val DistinctCommand: DistC[pack.type]
  implicit def DistinctWriter: pack.Writer[ResolvedCollectionCommand[DistinctCommand.Distinct]]
  implicit def DistinctResultReader: pack.Reader[DistinctCommand.DistinctResult]

  val InsertCommand: IC[pack.type]
  implicit def InsertWriter: pack.Writer[ResolvedCollectionCommand[InsertCommand.Insert]]

  val UpdateCommand: UC[pack.type]
  implicit def UpdateWriter: pack.Writer[ResolvedCollectionCommand[UpdateCommand.Update]]
  implicit def UpdateReader: pack.Reader[UpdateCommand.UpdateResult]

  val DeleteCommand: DC[pack.type]

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

  //implicit def LastErrorReader: pack.Reader[LastError]
}
