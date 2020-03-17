package reactivemongo.api.collections

import reactivemongo.api.SerializationPack

@deprecated("Will be removed", "0.16.0")
trait BatchCommands[P <: SerializationPack] {
  import reactivemongo.api.commands.{
    AggregationFramework => AC,
    FindAndModifyCommand => FNM
  }

  val pack: P

  val AggregationFramework: AC[pack.type]

  val FindAndModifyCommand: FNM[pack.type]
}
