package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

trait DistinctCommand[P <: SerializationPack] extends ImplicitCommandHelpers[P] {
  case class Distinct(
    //{ distinct: <collection>, key: <key>, query: <query> }
    keyString: String,
    query: Option[pack.Document]) extends CollectionCommand with CommandWithPack[pack.type] with CommandWithResult[DistinctResult]

  case class DistinctResult(values: List[pack.Value])
}
