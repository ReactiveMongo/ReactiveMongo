package reactivemongo.api.commands

import reactivemongo.api.SerializationPack
import reactivemongo.core.nodeset.NodeStatus

trait IsMasterCommand[P <: SerializationPack] {
  object IsMaster extends Command with CommandWithResult[IsMasterResult] with CommandWithPack[P]

  case class ReplicaSet(
    setName: String,
    me: String,
    primary: Option[String],
    hosts: Seq[String],
    passives: Seq[String],
    arbiters: Seq[String],
    isSecondary: Boolean, // `secondary`
    isArbiterOnly: Boolean, // `arbiterOnly`
    isPassive: Boolean, // `passive`
    isHidden: Boolean, // `hidden`
    tags: Option[P#Document])

  case class IsMasterResult(
      isMaster: Boolean, // `ismaster`
      maxBsonObjectSize: Int, // default = 16 * 1024 * 1024
      maxMessageSizeBytes: Int, // default = 48000000, mongod >= 2.4
      maxWriteBatchSize: Int, // default = 1000, mongod >= 2.6
      localTime: Option[Long], // date? mongod >= 2.2
      minWireVersion: Int, // int? mongod >= 2.6
      maxWireVersion: Int, // int? mongod >= 2.6
      replicaSet: Option[ReplicaSet], // flattened in the result
      msg: Option[String] // Contains the value isdbgrid when isMaster returns from a mongos instance.
      ) {
    def isMongos: Boolean = msg.isDefined
    def status: NodeStatus = if (isMaster) NodeStatus.Primary else if (replicaSet.exists(_.isSecondary)) NodeStatus.Secondary else NodeStatus.NonQueryableUnknownStatus
  }
}