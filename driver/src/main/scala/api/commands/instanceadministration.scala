package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

@deprecated("Will be private/internal", "0.16.0")
object DropDatabase extends Command with CommandWithResult[UnitBox.type]

/**
 * @param dropped true if the collection existed and was dropped
 */
@deprecated("Will be private/internal", "0.16.0")
case class DropCollectionResult(dropped: Boolean)

@deprecated("Will be private/internal", "0.16.0")
object DropCollection extends CollectionCommand
  with CommandWithResult[DropCollectionResult]

@deprecated("Will be private/internal", "0.16.0")
object EmptyCapped extends CollectionCommand
  with CommandWithResult[UnitBox.type]

@deprecated("Will be private/internal", "0.16.0")
case class RenameCollection(
  fullyQualifiedCollectionName: String,
  fullyQualifiedTargetName: String,
  dropTarget: Boolean = false) extends Command with CommandWithResult[UnitBox.type]

@deprecated("Will be private/internal", "0.16.0")
case class Create(
  capped: Option[Capped] = None, // if set, "capped" -> true, size -> <int>, max -> <int>
  autoIndexId: Boolean = true, // optional
  flags: Int = 1 // defaults to 1
) extends CollectionCommand with CommandWithResult[UnitBox.type]

@deprecated("Will be private/internal", "0.16.0")
case class Capped(
  size: Long,
  max: Option[Int] = None)

@deprecated("Will be private/internal", "0.16.0")
case class ConvertToCapped(
  capped: Capped) extends CollectionCommand with CommandWithResult[UnitBox.type]

@deprecated("Will be private/internal", "0.16.0")
case class DropIndexes(index: String) extends CollectionCommand with CommandWithResult[DropIndexesResult]

@deprecated("Will be private/internal", "0.16.0")
case class DropIndexesResult(value: Int) extends BoxedAnyVal[Int]

@deprecated("Will be private/internal", "0.16.0")
case class CollectionNames(names: List[String])

/** List the names of DB collections. */
@deprecated("Will be private/internal", "0.16.0")
object ListCollectionNames
  extends Command with CommandWithResult[CollectionNames]

import reactivemongo.api.indexes.Index

/**
 * Lists the indexes of the specified collection.
 *
 * @param db the database name
 */
@deprecated("Will be private/internal", "0.16.0")
case class ListIndexes(db: String) extends CollectionCommand
  with CommandWithResult[List[Index]]

/**
 * Creates the given indexes on the specified collection.
 *
 * @param db the database name
 * @param indexes the indexes to be created
 */
@deprecated("Will be private/internal", "0.16.0")
case class CreateIndexes(db: String, indexes: List[Index])
  extends CollectionCommand with CommandWithResult[WriteResult]

/**
 * Replica set member.
 *
 * @param name the member name (e.g. "host:port")
 * @param health the health indicator for this member
 * @param state the [[http://docs.mongodb.org/manual/reference/replica-states/ state code]] of the member
 * @param stateStr the string representation of the state
 * @param uptime the number of seconds that this member has been online
 * @param optime information regarding the last operation from the operation log that this member has applied
 * @param lastHeartbeat the time of the transmission time of last heartbeat received from this member
 * @param lastHeartbeatRecv the time that the last heartbeat was received from this member
 * @param lastHeartbeatMessage an optional message from the last heartbeat
 * @param electionTime if the member is the primary, the time it was elected as
 * @param self indicates which replica set member processed the replSetGetStatus command
 * @param pingMs the number of milliseconds (ms) that a round-trip packet takes to travel between the remote member and the local instance (does not appear for the member that returns the `rs.status()` data)
 * @param syncingTo the hostname of the member from which this instance is syncing (only present on the output of `rs.status()` on secondary and recovering members)
 * @param configVersion the configuration version (since MongoDB 3.0)
 */
case class ReplSetMember(
  _id: Long,
  name: String,
  health: Int,
  state: Int,
  stateStr: String,
  uptime: Long,
  optime: Long,
  lastHeartbeat: Option[Long],
  lastHeartbeatRecv: Option[Long],
  lastHeartbeatMessage: Option[String],
  electionTime: Option[Long],
  self: Boolean,
  pingMs: Option[Long],
  syncingTo: Option[String],
  configVersion: Option[Int])

/**
 * Result from the [[http://docs.mongodb.org/manual/reference/command/replSetGetStatus/ replSetGetStatus]].
 *
 * @param name the name of the replica set
 * @param time the current server time
 * @param state the [[http://docs.mongodb.org/manual/reference/replica-states/ state code]] of the current member
 * @param members the list of the members of this replicate set
 */
case class ReplSetStatus(
  name: String,
  time: Long,
  myState: Int,
  members: List[ReplSetMember])

/**
 * The command [[http://docs.mongodb.org/manual/reference/command/replSetGetStatus/ replSetGetStatus]]
 */
@deprecated("Will be private/internal", "0.16.0")
case object ReplSetGetStatus
  extends Command with CommandWithResult[ReplSetStatus]

sealed trait ServerProcess

object ServerProcess {
  def unapply(repr: String): Option[ServerProcess] = repr match {
    case "mongos" => Some(MongosProcess)
    case "mongod" => Some(MongodProcess)
    case _        => None
  }
}

case object MongodProcess extends ServerProcess {
  override val toString = "mongod"
}
case object MongosProcess extends ServerProcess {
  override val toString = "mongos"
}

object ResyncResult extends BoxedAnyVal[Unit] {
  val value = {}
}

/**
 * The command [[https://docs.mongodb.org/manual/reference/command/resync/ resync]]
 */
@deprecated("Will be private/internal", "0.16.0")
object Resync extends Command with CommandWithResult[ResyncResult.type]

/**
 * The [[https://docs.mongodb.org/manual/reference/command/replSetMaintenance/ replSetMaintenance]] command.
 * It must be executed against the `admin` database.
 *
 * @param enable if true the the member enters the `RECOVERING` state
 */
@deprecated("Will be private/internal", "0.16.0")
case class ReplSetMaintenance(enable: Boolean = true) extends Command
  with CommandWithResult[UnitBox.type]

import reactivemongo.api.SerializationPack

/**
 * @param name the name of user role
 */
class UserRole(val name: String)

/**
 * @param db the name of the database
 */
case class DBUserRole(
  override val name: String,
  db: String) extends UserRole(name)

/** User role extractor */
object UserRole {
  def apply(name: String): UserRole = new UserRole(name)
  def unapply(role: UserRole): Option[String] = Some(role.name)
}

@deprecated("Will be private/internal", "0.16.0")
trait CreateUserCommand[P <: SerializationPack]
  extends ImplicitCommandHelpers[P] {

  /**
   * The [[https://docs.mongodb.com/manual/reference/command/createUser/ createUser]] command.
   *
   * @param name the name of the user to be created
   * @param pwd the user password (not required if the database uses external credentials)
   * @param roles the roles granted to the user, possibly an empty to create users without roles
   * @param digestPassword when true, the mongod instance will create the hash of the user password (default: `true`)
   * @param writeConcern the optional level of [[https://docs.mongodb.com/manual/reference/write-concern/ write concern]]
   * @param customData the custom data to associate with the user account
   */
  case class CreateUser(
    name: String,
    pwd: Option[String],
    roles: List[UserRole],
    digestPassword: Boolean = true,
    writeConcern: Option[WriteConcern] = None,
    customData: Option[pack.Document] = None) extends Command with CommandWithPack[P]
    with CommandWithResult[UnitBox.type]

}

/**
 * The [[https://docs.mongodb.com/manual/reference/command/ping/ ping]] command.
 */
@deprecated("Will be private/internal", "0.16.0")
case object PingCommand extends Command with CommandWithResult[Boolean]
