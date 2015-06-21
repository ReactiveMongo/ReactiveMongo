package reactivemongo.api.commands

import reactivemongo.bson.BSONDocument

/**
 * Created by sh1ng on 20/06/15.
 */

trait Role

case class RoleInTheSameDB(name: String) extends Role
case class RoleWithDB(name: String, db: String) extends Role

case class CreateUser(createUser: String, pwd: String, roles: List[Role], customData: BSONDocument = BSONDocument.empty, writeConcern: WriteConcern = WriteConcern.Default)
  extends Command with CommandWithResult[UnitBox.type]

case class UpdateUser(updateUser: String, pwd: String, roles: List[Role], customData: BSONDocument = BSONDocument.empty, writeConcern: WriteConcern = WriteConcern.Default)
  extends Command with CommandWithResult[UnitBox.type]

case class DropUser(dropUser: String, writeConcern: WriteConcern = WriteConcern.Default)
  extends Command with CommandWithResult[UnitBox.type]

case class DropAllUsersFromDatabase(writeConcern: WriteConcern = WriteConcern.Default)
  extends Command with CommandWithResult[UnitBox.type]

case class GrantRolesToUser(grantRolesToUser: String, roles: List[Role], writeConcern: WriteConcern = WriteConcern.Default)
  extends Command with CommandWithResult[UnitBox.type]

case class RevokeRolesFromUser(revokeRolesFromUser: String, roles: List[Role], writeConcern: WriteConcern = WriteConcern.Default)
  extends Command with CommandWithResult[UnitBox.type]

// todo: unfinished
case class UsersInfo(users: List[(String, String)], showCredentials: Boolean, showPrivileges: Boolean)
  extends Command