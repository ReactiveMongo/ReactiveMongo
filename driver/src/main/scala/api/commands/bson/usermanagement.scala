package reactivemongo.api.commands.bson

import reactivemongo.api.commands._
import reactivemongo.bson._
import BSONCommonWriteCommandsImplicits._
import BSONRoleImplicits._


/**
 * Created by sh1ng on 21/06/15.
 */

 object BSONRoleImplicits {
  implicit object RolesWriter extends BSONWriter[List[Role], BSONValue]{
    /**
     * Writes an instance of `T` as a BSON value.
     *
     * This method may throw exceptions at runtime.
     * If used outside a reader, one should consider `writeTry(bson: B): Try[T]` or `writeOpt(bson: B): Option[T]`.
     */
    override def write(t: List[Role]): BSONValue = BSONArray(t.map(p=> p match {
      case RoleInTheSameDB(value) => BSONString(value)
      case RoleWithDB(value, db) => BSONDocument("role" -> value, "db" -> db)
    }))
  }
}

object BSONCreateUserImplicits {
  implicit object CreateUserWriter extends BSONDocumentWriter[CreateUser] {
    /**
     * Writes an instance of `T` as a BSON value.
     *
     * This method may throw exceptions at runtime.
     * If used outside a reader, one should consider `writeTry(bson: B): Try[T]` or `writeOpt(bson: B): Option[T]`.
     */
    override def write(t: CreateUser): BSONDocument =
      BSONDocument("createUser" -> t.createUser, "pwd" -> t.pwd, "customData" -> t.customData, "roles" -> t.roles,
        "writeConcern" -> t.writeConcern)
  }
}

object BSONUpdateUserImplicits {
  implicit object UpdateUserWriter extends BSONDocumentWriter[UpdateUser] {
    /**
     * Writes an instance of `T` as a BSON value.
     *
     * This method may throw exceptions at runtime.
     * If used outside a reader, one should consider `writeTry(bson: B): Try[T]` or `writeOpt(bson: B): Option[T]`.
     */
    override def write(t: UpdateUser): BSONDocument =
      BSONDocument("updateUser" -> t.updateUser, "pwd" -> t.pwd, "customData" -> t.customData, "roles" -> t.roles,
        "writeConcern" -> t.writeConcern)
  }
}

object BSONDropUserImplicits {
  implicit object DropUserWriter extends BSONDocumentWriter[DropUser] {
    /**
     * Writes an instance of `T` as a BSON value.
     *
     * This method may throw exceptions at runtime.
     * If used outside a reader, one should consider `writeTry(bson: B): Try[T]` or `writeOpt(bson: B): Option[T]`.
     */
    override def write(t: DropUser): BSONDocument = BSONDocument("dropUser" -> t.dropUser, "writeConcern" -> t.writeConcern)
  }
}

object BSONDropAllUsersFromDatabaseImplicits {
  implicit object DropAllUsersFromDatabaseWriter extends BSONDocumentWriter[DropAllUsersFromDatabase] {
    /**
     * Writes an instance of `T` as a BSON value.
     *
     * This method may throw exceptions at runtime.
     * If used outside a reader, one should consider `writeTry(bson: B): Try[T]` or `writeOpt(bson: B): Option[T]`.
     */
    override def write(t: DropAllUsersFromDatabase): BSONDocument = BSONDocument("dropAllUsersFromDatabase" -> 1, "writeConcern" -> t.writeConcern)
  }
}

object GrantRolesToUserImplicits {
  implicit object GrantRolesToUserWriter extends  BSONDocumentWriter[GrantRolesToUser] {
    /**
     * Writes an instance of `T` as a BSON value.
     *
     * This method may throw exceptions at runtime.
     * If used outside a reader, one should consider `writeTry(bson: B): Try[T]` or `writeOpt(bson: B): Option[T]`.
     */
    override def write(t: GrantRolesToUser): BSONDocument = BSONDocument("grantRolesToUser" -> t.grantRolesToUser, "roles" -> t.roles,
      "writeConcern" -> t.writeConcern)
  }
}

object RevokeRolesFromUserImplicits {
  implicit object RevokeRolesFromUserWriter extends BSONDocumentWriter[RevokeRolesFromUser] {
    /**
     * Writes an instance of `T` as a BSON value.
     *
     * This method may throw exceptions at runtime.
     * If used outside a reader, one should consider `writeTry(bson: B): Try[T]` or `writeOpt(bson: B): Option[T]`.
     */
    override def write(t: RevokeRolesFromUser): BSONDocument = BSONDocument("revokeRolesFromUser" -> t.revokeRolesFromUser, "roles" -> t.roles,
      "writeConcern" -> t.writeConcern)
  }
}
