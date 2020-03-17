package reactivemongo.api.commands.bson

import reactivemongo.bson.{
  BSONBooleanLike,
  BSONDocument,
  BSONDocumentWriter,
  BSONString,
  BSONValue,
  BSONWriter
}

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands._

import reactivemongo.api.BSONSerializationPack

private[reactivemongo] object BSONCreateUserCommand
  extends CreateUserCommand[BSONSerializationPack.type] {

  import BSONCommonWriteCommandsImplicits.WriteConcernWriter

  val pack = BSONSerializationPack

  implicit object UserRoleWriter extends BSONWriter[UserRole, BSONValue] {
    def write(role: UserRole): BSONValue = role match {
      case DBUserRole(name, dbn) => BSONDocument("role" -> name, "db" -> dbn)
      case _                     => BSONString(role.name)
    }
  }

  object CreateUserWriter extends BSONDocumentWriter[CreateUser] {
    def write(create: CreateUser) = BSONDocument(
      "createUser" -> create.name,
      "pwd" -> create.pwd,
      "customData" -> create.customData,
      "roles" -> create.roles,
      "digestPassword" -> create.digestPassword,
      "writeConcern" -> create.writeConcern)
  }
}

private[reactivemongo] object BSONPingCommandImplicits {
  implicit object PingWriter extends BSONDocumentWriter[PingCommand.type] {
    val command = BSONDocument("ping" -> 1.0)
    def write(ping: PingCommand.type): BSONDocument = command
  }

  implicit object PingReader extends DealingWithGenericCommandErrorsReader[Boolean] {
    def readResult(bson: BSONDocument): Boolean =
      bson.getAs[BSONBooleanLike]("ok").exists(_.toBoolean)
  }
}
