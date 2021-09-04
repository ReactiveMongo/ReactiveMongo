package reactivemongo.api.commands

private[reactivemongo] final class CommandKind private (
  val name: String) extends AnyVal {

  override def toString = name
}

private[reactivemongo] object CommandKind {
  val Undefined = new CommandKind("_undefined_")

  val Hello = new CommandKind("Hello") // isMaster

  val Aggregate = new CommandKind("aggregate")

  val Update = new CommandKind("update")

  val Insert = new CommandKind("insert")

  val Delete = new CommandKind("delete")

  val FindAndModify = new CommandKind("findAndModify")

  val Distinct = new CommandKind("distinct")

  val Count = new CommandKind("count")

  val GetNonce = new CommandKind("getNonce")

  val Authenticate = new CommandKind("authenticate")

  val DropDatabase = new CommandKind("dropDatabase")

  val DropCollection = new CommandKind("dropCollection")

  val DropIndexes = new CommandKind("dropIndexes")

  val RenameCollection = new CommandKind("renameCollection")

  val Create = new CommandKind("create")

  val CreateUser = new CommandKind("createUser")

  val CreateView = new CommandKind("createView")

  val CreateIndexes = new CommandKind("createIndexes")

  val ConvertToCapped = new CommandKind("convertToCapped")

  val EndTransaction = new CommandKind("endTransaction")

  val StartSession = new CommandKind("startSession")

  val EndSession = new CommandKind("endSession")

  val CollStats = new CommandKind("collStats")

  val ListCollections = new CommandKind("listCollections")

  val ListIndexes = new CommandKind("listIndexes")

  val SaslStart = new CommandKind("saslStart")

  val SaslContinue = new CommandKind("saslContinue")

  val ReplSetGetStatus = new CommandKind("replSetGetStatus")

  val Resync = new CommandKind("resync")

  val ReplSetMaintenance = new CommandKind("replSetMaintenance")

  val KillCursors = new CommandKind("killCursors")

  val Query = new CommandKind("query")

  val Ping = new CommandKind("ping")

  def canCompress(kind: CommandKind): Boolean = kind match {
    case Hello | SaslStart | SaslContinue | GetNonce | Authenticate |
      CreateUser | Undefined => false

    case _ => true
  }
}
