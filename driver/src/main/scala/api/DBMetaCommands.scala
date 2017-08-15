package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.api.indexes.IndexesManager
import reactivemongo.bson.{
  BSONDocument,
  BSONDocumentReader,
  BSONRegex,
  BSONString
}

import reactivemongo.api.collections.bson.BSONCollection

/** A mixin that provides commands about this database itself. */
trait DBMetaCommands { self: DB =>
  import reactivemongo.core.protocol.MongoWireVersion
  import reactivemongo.api.commands.{
    Command,
    DropDatabase,
    ListCollectionNames,
    PingCommand,
    ServerStatus,
    ServerStatusResult,
    UserRole,
    WriteConcern
  }
  import reactivemongo.api.commands.bson.{
    CommonImplicits,
    BSONDropDatabaseImplicits,
    BSONServerStatusImplicits,
    BSONCreateUserCommand,
    BSONPingCommandImplicits
  }
  import reactivemongo.api.commands.bson.BSONListCollectionNamesImplicits._
  import CommonImplicits._
  import BSONDropDatabaseImplicits._
  import BSONServerStatusImplicits._
  import BSONPingCommandImplicits._

  /** Drops this database. */
  def drop()(implicit ec: ExecutionContext): Future[Unit] =
    Command.run(BSONSerializationPack).unboxed(
      self, DropDatabase, ReadPreference.primary)

  /** Returns an index manager for this database. */
  def indexesManager(implicit ec: ExecutionContext) = IndexesManager(self)

  private object CollectionNameReader extends BSONDocumentReader[String] {
    val prefixLength = name.size + 1

    def read(bson: BSONDocument) = bson.get("name").collect {
      case BSONString(value) => value.substring(prefixLength)
    }.getOrElse(throw new Exception(
      "name is expected on system.namespaces query"))

  }

  /** Returns the names of the collections in this database. */
  def collectionNames(implicit ec: ExecutionContext): Future[List[String]] = {
    val wireVer = connection.metadata.map(_.maxWireVersion)

    if (wireVer.exists(_ >= MongoWireVersion.V30)) {
      Command.run(BSONSerializationPack)(
        self, ListCollectionNames, ReadPreference.primary).map(_.names)

    } else collection("system.namespaces").as[BSONCollection]().
      find(BSONDocument(
        "name" -> BSONRegex("^[^\\$]+$", "") // strip off any indexes
      )).cursor(defaultReadPreference)(
        CollectionNameReader, ec, CursorProducer.defaultCursorProducer).collect[List]()
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/command/renameCollection/ Renames a collection]].
   * Can only be executed if the this database reference is the `admin` one.
   *
   * @param db the name of the database where the collection exists with the `current` name
   * @param from the current name of the collection, in the specified `db`
   * @param to the new name of this collection (inside the same `db`)
   * @param dropExisting If a collection of name `to` already exists, then drops that collection before renaming this one.
   *
   * @return a failure if the dropExisting option is false and the target collection already exists
   */
  def renameCollection[C <: Collection](db: String, from: String, to: String, dropExisting: Boolean = false, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit ec: ExecutionContext, producer: CollectionProducer[C] = collections.bson.BSONCollectionProducer): Future[C] = {
    import reactivemongo.api.commands.RenameCollection
    import reactivemongo.api.commands.bson.BSONRenameCollectionImplicits._

    Command.run(BSONSerializationPack, failoverStrategy).unboxed(
      self, RenameCollection(s"${db}.$from", s"${db}.$to", dropExisting),
      ReadPreference.primary).map(_ => self.collection(to))
  }

  /** Returns the server status. */
  def serverStatus(implicit ec: ExecutionContext): Future[ServerStatusResult] =
    Command.run(BSONSerializationPack)(
      self, ServerStatus, ReadPreference.primary)

  /**
   * Create the specified user.
   *
   * @param name the name of the user to be created
   * @param pwd the user password (not required if the database uses external credentials)
   * @param roles the roles granted to the user, possibly an empty to create users without roles
   * @param digestPassword when true, the mongod instance will create the hash of the user password (default: `true`)
   * @param writeConcern the optional level of [[https://docs.mongodb.com/manual/reference/write-concern/ write concern]]
   * @param customData the custom data to associate with the user account
   *
   * @see https://docs.mongodb.com/manual/reference/command/createUser/
   */
  def createUser(
    name: String,
    pwd: Option[String],
    roles: List[UserRole],
    digestPassword: Boolean = true,
    writeConcern: WriteConcern = connection.options.writeConcern,
    customData: Option[BSONDocument] = None)(implicit ec: ExecutionContext): Future[Unit] = {
    val command = BSONCreateUserCommand.CreateUser(
      name, pwd, roles, digestPassword, Some(writeConcern), customData)

    Command.run(BSONSerializationPack)(
      self, command, ReadPreference.primary).map(_ => {})
  }

  /**
   * Tests if the server, resolved according to the given read preference, responds to commands.
   * (since MongoDB 3.0)
   *
   * @return true if successful (even if the server is write locked)
   */
  def ping(readPreference: ReadPreference = ReadPreference.nearest)(implicit ec: ExecutionContext): Future[Boolean] = {
    Command.run(BSONSerializationPack, failoverStrategy)
      .apply(self, PingCommand, readPreference)
  }
}
