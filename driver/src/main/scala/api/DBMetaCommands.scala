package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.api.commands.{
  AuthenticationRestriction,
  CommandCodecs,
  RenameCollection
}

import reactivemongo.api.indexes.IndexesManager
import reactivemongo.bson.{
  BSONDocument,
  BSONDocumentReader,
  BSONDocumentWriter,
  BSONRegex,
  BSONString
}

import reactivemongo.api.collections.bson.{
  BSONCollection,
  BSONCollectionProducer
}

/** A mixin that provides commands about this database itself. */
trait DBMetaCommands { self: DB =>
  import reactivemongo.core.protocol.MongoWireVersion
  import reactivemongo.api.commands.{
    Command,
    CreateUserCommand,
    DropDatabase,
    DBHash,
    DBHashResult,
    ListCollectionNames,
    PingCommand,
    ServerStatus,
    ServerStatusResult,
    UserRole,
    WriteConcern => WC
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
    Command.run(BSONSerializationPack, failoverStrategy).
      unboxed(self, DropDatabase, ReadPreference.primary)

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
    val wireVer = connectionState.metadata.maxWireVersion

    if (wireVer >= MongoWireVersion.V30) {
      Command.run(BSONSerializationPack, failoverStrategy)(
        self, ListCollectionNames, ReadPreference.primary).map(_.names)

    } else {
      implicit def producer = BSONCollectionProducer

      val coll: BSONCollection =
        producer.apply(self, "system.namespaces", self.failoverStrategy)

      coll.find(
        // strip off any indexes
        selector = BSONDocument("name" -> BSONRegex("^[^\\$]+$", "")),
        projection = Option.empty[BSONDocument]).cursor(defaultReadPreference)(
          CollectionNameReader, CursorProducer.defaultCursorProducer).
        collect[List](-1, Cursor.FailOnError[List[String]]())
    }
  }

  private lazy implicit val unitBoxReader =
    CommandCodecs.unitBoxReader(Compat.internalSerializationPack)

  private lazy implicit val renameWriter =
    RenameCollection.writer(Compat.internalSerializationPack)

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
  def renameCollection[C <: Collection](db: String, from: String, to: String, dropExisting: Boolean = false, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit ec: ExecutionContext, producer: CollectionProducer[C] = Compat.defaultCollectionProducer): Future[C] = {
    Command.run(Compat.internalSerializationPack, failoverStrategy).unboxed(
      self, RenameCollection(s"${db}.$from", s"${db}.$to", dropExisting),
      ReadPreference.primary).map(_ => self.collection(to))
  }

  /** Returns the server status. */
  def serverStatus(implicit ec: ExecutionContext): Future[ServerStatusResult] =
    Command.run(BSONSerializationPack, failoverStrategy)(
      self, ServerStatus, ReadPreference.primary)

  @deprecated("Use `createUser` with complete authentication options", "0.18.4")
  def createUser(
    @deprecatedName('name) user: String,
    pwd: Option[String],
    roles: List[UserRole],
    digestPassword: Boolean = true,
    writeConcern: WC = connection.options.writeConcern,
    customData: Option[BSONDocument] = None)(implicit ec: ExecutionContext): Future[Unit] = createUser(user, pwd, customData, roles, digestPassword, writeConcern, List.empty, List.empty)

  implicit private val createUserWriter: BSONDocumentWriter[CreateUserCommand[BSONSerializationPack.type]#CreateUser] = CreateUserCommand.writer(BSONSerializationPack)

  /**
   * Create the specified user.
   *
   * @param user the name of the user to be created
   * @param pwd the user password (not required if the database uses external credentials)
   * @param customData the custom data to associate with the user account
   * @param roles the roles granted to the user, possibly an empty to create users without roles
   * @param digestPassword when true, the mongod instance will create the hash of the user password (default: `true`)
   * @param writeConcern the optional level of [[https://docs.mongodb.com/manual/reference/write-concern/ write concern]]
   * @param restrictions the authentication restriction
   * @param mechanisms the authentication mechanisms (e.g. SCRAM-SHA-1)
   *
   * @see https://docs.mongodb.com/manual/reference/command/createUser/
   */
  def createUser(
    user: String,
    pwd: Option[String],
    customData: Option[BSONDocument],
    roles: List[UserRole],
    digestPassword: Boolean,
    writeConcern: WC,
    restrictions: List[AuthenticationRestriction],
    mechanisms: List[AuthenticationMode])(implicit ec: ExecutionContext): Future[Unit] = {
    val command: CreateUserCommand[BSONSerializationPack.type]#CreateUser =
      new BSONCreateUserCommand.CreateUser(
        name = user,
        pwd = pwd,
        customData = customData,
        roles = roles,
        digestPassword = digestPassword,
        writeConcern = Some(writeConcern),
        authenticationRestrictions = restrictions,
        mechanisms = mechanisms)

    Command.run(BSONSerializationPack, failoverStrategy)(
      self, command, ReadPreference.primary).map(_ => {})
  }

  /**
   * Tests if the server, resolved according to the given read preference, responds to commands.
   * (since MongoDB 3.0)
   *
   * @return true if successful (even if the server is write locked)
   */
  def ping(readPreference: ReadPreference = ReadPreference.nearest)(implicit ec: ExecutionContext): Future[Boolean] = {
    Command.run(BSONSerializationPack, failoverStrategy).
      apply(self, PingCommand, readPreference)
  }

  // TODO: Public once covered with test
  // See: https://docs.mongodb.com/manual/reference/command/dbHash/
  private[reactivemongo] def hash(collections: Seq[String], readPreference: ReadPreference = defaultReadPreference)(implicit ec: ExecutionContext): Future[DBHashResult] = {
    implicit def w: BSONSerializationPack.Writer[DBHash] =
      DBHash.commandWriter(BSONSerializationPack)

    implicit def r: BSONSerializationPack.Reader[DBHashResult] =
      DBHashResult.reader(BSONSerializationPack)

    Command.run(BSONSerializationPack, failoverStrategy).
      apply(self, new DBHash(collections), readPreference)
  }
}
