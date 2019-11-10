package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.api.commands.{
  AuthenticationRestriction,
  RenameCollection
}

import reactivemongo.api.indexes.IndexesManager
import reactivemongo.bson.{ BSONDocument => LegacyDoc, BSONDocumentWriter }

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
    BSONCreateUserCommand,
    BSONPingCommandImplicits
  }
  import CommonImplicits._
  import BSONPingCommandImplicits._
  import Serialization.{ Pack, internalSerializationPack, unitBoxReader }

  private implicit lazy val dropWriter =
    DropDatabase.writer(internalSerializationPack)

  /** Drops this database. */
  def drop()(implicit ec: ExecutionContext): Future[Unit] =
    Command.run(internalSerializationPack, failoverStrategy).
      unboxed(self, DropDatabase, ReadPreference.primary)

  /** Returns an index manager for this database. */
  def indexesManager(implicit ec: ExecutionContext) = IndexesManager(self)

  private lazy val collectionNameReader = {
    val prefixLength = name.size + 1
    val dec = internalSerializationPack.newDecoder

    internalSerializationPack.reader { doc =>
      dec.string(doc, "name").collect {
        case value => value.substring(prefixLength)
      }.getOrElse(throw new Exception(
        "name is expected on system.namespaces query"))
    }
  }

  private implicit lazy val colNamesWriter =
    ListCollectionNames.writer(internalSerializationPack)

  private implicit lazy val colNamesReader =
    ListCollectionNames.reader(internalSerializationPack)

  /** Returns the names of the collections in this database. */
  def collectionNames(implicit ec: ExecutionContext): Future[List[String]] = {
    val wireVer = connectionState.metadata.maxWireVersion

    if (wireVer >= MongoWireVersion.V30) {
      Command.run(internalSerializationPack, failoverStrategy)(
        self, ListCollectionNames, ReadPreference.primary).map(_.names)

    } else {
      implicit def producer = Serialization.defaultCollectionProducer

      val coll = producer(self, "system.namespaces", self.failoverStrategy)
      import coll.{ pack => p }
      val builder = p.newBuilder

      val selector = builder.document(Seq(
        builder.elementProducer("name", builder.regex("^[^\\$]+$", ""))))

      coll.find(
        // strip off any indexes
        selector = selector,
        projection = Option.empty[p.Document]).cursor(defaultReadPreference)(
        collectionNameReader, CursorProducer.defaultCursorProducer).
        collect[List](-1, Cursor.FailOnError[List[String]]())
    }
  }

  private lazy implicit val renameWriter =
    RenameCollection.writer(internalSerializationPack)

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
  def renameCollection[C <: Collection](db: String, from: String, to: String, dropExisting: Boolean = false, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit ec: ExecutionContext, producer: CollectionProducer[C] = Serialization.defaultCollectionProducer): Future[C] = {
    Command.run(internalSerializationPack, failoverStrategy).unboxed(
      self, RenameCollection(s"${db}.$from", s"${db}.$to", dropExisting),
      ReadPreference.primary).map(_ => self.collection(to))
  }

  private implicit lazy val serverStatusWriter =
    ServerStatus.writer(internalSerializationPack)

  private implicit lazy val serverStatusReader =
    ServerStatus.reader(internalSerializationPack)

  /** Returns the server status. */
  def serverStatus(implicit ec: ExecutionContext): Future[ServerStatusResult] =
    Command.run(internalSerializationPack, failoverStrategy)(
      self, ServerStatus, ReadPreference.primary)

  @deprecated("Use `createUser` with complete authentication options", "0.18.4")
  def createUser(
    @deprecatedName(Symbol("name")) user: String,
    pwd: Option[String],
    roles: List[UserRole],
    digestPassword: Boolean = true,
    writeConcern: WC = connection.options.writeConcern,
    customData: Option[LegacyDoc] = None)(implicit ec: ExecutionContext): Future[Unit] = createUser(user, pwd, customData, roles, digestPassword, writeConcern, List.empty, List.empty)

  private lazy val createUserWriter: BSONDocumentWriter[CreateUserCommand[BSONSerializationPack.type]#CreateUser] = CreateUserCommand.writer(BSONSerializationPack)

  @deprecated("Use `createUser` with `DBMetaWriter`", "0.19.1")
  def createUser(
    user: String,
    pwd: Option[String],
    customData: Option[LegacyDoc],
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

    implicit def writer = createUserWriter

    Command.run(BSONSerializationPack, failoverStrategy)(
      self, command, ReadPreference.primary).map(_ => {})
  }

  /** Type of writer to serialization database metadata */
  type DBMetaWriter[T] = Pack#Writer[T]

  private object InternalCreateUser extends CreateUserCommand[Pack] {
    val pack: Pack = internalSerializationPack

    implicit val writer =
      CreateUserCommand.writer[Pack](InternalCreateUser.pack: Pack)
  }

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
  def createUser[T](
    user: String,
    pwd: Option[String],
    customData: Option[T],
    roles: List[UserRole],
    digestPassword: Boolean,
    writeConcern: WC,
    restrictions: List[AuthenticationRestriction],
    mechanisms: List[AuthenticationMode])(implicit ec: ExecutionContext, w: DBMetaWriter[T]): Future[Unit] = {
    val command: CreateUserCommand[Pack]#CreateUser =
      new InternalCreateUser.CreateUser(
        name = user,
        pwd = pwd,
        customData = customData.flatMap(w.writeOpt),
        roles = roles,
        digestPassword = digestPassword,
        writeConcern = Some(writeConcern),
        authenticationRestrictions = restrictions,
        mechanisms = mechanisms)

    import InternalCreateUser.writer

    Command.run(InternalCreateUser.pack, failoverStrategy)(
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
