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

  /**
   * Drops this database.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.DefaultDB
   *
   * def dropDB(db: DefaultDB)(
   *   implicit ec: ExecutionContext): Future[Unit] = db.drop()
   * }}}
   */
  def drop()(implicit ec: ExecutionContext): Future[Unit] =
    Command.run(internalSerializationPack, failoverStrategy).
      unboxed(self, DropDatabase, ReadPreference.primary)

  /**
   * Returns an index manager for this database.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.DefaultDB
   * import reactivemongo.api.indexes.NSIndex
   *
   * def listIndexes(db: DefaultDB)(
   *   implicit ec: ExecutionContext): Future[List[String]] =
   *   db.indexesManager.list().map(_.flatMap { ni: NSIndex =>
   *     ni.index.name.toList
   *   })
   * }}}
   */
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

  /**
   * Returns the names of the collections in this database.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.DefaultDB
   *
   * def listCollections(db: DefaultDB)(
   *   implicit ec: ExecutionContext): Future[List[String]] =
   *   db.collectionNames
   * }}}
   */
  def collectionNames(implicit ec: ExecutionContext): Future[List[String]] = {
    val wireVer = connectionState.metadata.maxWireVersion

    if (wireVer >= MongoWireVersion.V30) {
      Command.run(internalSerializationPack, failoverStrategy)(
        self, ListCollectionNames, ReadPreference.primary).map(_.names)

    } else {
      // TODO: Remove (MongoDB 2.6-)
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
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.DefaultDB
   *
   * def addCollSuffix(
   *   admin: DefaultDB,
   *   coll: String,
   *   suffix: String)(implicit ec: ExecutionContext): Future[Unit] =
   *   admin.renameCollection("myDB", coll, s"${coll}${suffix}").map(_ => {})
   * }}}
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
  @deprecated("Will be removed (not maintained): use `db.runCommand(BSONDocument(\"serverStatus\" -> 1)` with custom reader", "0.19.4")
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
   * [[https://docs.mongodb.com/manual/reference/command/createUser/ Create the user]] with given properties.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.{
   *   DefaultDB,
   *   ScramSha256Authentication,
   *   ScramSha1Authentication,
   *   WriteConcern
   * }
   * import reactivemongo.api.commands.UserRole
   *
   * def createReadWriteUser(db: DefaultDB, name: String)(
   *   implicit ec: ExecutionContext): Future[Unit] =
   *   db.createUser(
   *     user = name,
   *     pwd = None, // no initial password
   *     customData = None, // no custom data
   *     roles = List(UserRole("readWrite")),
   *     digestPassword = true,
   *     writeConcern = WriteConcern.Default,
   *     restrictions = List.empty,
   *     mechanisms = List(
   *       ScramSha1Authentication, ScramSha256Authentication))
   * }}}
   *
   * @tparam T the type of custom data associated with the created user
   * @param user the name of the user to be created
   * @param pwd the user password (not required if the database uses external credentials)
   * @param customData the custom data to associate with the user account
   * @param roles the roles granted to the user, possibly an empty to create users without roles
   * @param digestPassword when true, the mongod instance will create the hash of the user password (default: `true`)
   * @param writeConcern the optional level of [[https://docs.mongodb.com/manual/reference/write-concern/ write concern]]
   * @param restrictions the authentication restriction
   * @param mechanisms the authentication mechanisms (e.g. [[ScramSha1Authentication]])
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
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.DefaultDB
   *
   * def pingDB(db: DefaultDB)(
   *   implicit ec: ExecutionContext): Future[Boolean] =
   *   db.ping() // with default ReadPreference
   * }}}
   *
   * @return true if successful (even if the server is write locked)
   */
  def ping(readPreference: ReadPreference = ReadPreference.nearest)(implicit ec: ExecutionContext): Future[Boolean] = {
    Command.run(BSONSerializationPack, failoverStrategy).
      apply(self, PingCommand, readPreference)
  }
}
