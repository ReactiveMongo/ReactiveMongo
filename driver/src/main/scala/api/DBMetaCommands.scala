package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.api.commands.{
  AuthenticationRestriction,
  CommandCodecs,
  RenameCollection
}

import reactivemongo.api.indexes.IndexesManager

import reactivemongo.api.gridfs.GridFS

/** A mixin that provides commands about this database itself. */
private[api] trait DBMetaCommands { self: DB =>
  import reactivemongo.api.commands.{
    Command,
    CreateUserCommand,
    DropDatabase,
    ListCollectionNames,
    PingCommand,
    UserRole
  }
  import Serialization.{ Pack, internalSerializationPack, unitBoxReader }

  private implicit lazy val dropWriter =
    DropDatabase.writer(internalSerializationPack)

  /**
   * Drops this database.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.DB
   *
   * def dropDB(db: DB)(
   *   implicit ec: ExecutionContext): Future[Unit] = db.drop()
   * }}}
   */
  def drop()(implicit ec: ExecutionContext): Future[Unit] =
    Command.run(internalSerializationPack, failoverStrategy).
      unboxed(self, DropDatabase, ReadPreference.primary)

  /**
   * The GridFS with the default serialization and collection prefix.
   *
   * {{{
   * import scala.reflect.ClassTag
   *
   * import reactivemongo.api.DB
   * import reactivemongo.api.bson.{ BSONDocument, BSONValue }
   *
   * def findFile(db: DB, query: BSONDocument)(
   *   implicit it: ClassTag[BSONValue]) =
   *   db.gridfs.find(query)
   * }}}
   */
  @inline def gridfs: GridFS[Serialization.Pack] = gridfs("fs")

  /**
   * The GridFS with the default serialization.
   *
   * @param prefix the collection prefix
   */
  @inline def gridfs(prefix: String): GridFS[Serialization.Pack] =
    gridfs[Serialization.Pack](
      Serialization.internalSerializationPack, prefix)

  /**
   * The GridFS with the default serialization.
   *
   * @tparam P the type of serialization
   * @param pack the serialization pack
   * @param prefix the collection prefix
   */
  def gridfs[P <: SerializationPack with Singleton](
    pack: P, prefix: String): GridFS[P] =
    GridFS[P](pack, this, prefix)

  /**
   * Returns an index manager for this database.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.DB
   * import reactivemongo.api.indexes.NSIndex
   *
   * def listIndexes(db: DB)(
   *   implicit ec: ExecutionContext): Future[List[String]] =
   *   db.indexesManager.list().map(_.flatMap { ni: NSIndex =>
   *     ni.index.name.toList
   *   })
   * }}}
   */
  def indexesManager(implicit ec: ExecutionContext) = IndexesManager(self)

  private[api] def indexesManager[P <: SerializationPack with Singleton](pack: P)(implicit ec: ExecutionContext): IndexesManager.Aux[P] = IndexesManager[P](pack, self)

  private implicit lazy val colNamesWriter =
    ListCollectionNames.writer(internalSerializationPack)

  private implicit lazy val colNamesReader =
    ListCollectionNames.reader(internalSerializationPack)

  /**
   * Returns the names of the collections in this database.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.DB
   *
   * def listCollections(db: DB)(
   *   implicit ec: ExecutionContext): Future[List[String]] =
   *   db.collectionNames
   * }}}
   */
  def collectionNames(implicit ec: ExecutionContext): Future[List[String]] =
    Command.run(internalSerializationPack, failoverStrategy)(
      self, ListCollectionNames, ReadPreference.primary).map(_.names)

  private lazy implicit val renameWriter =
    RenameCollection.writer(internalSerializationPack)

  /**
   * [[https://docs.mongodb.com/manual/reference/command/renameCollection/ Renames a collection]].
   * Can only be executed if the this database reference is the `admin` one.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.DB
   *
   * def addCollSuffix(
   *   admin: DB,
   *   coll: String,
   *   suffix: String)(implicit ec: ExecutionContext): Future[Unit] =
   *   admin.renameCollection("myDB", coll, coll + suffix).map(_ => {})
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

  /** Type of writer to serialization database metadata */
  final type DBMetaWriter[T] = Pack#Writer[T]

  private object InternalCreateUser extends CreateUserCommand[Pack] {
    val pack: Pack = internalSerializationPack

    implicit lazy val writer =
      CreateUserCommand.writer[Pack](InternalCreateUser.pack: Pack)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/command/createUser/ Create the user]] with given properties.
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   *
   * import reactivemongo.api.{
   *   DB,
   *   ScramSha256Authentication,
   *   ScramSha1Authentication,
   *   WriteConcern
   * }
   * import reactivemongo.api.commands.UserRole
   *
   * def createReadWriteUser(db: DB, name: String)(
   *   implicit ec: ExecutionContext): Future[Unit] =
   *   db.createUser(
   *     user = name,
   *     pwd = None, // no initial password
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
    customData: Option[T] = Option.empty[Pack#Document],
    roles: List[UserRole] = List.empty,
    digestPassword: Boolean = true,
    writeConcern: WriteConcern = connection.options.writeConcern,
    restrictions: List[AuthenticationRestriction] = List.empty,
    mechanisms: List[AuthenticationMode] = List.empty)(implicit ec: ExecutionContext, w: DBMetaWriter[T]): Future[Unit] = {
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

  private implicit lazy val pingWriter: pack.Writer[PingCommand.type] = {
    val builder = internalSerializationPack.newBuilder
    val cmd = builder.document(Seq(builder.elementProducer(
      "ping", builder.double(1.0D))))

    pack.writer[PingCommand.type] { _ => cmd }
  }

  private implicit lazy val pingReader: pack.Reader[Boolean] =
    CommandCodecs.dealingWithGenericCommandErrorsReader[Pack, Boolean](internalSerializationPack) { _ => true }

  /**
   * Tests if the server, resolved according to the given read preference, responds to commands.
   *
   * @since MongoDB 3.0
   *
   * {{{
   * import scala.concurrent.{ ExecutionContext, Future }
   * import reactivemongo.api.DB
   *
   * def pingDB(db: DB)(
   *   implicit ec: ExecutionContext): Future[Boolean] =
   *   db.ping() // with default ReadPreference
   * }}}
   *
   * @return true if successful (even if the server is write locked)
   */
  def ping(readPreference: ReadPreference = ReadPreference.nearest)(implicit ec: ExecutionContext): Future[Boolean] = {
    Command.run(internalSerializationPack, failoverStrategy).
      apply(self, PingCommand, readPreference)
  }
}
