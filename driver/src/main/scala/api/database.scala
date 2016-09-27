/*
 * Copyright 2012-2013 Stephane Godbillon (@sgodbillon) and Zenexity
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration

import reactivemongo.core.commands.{
  Command => CoreCommand,
  SuccessfulAuthentication
}

import reactivemongo.api.indexes.IndexesManager
import reactivemongo.bson.{
  BSONDocument,
  BSONDocumentReader,
  BSONRegex,
  BSONString
}
import reactivemongo.utils.EitherMappableFuture.futureToEitherMappable

import reactivemongo.api.collections.bson.BSONCollection

/**
 * The reference to a MongoDB database, obtained from a [[reactivemongo.api.MongoConnection]].
 *
 * You should consider the provided [[reactivemongo.api.DefaultDB]] implementation.
 *
 * {{{
 * import reactivemongo.api._
 *
 * val connection = MongoConnection(List("localhost:27017"))
 * val db = connection.database("plugin")
 * val collection = db.map(_("acoll"))
 * }}}
 *
 * @define resolveDescription Returns a [[reactivemongo.api.Collection]] from this database
 * @define nameParam the name of the collection to resolve
 */
sealed trait DB {
  /** The [[reactivemongo.api.MongoConnection]] that will be used to query this database. */
  @transient def connection: MongoConnection

  /** This database name. */
  def name: String

  /** A failover strategy for sending requests. */
  def failoverStrategy: FailoverStrategy

  /**
   * $resolveDescription (alias for the `collection` method).
   *
   * @param name $nameParam
   */
  def apply[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = collections.bson.BSONCollectionProducer): C = collection(name, failoverStrategy)

  /**
   * $resolveDescription.
   *
   * @param name $nameParam
   */
  def collection[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = collections.bson.BSONCollectionProducer): C = producer(this, name, failoverStrategy)

  @inline def defaultReadPreference: ReadPreference =
    connection.options.readPreference

  /**
   * Sends a command and get the future result of the command.
   *
   * @param command The command to send.
   * @param readPreference The ReadPreference to use for this command (defaults to [[MongoConnectionOptions.readPreference]]).
   *
   * @return a future containing the result of the command.
   */
  @deprecated("Consider using reactivemongo.api.commands along with `DefaultDB.runCommand` methods", "0.11.0")
  def command[T](command: CoreCommand[T], readPreference: ReadPreference = defaultReadPreference)(implicit ec: ExecutionContext): Future[T] =
    Failover(
      command.apply(name).maker(readPreference),
      connection, failoverStrategy
    ).future.mapEither(command.ResultMaker(_))

  /** Authenticates the connection on this database. */
  def authenticate(user: String, password: String)(implicit timeout: FiniteDuration): Future[SuccessfulAuthentication] = connection.authenticate(name, user, password)

  /**
   * Returns the database of the given name on the same MongoConnection.
   * @see [[sibling1]]
   */
  def sibling(name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit ec: ExecutionContext): DefaultDB = connection(name, failoverStrategy)

  /** Returns the database of the given name on the same MongoConnection. */
  def sibling1(name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit ec: ExecutionContext): Future[DefaultDB] = connection.database(name, failoverStrategy)

}

@deprecated(message = "Will be made sealed", since = "0.12-RC0")
trait GenericDB[P <: SerializationPack with Singleton] { self: DB =>
  val pack: P

  import reactivemongo.api.commands._

  @deprecated(message = "Either use one of the `runX` function on the DB instance, or use `reactivemongo.api.commands.Command.run` directly", since = "0.12-RC0")
  def runner = Command.run(pack)

  @deprecated(message = "Use `runCommand` with the `failoverStrategy` parameter", since = "0.12-RC0")
  def runCommand[R, C <: Command with CommandWithResult[R]](command: C with CommandWithResult[R])(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = runCommand[R, C](command, failoverStrategy)

  def runCommand[R, C <: Command with CommandWithResult[R]](command: C with CommandWithResult[R], failoverStrategy: FailoverStrategy)(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = Command.run(pack, failoverStrategy).apply(self, command)

  @deprecated(message = "Use `runCommand` with the `failoverStrategy` parameter", since = "0.12-RC0")
  def runCommand[C <: Command](command: C)(implicit writer: pack.Writer[C]): CursorFetcher[pack.type, Cursor] = runCommand[C](command, failoverStrategy)

  def runCommand[C <: Command](command: C, failoverStrategy: FailoverStrategy)(implicit writer: pack.Writer[C]): CursorFetcher[pack.type, Cursor] = Command.run(pack, failoverStrategy).apply(self, command)

  @deprecated(message = "Use `runValueCommand` with the `failoverStrategy` parameter", since = "0.12-RC0")
  def runValueCommand[A <: AnyVal, R <: BoxedAnyVal[A], C <: Command with CommandWithResult[R]](command: C with CommandWithResult[R with BoxedAnyVal[A]])(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[A] = runValueCommand[A, R, C](command, failoverStrategy)

  @deprecated("Use the alternative with `ReadPreference`", "0.12-RC5")
  def runValueCommand[A <: AnyVal, R <: BoxedAnyVal[A], C <: Command with CommandWithResult[R]](command: C with CommandWithResult[R with BoxedAnyVal[A]], failoverStrategy: FailoverStrategy)(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[A] = runValueCommand[A, R, C](command, failoverStrategy, ReadPreference.primary)

  def runValueCommand[A <: AnyVal, R <: BoxedAnyVal[A], C <: Command with CommandWithResult[R]](command: C with CommandWithResult[R with BoxedAnyVal[A]], failoverStrategy: FailoverStrategy, readPreference: ReadPreference)(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[A] = Command.run(pack, failoverStrategy).unboxed(self, command, readPreference)
}

/** A mixin that provides commands about this database itself. */
trait DBMetaCommands { self: DB =>
  import reactivemongo.core.protocol.MongoWireVersion
  import reactivemongo.api.commands.{
    Command,
    DropDatabase,
    ListCollectionNames,
    ServerStatus,
    ServerStatusResult,
    UserRole,
    WriteConcern
  }
  import reactivemongo.api.commands.bson.{
    CommonImplicits,
    BSONDropDatabaseImplicits,
    BSONServerStatusImplicits,
    BSONCreateUserCommand
  }
  import reactivemongo.api.commands.bson.BSONListCollectionNamesImplicits._
  import CommonImplicits._
  import BSONDropDatabaseImplicits._
  import BSONServerStatusImplicits._

  /** Drops this database. */
  def drop()(implicit ec: ExecutionContext): Future[Unit] =
    Command.run(BSONSerializationPack).unboxed(
      self, DropDatabase, ReadPreference.primary
    )

  /** Returns an index manager for this database. */
  def indexesManager(implicit ec: ExecutionContext) = IndexesManager(self)

  private object CollectionNameReader extends BSONDocumentReader[String] {
    val prefixLength = name.size + 1

    def read(bson: BSONDocument) = bson.get("name").collect {
      case BSONString(value) => value.substring(prefixLength)
    }.getOrElse(throw new Exception(
      "name is expected on system.namespaces query"
    ))

  }

  /** Returns the names of the collections in this database. */
  def collectionNames(implicit ec: ExecutionContext): Future[List[String]] = {
    val wireVer = connection.metadata.map(_.maxWireVersion)

    if (wireVer.exists(_ >= MongoWireVersion.V30)) {
      Command.run(BSONSerializationPack)(
        self, ListCollectionNames, ReadPreference.primary
      ).map(_.names)

    } else collection("system.namespaces").as[BSONCollection]().
      find(BSONDocument(
        "name" -> BSONRegex("^[^\\$]+$", "") // strip off any indexes
      )).cursor(defaultReadPreference)(
        CollectionNameReader, ec, CursorProducer.defaultCursorProducer
      ).collect[List]()
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/command/renameCollection/ Renames a collection]].
   * Can only be executed if the this database reference is the `admin` one.
   *
   * @param db the name of the database where the collection exists with the `current` name
   * @param current the current name of the collection, in the specified `db`
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
      ReadPreference.primary
    ).map(_ => self.collection(to))
  }

  /** Returns the server status. */
  def serverStatus(implicit ec: ExecutionContext): Future[ServerStatusResult] =
    Command.run(BSONSerializationPack)(
      self, ServerStatus, ReadPreference.primary
    )

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
    customData: Option[BSONDocument] = None
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val command = BSONCreateUserCommand.CreateUser(
      name, pwd, roles, digestPassword, Some(writeConcern), customData
    )

    Command.run(BSONSerializationPack)(
      self, command, ReadPreference.primary
    ).map(_ => {})
  }
}

/** The default DB implementation, that mixes in the database traits. */
@SerialVersionUID(235871232L)
case class DefaultDB(
    name: String,
    @transient connection: MongoConnection,
    failoverStrategy: FailoverStrategy = FailoverStrategy()
) extends DB with DBMetaCommands with GenericDB[BSONSerializationPack.type] {

  @transient val pack: BSONSerializationPack.type = BSONSerializationPack
}

object DB {
  @deprecated("Use [[MongoConnection.database]]", "0.12.0")
  def apply(name: String, connection: MongoConnection, failoverStrategy: FailoverStrategy = FailoverStrategy()) = DefaultDB(name, connection, failoverStrategy)

}
