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

import collections.bson.BSONCollection
import reactivemongo.api.indexes.IndexesManager
import reactivemongo.bson._
import reactivemongo.core.commands.{ Update => UpdateCommand, _ }
import reactivemongo.utils.EitherMappableFuture._

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

/**
 * A Mongo Database.
 *
 * You should consider the provided [[reactivemongo.api.DefaultDB]] implementation.
 *
 * Example:
 * {{{
 * import reactivemongo.api._
 *
 * val connection = MongoConnection( List( "localhost:27016" ) )
 * val db = connection.database("plugin")
 * val collection = db("acoll")
 * }}}
 */
sealed trait DB {
  /** The [[reactivemongo.api.MongoConnection]] that will be used to query this database. */
  def connection: MongoConnection

  /** This database name. */
  def name: String

  /** A failover strategy for sending requests. */
  def failoverStrategy: FailoverStrategy

  /**
   * Returns a [[reactivemongo.api.Collection]] from this database
   * (alias for the `collection` method).
   *
   * @param name the name of the collection to open
   */
  def apply[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = collections.bson.BSONCollectionProducer): C = collection(name, failoverStrategy)

  /**
   * Returns a [[reactivemongo.api.Collection]] from this database.
   *
   * @param name the name of the collection to open
   */
  def collection[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = collections.bson.BSONCollectionProducer): C = producer(this, name, failoverStrategy)

  /**
   * Returns a [[reactivemongo.api.Collection]] from this database, when ready.
   *
   * @param name the name of the collection to open
   */
  def coll[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit ec: ExecutionContext, producer: CollectionProducer[C] = collections.bson.BSONCollectionProducer): Future[C] = connection.waitIsAvailable(failoverStrategy).map(_ => producer(this, name, failoverStrategy))

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
  @deprecated("Consider using reactivemongo.api.commands along with `GenericDB.runCommand` methods", "0.11.0")
  def command[T](command: Command[T], readPreference: ReadPreference = defaultReadPreference)(implicit ec: ExecutionContext): Future[T] =
    Failover(command.apply(name).maker(readPreference),
      connection, failoverStrategy).future.mapEither(command.ResultMaker(_))

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

trait GenericDB[P <: SerializationPack with Singleton] { self: DB =>
  val pack: P

  import reactivemongo.api.commands._

  def runner = Command.run(pack)

  def runCommand[R, C <: Command with CommandWithResult[R]](command: C with CommandWithResult[R])(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = runner(self, command)

  def runCommand[C <: Command](command: C)(implicit writer: pack.Writer[C]): CursorFetcher[pack.type, Cursor] = runner(self, command)

  def runValueCommand[A <: AnyVal, R <: BoxedAnyVal[A], C <: Command with CommandWithResult[R]](command: C with CommandWithResult[R with BoxedAnyVal[A]])(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[A] = runner.unboxed(self, command)
}

/** A mixin that provides commands about this database itself. */
trait DBMetaCommands { self: DB =>
  import reactivemongo.core.protocol.MongoWireVersion
  import reactivemongo.api.commands.{
    Command,
    DropDatabase,
    ListCollectionNames,
    ServerStatus,
    ServerStatusResult
  }
  import reactivemongo.api.commands.bson.{
    CommonImplicits,
    BSONDropDatabaseImplicits,
    BSONServerStatusImplicits
  }
  import reactivemongo.api.commands.bson.BSONListCollectionNamesImplicits._
  import CommonImplicits._
  import BSONDropDatabaseImplicits._
  import BSONServerStatusImplicits._

  /** Drops this database. */
  def drop()(implicit ec: ExecutionContext): Future[Unit] =
    Command.run(BSONSerializationPack).unboxed(self, DropDatabase)

  /** Returns an index manager for this database. */
  def indexesManager(implicit ec: ExecutionContext) = IndexesManager(self)

  private lazy val collectionNameReader = new BSONDocumentReader[String] {
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
      Command.run(BSONSerializationPack)(self, ListCollectionNames).map(_.names)
    } else collection("system.namespaces").as[BSONCollection]().
      find(BSONDocument(
        "name" -> BSONRegex("^[^\\$]+$", "") // strip off any indexes
        )).cursor(defaultReadPreference)(
        collectionNameReader, ec, CursorProducer.defaultCursorProducer).
        collect[List]()
  }

  /** Returns the server status. */
  def serverStatus(implicit ec: ExecutionContext): Future[ServerStatusResult] =
    Command.run(BSONSerializationPack)(self, ServerStatus)
}

/** The default DB implementation, that mixes in the database traits. */
case class DefaultDB(
    name: String,
    connection: MongoConnection,
    failoverStrategy: FailoverStrategy = FailoverStrategy()) extends DB with DBMetaCommands with GenericDB[BSONSerializationPack.type] {

  val pack: BSONSerializationPack.type = BSONSerializationPack
}

object DB {
  @deprecated("Use [[MongoConnection.database]]", "0.12.0")
  def apply(name: String, connection: MongoConnection, failoverStrategy: FailoverStrategy = FailoverStrategy()) = DefaultDB(name, connection, failoverStrategy)

}
