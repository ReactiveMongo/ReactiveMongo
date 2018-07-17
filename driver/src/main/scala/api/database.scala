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

import reactivemongo.core.commands.SuccessfulAuthentication

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

  /** Authenticates the connection on this database. */
  def authenticate(user: String, password: String)(implicit timeout: FiniteDuration): Future[SuccessfulAuthentication] = connection.authenticate(name, user, password)

  /**
   * Returns the database of the given name on the same MongoConnection.
   *
   * @param name $nameParam
   */
  def sibling(name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit ec: ExecutionContext): Future[DefaultDB] = connection.database(name, failoverStrategy)

}

sealed trait GenericDB[P <: SerializationPack with Singleton] { self: DB =>
  val pack: P

  import reactivemongo.api.commands._

  def runCommand[R, C <: Command with CommandWithResult[R]](command: C with CommandWithResult[R], failoverStrategy: FailoverStrategy)(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = Command.run(pack, failoverStrategy).apply(self, command, self.defaultReadPreference)

  def runCommand[C <: Command](command: C, failoverStrategy: FailoverStrategy)(implicit writer: pack.Writer[C]): CursorFetcher[pack.type, Cursor] = Command.run(pack, failoverStrategy).apply(self, command)

  def runValueCommand[A <: AnyVal, R <: BoxedAnyVal[A], C <: Command with CommandWithResult[R]](command: C with CommandWithResult[R with BoxedAnyVal[A]], failoverStrategy: FailoverStrategy, readPreference: ReadPreference)(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[A] = Command.run(pack, failoverStrategy).unboxed(self, command, readPreference)
}

/** The default DB implementation, that mixes in the database traits. */
@SerialVersionUID(235871232L)
case class DefaultDB(
  name: String,
  @transient connection: MongoConnection,
  failoverStrategy: FailoverStrategy = FailoverStrategy()) extends DB with DBMetaCommands with GenericDB[BSONSerializationPack.type] {

  @transient val pack: BSONSerializationPack.type = BSONSerializationPack
}

object DB {
  @deprecated("Use [[MongoConnection.database]]", "0.12.0")
  def apply(name: String, connection: MongoConnection, failoverStrategy: FailoverStrategy = FailoverStrategy()) = DefaultDB(name, connection, failoverStrategy)

}
