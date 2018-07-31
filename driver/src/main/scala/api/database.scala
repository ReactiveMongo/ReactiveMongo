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

import java.util.UUID

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration

import reactivemongo.core.commands.SuccessfulAuthentication

import reactivemongo.api.commands.{
  Command,
  CommandCodecs,
  EndSessions,
  KillSessions,
  StartSession,
  StartSessionResult,
  UnitBox
}

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
  protected type DBType <: DB

  /** The [[reactivemongo.api.MongoConnection]] that will be used to query this database. */
  @transient def connection: MongoConnection

  /** The state of the associated [[connection]] */
  private[api] def connectionState: ConnectionState

  /** This database name. */
  def name: String

  /** A failover strategy for sending requests. */
  def failoverStrategy: FailoverStrategy

  private[api] def session: Option[Session]

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
   * @see [[sibling1]]
   */
  @deprecated("Use [[sibling1]]", "0.16.0")
  def sibling(name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit ec: ExecutionContext): DefaultDB = Await.result(connection.database(name, failoverStrategy), FiniteDuration(15, "seconds"))

  /**
   * Returns the database of the given name on the same MongoConnection.
   *
   * @param name $nameParam
   */
  def sibling1(name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit ec: ExecutionContext): Future[DefaultDB] = connection.database(name, failoverStrategy)

  /**
   * Starts a [[https://docs.mongodb.com/manual/reference/command/startSession/ new session]], do nothing if a session has already being started (since MongoDB 3.6)
   */
  def startSession()(implicit ec: ExecutionContext): Future[DBType]

  /**
   * @param session the result of the startSession command
   */
  private[api] def withNewSession(result: StartSessionResult): DBType

  /**
   * Ends the session associated with this database reference.
   *
   * @return UUID for the ended session, if there is some
   * @see [[https://docs.mongodb.com/manual/reference/command/endSessions Command endSessions]]
   */
  def endSession()(implicit ec: ExecutionContext): Future[Option[UUID]]

  /**
   * Ends the session associated with this database reference.
   *
   * @return UUID for the ended session, if there is some
   * @see [[https://docs.mongodb.com/manual/reference/command/killSessions Command killSessions]]
   */
  def killSession()(implicit ec: ExecutionContext): Future[Option[UUID]]
}

private[api] sealed trait GenericDB[P <: SerializationPack with Singleton] { self: DB =>
  val pack: P

  import reactivemongo.api.commands._

  def runCommand[R, C <: Command with CommandWithResult[R]](command: C with CommandWithResult[R], failoverStrategy: FailoverStrategy)(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = Command.run(pack, failoverStrategy).apply(self, command, self.defaultReadPreference)

  def runCommand[C <: Command](command: C, failoverStrategy: FailoverStrategy)(implicit writer: pack.Writer[C]): CursorFetcher[pack.type, Cursor] = Command.run(pack, failoverStrategy).apply(self, command)

  def runValueCommand[A <: AnyVal, R <: BoxedAnyVal[A], C <: Command with CommandWithResult[R]](command: C with CommandWithResult[R with BoxedAnyVal[A]], failoverStrategy: FailoverStrategy, readPreference: ReadPreference)(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[A] = Command.run(pack, failoverStrategy).unboxed(self, command, readPreference)
}

/** The default DB implementation, that mixes in the database traits. */
@SerialVersionUID(235871232L)
class DefaultDB private[api] (
  val name: String,
  @transient val connection: MongoConnection,
  val connectionState: ConnectionState,
  @transient val failoverStrategy: FailoverStrategy = FailoverStrategy(),
  @transient val session: Option[Session] = Option.empty)
  extends DB with DBMetaCommands with GenericDB[BSONSerializationPack.type]
  with Product with Serializable {

  type DBType = DefaultDB

  @transient val pack: BSONSerializationPack.type = BSONSerializationPack

  def startSession()(implicit ec: ExecutionContext): Future[DefaultDB] =
    session match {
      case Some(_) => Future.successful(this) // NoOp

      case _ => {
        implicit def w: BSONSerializationPack.Writer[StartSession.type] =
          StartSession.commandWriter(BSONSerializationPack)

        implicit def r: BSONSerializationPack.Reader[StartSessionResult] =
          StartSessionResult.reader(BSONSerializationPack)

        Command.run(BSONSerializationPack, failoverStrategy).
          apply(this, StartSession, defaultReadPreference).map(withNewSession)
      }
    }

  private[api] def withNewSession(result: StartSessionResult): DefaultDB =
    new DefaultDB(name, connection, connectionState, failoverStrategy,
      Option(result).map { r =>
        connectionState.setName match {
          case Some(_) => new ReplicaSetSession(r.id)
          case _       => new PlainSession(r.id)
        }
      })

  def endSession()(implicit ec: ExecutionContext): Future[Option[UUID]] =
    session.map(_.lsid) match {
      case state @ Some(uuid) => {
        implicit def w: BSONSerializationPack.Writer[EndSessions] =
          EndSessions.commandWriter(BSONSerializationPack)

        implicit def r: BSONSerializationPack.Reader[UnitBox.type] =
          CommandCodecs.unitBoxReader(BSONSerializationPack)

        val endSessions = new EndSessions(uuid)

        Command.run(BSONSerializationPack, failoverStrategy).
          apply(this, endSessions, defaultReadPreference).map(_ => state)

      }

      case _ =>
        Future.successful(Option.empty[UUID]) // NoOp
    }

  def killSession()(implicit ec: ExecutionContext): Future[Option[UUID]] =
    session.map(_.lsid) match {
      case state @ Some(uuid) => {
        implicit def w: BSONSerializationPack.Writer[KillSessions] =
          KillSessions.commandWriter(BSONSerializationPack)

        implicit def r: BSONSerializationPack.Reader[UnitBox.type] =
          CommandCodecs.unitBoxReader(BSONSerializationPack)

        val killSessions = new KillSessions(uuid)

        Command.run(BSONSerializationPack, failoverStrategy).
          apply(this, killSessions, defaultReadPreference).map(_ => state)

      }

      case _ => Future.successful(Option.empty[UUID]) // NoOp
    }

  @deprecated("DefaultDB will no longer be a Product", "0.16.0")
  val productArity = 3

  @deprecated("DefaultDB will no longer be a Product", "0.16.0")
  def productElement(n: Int): Any = (n: @scala.annotation.switch) match {
    case 0 => name
    case 1 => connection
    case _ => failoverStrategy
  }

  def canEqual(that: Any): Boolean = that match {
    case _: DefaultDB => true
    case _            => false
  }
}

@deprecated("Use DefaultDB class", "0.16.0")
object DefaultDB extends scala.runtime.AbstractFunction3[String, MongoConnection, FailoverStrategy, DefaultDB] {
  @deprecated("Use DefaultDB constructor", "0.16.0")
  def apply(name: String, connection: MongoConnection, failoverStrategy: FailoverStrategy): DefaultDB = throw new UnsupportedOperationException("Use DefaultDB constructor")
}

@deprecated("Will be removed", "0.16.0")
object DB {
}
