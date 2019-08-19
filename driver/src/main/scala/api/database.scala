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

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration

import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.core.commands.SuccessfulAuthentication

import reactivemongo.api.commands.{
  Command,
  CommandCodecs,
  CommandError,
  EndSessions,
  StartSession,
  StartSessionResult,
  EndTransaction,
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
 * @define failoverStrategyParam the failover strategy to override the default one
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
   * @param failoverStrategy $failoverStrategyParam
   */
  def apply[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = collections.bson.BSONCollectionProducer): C = collection(name, failoverStrategy)

  /**
   * $resolveDescription.
   *
   * @param name $nameParam
   * @param failoverStrategy $failoverStrategyParam
   */
  def collection[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = collections.bson.BSONCollectionProducer): C = producer(this, name, failoverStrategy)

  @inline def defaultReadPreference: ReadPreference =
    connection.options.readPreference

  /**
   * Authenticates the connection on this database.
   *
   * @param user the name of the user
   * @param password the user password
   */
  def authenticate(user: String, password: String)(implicit ec: ExecutionContext): Future[SuccessfulAuthentication] = connection.authenticate(name, user, password, failoverStrategy)

  /**
   * Returns the database of the given name on the same MongoConnection.
   *
   * @param name $nameParam
   * @param failoverStrategy $failoverStrategyParam
   * @see [[sibling1]]
   */
  @deprecated("Use [[sibling1]]", "0.16.0")
  def sibling(name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit ec: ExecutionContext): DefaultDB = Await.result(connection.database(name, failoverStrategy), FiniteDuration(15, "seconds"))

  /**
   * Returns the database of the given name on the same MongoConnection.
   *
   * @param name $nameParam
   * @param failoverStrategy $failoverStrategyParam
   */
  def sibling1(name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit ec: ExecutionContext): Future[DefaultDB] = connection.database(name, failoverStrategy)

  /**
   * Starts a [[https://docs.mongodb.com/manual/reference/command/startSession/ new session]], does nothing if a session has already being started (since MongoDB 3.6).
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * @return `None` if session is already started,
   * or `Some` database reference updated with a new session
   */
  def startSession()(implicit ec: ExecutionContext): Future[Option[DBType]]

  /**
   * Starts a transaction if none is already started with
   * the current client session, otherwise does nothing (since MongoDB 3.6).
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * @return `None` if there is no session/transaction to abort
   * or `Some` database reference with transaction aborted (but not session)
   */
  def startTransaction(writeConcern: Option[WriteConcern]): Option[DBType]

  /**
   * [[https://docs.mongodb.com/manual/reference/command/abortTransaction Ends the transaction]] associated with the current client session, if any otherwise does nothing (since MongoDB 3.6).
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * @return `None` if there is no session/transaction to abort
   * or `Some` database reference with transaction aborted (but not session)
   */
  def abortTransaction()(implicit ec: ExecutionContext): Future[Option[DBType]]

  /**
   * [[https://docs.mongodb.com/manual/reference/command/commitTransaction Ends the transaction]] associated with the current client session, if any otherwise does nothing (since MongoDB 3.6).
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * @return `None` if there is no session/transaction to abort
   * or `Some` database reference with transaction aborted (but not session)
   */
  def commitTransaction()(implicit ec: ExecutionContext): Future[Option[DBType]]

  /**
   * [[https://docs.mongodb.com/manual/reference/command/endSessions Ends the session]] associated with this database reference, if any otherwise does nothing (since MongoDB 3.6).
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * @return `None` if there is no session to end
   * or `Some` database reference with session ended
   */
  def endSession()(implicit ec: ExecutionContext): Future[Option[DBType]]

  /**
   * [[https://docs.mongodb.com/manual/reference/command/killSessions Kills the session]] (abort) associated with this database reference, if any otherwise does nothing (since MongoDB 3.6).
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * @return `None` if there is no session to kill
   * or `Some` database reference with session aborted
   */
  def killSession()(implicit ec: ExecutionContext): Future[DBType]
}

/**
 * @define failoverStrategyParam the failover strategy to override the default one
 */
private[api] sealed trait GenericDB[P <: SerializationPack with Singleton] { self: DB =>
  val pack: P

  import reactivemongo.api.commands._

  /**
   * @param failoverStrategy $failoverStrategyParam
   */
  def runCommand[R, C <: Command with CommandWithResult[R]](command: C with CommandWithResult[R], failoverStrategy: FailoverStrategy)(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = Command.run(pack, failoverStrategy).apply(self, command, self.defaultReadPreference)

  /**
   * @param failoverStrategy $failoverStrategyParam
   */
  def runCommand[C <: Command](command: C, failoverStrategy: FailoverStrategy)(implicit writer: pack.Writer[C]): CursorFetcher[pack.type, Cursor] = Command.run(pack, failoverStrategy).apply(self, command)

  /**
   * @param failoverStrategy $failoverStrategyParam
   */
  def runValueCommand[A <: AnyVal, R <: BoxedAnyVal[A], C <: Command with CommandWithResult[R]](command: C with CommandWithResult[R with BoxedAnyVal[A]], failoverStrategy: FailoverStrategy, readPreference: ReadPreference)(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[A] = Command.run(pack, failoverStrategy).unboxed(self, command, readPreference)
}

/** The default DB implementation, that mixes in the database traits. */
@SerialVersionUID(235871232L)
class DefaultDB private[api] (
  val name: String,
  @transient val connection: MongoConnection,
  val connectionState: ConnectionState,
  @transient val failoverStrategy: FailoverStrategy = FailoverStrategy(),
  @transient private[reactivemongo] val session: Option[Session] = Option.empty)
  extends DB with DBMetaCommands with GenericDB[BSONSerializationPack.type]
  with Product with Serializable {

  type DBType = DefaultDB

  @transient val pack: BSONSerializationPack.type = BSONSerializationPack

  private implicit def unitReader: BSONSerializationPack.Reader[UnitBox.type] =
    CommandCodecs.unitBoxReader(BSONSerializationPack)

  def startSession()(implicit ec: ExecutionContext): Future[Option[DefaultDB]] =
    session match {
      case Some(_) => Future.successful(Option.empty[DefaultDB]) // NoOp

      case _ => {
        implicit def w: BSONSerializationPack.Writer[StartSession.type] =
          StartSession.commandWriter(BSONSerializationPack)

        implicit def r: BSONSerializationPack.Reader[StartSessionResult] =
          StartSessionResult.reader(BSONSerializationPack)

        Command.run(BSONSerializationPack, failoverStrategy).
          apply(this, StartSession, defaultReadPreference).map { res =>
            Option(withNewSession(res))
          }
      }
    }

  private def withNewSession(result: StartSessionResult): DefaultDB =
    new DefaultDB(name, connection, connectionState, failoverStrategy,
      session = Option(result).map { r =>
        if (connectionState.setName.isDefined || (
          connectionState.isMongos &&
          connectionState.metadata.
          maxWireVersion.compareTo(MongoWireVersion.V42) >= 0)) {

          // rs || (sharding && v4.2+)
          new ReplicaSetSession(r.id)
        } else {
          new PlainSession(r.id)
        }
      })

  def startTransaction(writeConcern: Option[WriteConcern]): Option[DBType] = {
    val wc = writeConcern getOrElse defaultWriteConcern

    session.flatMap(_.startTransaction(wc)).map(_ => this)
  }

  def abortTransaction()(implicit ec: ExecutionContext): Future[Option[DBType]] = endTransaction { (s, wc) =>
    EndTransaction.abort(s, wc)
  }

  def commitTransaction()(implicit ec: ExecutionContext): Future[Option[DBType]] = endTransaction { (s, wc) =>
    EndTransaction.commit(s, wc)
  }

  private def endTransaction(command: (Session, WriteConcern) => EndTransaction)(implicit ec: ExecutionContext): Future[Option[DBType]] =
    (for {
      s <- session
      tx <- s.transaction
      wc <- tx.writeConcern
    } yield s -> wc) match {
      case Some((s, wc)) => {
        implicit def w: BSONSerializationPack.Writer[EndTransaction] =
          EndTransaction.commandWriter(BSONSerializationPack)

        connection.database("admin").flatMap { adminDb =>
          Command.run(BSONSerializationPack, failoverStrategy).
            apply(adminDb, command(s, wc), defaultReadPreference).
            andThen {
              case res =>
                println(s"endTransaction: $res")
            }.
            map(_ => { /*TODO*/ }).recoverWith {
              case CommandError.Code(251) =>
                // Transaction isn't in progress (started but no op within)
                Future.successful({})
            }.
            map(_ => s.endTransaction().map(_ => this))
        }
      }

      case _ => Future.successful(Option.empty[DefaultDB]) // NoOp
    }

  def endSession()(implicit ec: ExecutionContext): Future[Option[DefaultDB]] =
    endSessionById { lsid => EndSessions.end(lsid) }

  def killSession()(implicit ec: ExecutionContext): Future[DefaultDB] =
    endSessionById { lsid => EndSessions.kill(lsid) }.map {
      case Some(updated) => updated
      case _             => this
    }

  private def endSessionById(command: java.util.UUID => EndSessions)(implicit ec: ExecutionContext): Future[Option[DefaultDB]] = session.map(_.lsid) match {
    case Some(lsid) => {
      implicit def w: BSONSerializationPack.Writer[EndSessions] =
        EndSessions.commandWriter(BSONSerializationPack)

      Command.run(BSONSerializationPack, failoverStrategy).
        apply(this, command(lsid), defaultReadPreference).map(_ =>
          Some(new DefaultDB(
            name, connection, connectionState, failoverStrategy,
            session = None)))

    }

    case _ =>
      Future.successful(Option.empty[DefaultDB]) // NoOp
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

  @inline private def defaultWriteConcern: WriteConcern = connection.options.writeConcern

  override def toString = s"${getClass.getName}($name)"
}

@deprecated("Use DefaultDB class", "0.16.0")
object DefaultDB extends scala.runtime.AbstractFunction3[String, MongoConnection, FailoverStrategy, DefaultDB] {
  @deprecated("Use DefaultDB constructor", "0.16.0")
  def apply(name: String, connection: MongoConnection, failoverStrategy: FailoverStrategy): DefaultDB = throw new UnsupportedOperationException("Use DefaultDB constructor")
}

@deprecated("Will be removed", "0.16.0")
object DB {
}
