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

import scala.util.{ Failure, Success }

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration

import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.core.errors.GenericDriverException

import reactivemongo.core.commands.SuccessfulAuthentication

import reactivemongo.api.commands.{
  Command,
  CommandError,
  EndSessions,
  StartSession,
  StartSessionResult,
  EndTransaction
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
 * @define startSessionDescription Starts a [[https://docs.mongodb.com/manual/reference/command/startSession/ new session]] (since MongoDB 3.6)
 * @define startTxDescription Starts a transaction (since MongoDB 4.0)
 * @define abortTxDescription [[https://docs.mongodb.com/manual/reference/command/abortTransaction Aborts the transaction]] associated with the current client session (since MongoDB 4.0)
 * @define commitTxDescription [[https://docs.mongodb.com/manual/reference/command/commitTransaction Commits the transaction]] associated with the current client session (since MongoDB 4.0)
 * @define killSessionDescription [[https://docs.mongodb.com/manual/reference/command/killSessions Kills (aborts) the session]] associated with this database reference (since MongoDB 3.6)
 * @define endSessionDescription [[https://docs.mongodb.com/manual/reference/command/endSessions Ends (closes) the session]] associated with this database reference (since MongoDB 3.6)
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
  def apply[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = Compat.defaultCollectionProducer): C = collection(name, failoverStrategy)

  /**
   * $resolveDescription.
   *
   * @param name $nameParam
   * @param failoverStrategy $failoverStrategyParam
   */
  def collection[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = Compat.defaultCollectionProducer): C = producer(this, name, failoverStrategy)

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
   * $startSessionDescription, does nothing if a session has already being started .
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * {{{
   * // Equivalent to
   * db.startSession(failIfAlreadyStarted = false)
   * }}}
   *
   * @return The database reference updated with a new session
   */
  @inline final def startSession()(implicit ec: ExecutionContext): Future[DBType] = startSession(failIfAlreadyStarted = false)

  /**
   * $startSessionDescription.
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * @param failIfAlreadyStarted if true fails if a session is already started
   *
   * @return The database reference updated with a new session,
   * if none is already started with the current reference.
   */
  def startSession(failIfAlreadyStarted: Boolean)(implicit ec: ExecutionContext): Future[DBType]

  /**
   * $startTxDescription, if none is already started with
   * the current client session otherwise does nothing.
   *
   * It fails if no session is previously started (see `startSession`).
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * {{{
   * // Equivalent to
   * db.startTransaction(aWriteConcern, failIfAlreadyStarted = false)
   * }}}
   *
   * @param writeConcern the write concern for the transaction operation
   *
   * @return The database reference with transaction.
   */
  @inline final def startTransaction(writeConcern: Option[WriteConcern])(implicit ec: ExecutionContext): Future[DBType] = startTransaction(writeConcern, false)

  /**
   * $startTxDescription, if none is already started with
   * the current client session otherwise does nothing.
   *
   * It fails if no session is previously started (see `startSession`).
   *
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * {{{
   * // Equivalent to
   * db.startTransaction(aWriteConcern, failIfAlreadyStarted = false)
   * }}}
   *
   * @param writeConcern the write concern for the transaction operation
   *
   * @return The database reference with transaction.
   */
  def startTransaction(writeConcern: Option[WriteConcern], failIfAlreadyStarted: Boolean)(implicit ec: ExecutionContext): Future[DBType]

  /**
   * $abortTxDescription, if any otherwise does nothing .
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * {{{
   * // Equivalent to
   * db.abortTransaction(failIfNotStarted = false)
   * }}}
   *
   * @return The database reference with transaction aborted (but not session)
   */
  @inline final def abortTransaction()(implicit ec: ExecutionContext): Future[DBType] = abortTransaction(failIfNotStarted = false)

  /**
   * $abortTxDescription, if any otherwise does nothing .
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * @return The database reference with transaction aborted (but not session)
   */
  def abortTransaction(failIfNotStarted: Boolean)(implicit ec: ExecutionContext): Future[DBType]

  /**
   * $commitTxDescription, if any otherwise does nothing .
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * {{{
   * // Equivalent to
   * db.commitTransaction(failIfNotStarted = false)
   * }}}
   *
   * @return The database reference with transaction commited (but not session)
   */
  @inline final def commitTransaction()(implicit ec: ExecutionContext): Future[DBType] = commitTransaction(failIfNotStarted = false)

  /**
   * $commitTxDescription, if any otherwise does nothing .
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * @return The database reference with transaction commited (but not session)
   */
  def commitTransaction(failIfNotStarted: Boolean)(implicit ec: ExecutionContext): Future[DBType]

  /**
   * $endSessionDescription, if any otherwise does nothing .
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * {{{
   * // Equivalent to
   * db.endSession(failIfNotStarted = false)
   * }}}
   *
   * @return The database reference with session ended
   */
  @inline final def endSession()(implicit ec: ExecutionContext): Future[DBType] = endSession(failIfNotStarted = false)

  /**
   * $endSessionDescription, if any otherwise does nothing .
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * @return The database reference with session ended
   */
  def endSession(failIfNotStarted: Boolean)(implicit ec: ExecutionContext): Future[DBType]

  /**
   * $killSessionDescription, if any otherwise does nothing .
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * {{{
   * // Equivalent to
   * db.killSession(failIfNotStarted = false)
   * }}}
   *
   * @return The database reference with session aborted
   */
  @inline final def killSession()(implicit ec: ExecutionContext): Future[DBType] = killSession(failIfNotStarted = false)

  /**
   * $killSessionDescription, if any otherwise does nothing .
   *
   * '''EXPERIMENTAL:''' API may change without notice.
   *
   * @return The database reference with session aborted
   */
  def killSession(failIfNotStarted: Boolean)(implicit ec: ExecutionContext): Future[DBType]

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
  extends DB with DBMetaCommands with GenericDB[Compat.SerializationPack]
  with Product with Serializable {

  import Compat.{ internalSerializationPack, unitBoxReader }

  type DBType = DefaultDB

  @transient val pack: Compat.SerializationPack = internalSerializationPack

  def startSession(failIfAlreadyStarted: Boolean)(implicit ec: ExecutionContext): Future[DefaultDB] = session match {
    case Some(s) if failIfAlreadyStarted =>
      Future.failed[DefaultDB](GenericDriverException(
        s"Session '${s.lsid}' is already started"))

    case Some(_) =>
      Future.successful(this) // NoOp

    case _ => {
      implicit def w = StartSession.commandWriter(internalSerializationPack)
      implicit def r = StartSessionResult.reader(internalSerializationPack)

      Command.run(internalSerializationPack, failoverStrategy).
        apply(this, StartSession, defaultReadPreference).map { res =>
          withNewSession(res)
        }
    }
  }

  private def withNewSession(r: StartSessionResult): DefaultDB = withSession {
    if (connectionState.setName.isDefined) {
      new NodeSetSession(r.id)
    } else if (connectionState.isMongos &&
      connectionState.metadata.
      maxWireVersion.compareTo(MongoWireVersion.V42) >= 0) {

      new DistributedSession(r.id)
    } else {
      new PlainSession(r.id)
    }
  }

  @inline private def withSession(session: Session): DefaultDB = new DefaultDB(
    name, connection, connectionState, failoverStrategy, Some(session))

  private def transactionNode()(implicit ec: ExecutionContext): Future[Option[String]] = {
    if (connectionState.isMongos) { // node required to pin transaction
      connection.pickNode(defaultReadPreference).map(Option(_))
    } else {
      Future.successful(Option.empty[String])
    }
  }

  def startTransaction(
    writeConcern: Option[WriteConcern],
    failIfAlreadyStarted: Boolean)(
    implicit
    ec: ExecutionContext): Future[DBType] = {

    session match {
      case Some(s) => transactionNode().flatMap { txNode =>
        val wc = writeConcern getOrElse defaultWriteConcern

        s.startTransaction(wc, txNode) match {
          case Failure(cause) =>
            Future.failed[DBType](cause)

          case Success((tx, false)) if failIfAlreadyStarted =>
            Future.failed[DBType](GenericDriverException(s"Transaction ${tx.txnNumber} was already started with session '${s.lsid}'"))

          case Success(_) =>
            Future.successful(this)
        }
      }

      case _ =>
        Future.failed[DBType](GenericDriverException(
          s"Cannot start a transaction without a started session"))
    }
  }

  def abortTransaction(failIfNotStarted: Boolean)(implicit ec: ExecutionContext): Future[DBType] = endTransaction(failIfNotStarted) { (s, wc) =>
    EndTransaction.abort(s, wc)
  }

  def commitTransaction(failIfNotStarted: Boolean)(implicit ec: ExecutionContext): Future[DBType] = endTransaction(failIfNotStarted) { (s, wc) =>
    EndTransaction.commit(s, wc)
  }

  private def endTransaction(failIfNotStarted: Boolean)(
    command: (Session, WriteConcern) => EndTransaction)(
    implicit
    ec: ExecutionContext): Future[DBType] = session match {
    case Some(s) => s.transaction match {
      case Failure(cause) if failIfNotStarted =>
        Future.failed[DBType](GenericDriverException(
          s"Cannot end failed transaction (${cause.getMessage})"))

      case Failure(_) =>
        Future.successful(this)

      case Success(tx) => tx.writeConcern match {
        case Some(wc) => {
          implicit def w = EndTransaction.commandWriter(
            internalSerializationPack)

          connection.database("admin").flatMap { adminDb =>
            Command.run(internalSerializationPack, failoverStrategy).apply(
              adminDb.withSession(s), command(s, wc), defaultReadPreference).
              map(_ => {}).recoverWith {
                case CommandError.Code(251) =>
                  // Transaction isn't in progress (started but no op within)
                  Future.successful({})
              }.flatMap { _ =>
                s.endTransaction() match {
                  case Some(_) =>
                    Future.successful(this)

                  case _ if failIfNotStarted =>
                    Future.failed[DBType](
                      GenericDriverException("Cannot end transaction"))

                  case _ =>
                    Future.successful(this)
                }
              }
          }
        }

        case _ if failIfNotStarted =>
          Future.failed[DBType](GenericDriverException(s"Cannot end transaction without write concern (session '${s.lsid}')"))

        case _ =>
          Future.successful(this)
      }
    }

    case _ if failIfNotStarted =>
      Future.failed[DBType](GenericDriverException(
        "Cannot end transaction without a started session"))

    case _ =>
      Future.successful(this)
  }

  def endSession(failIfNotStarted: Boolean)(implicit ec: ExecutionContext): Future[DefaultDB] = endSessionById(failIfNotStarted) { lsid => EndSessions.end(lsid) }

  def killSession(failIfNotStarted: Boolean)(implicit ec: ExecutionContext): Future[DefaultDB] =
    endSessionById(failIfNotStarted) { lsid => EndSessions.kill(lsid) }

  private def endSessionById(failIfNotStarted: Boolean)(command: java.util.UUID => EndSessions)(implicit ec: ExecutionContext): Future[DefaultDB] = session.map(_.lsid) match {
    case Some(lsid) => {
      implicit def w = EndSessions.commandWriter(internalSerializationPack)

      Command.run(internalSerializationPack, failoverStrategy).
        apply(this, command(lsid), defaultReadPreference).map(_ =>
          new DefaultDB(
            name, connection, connectionState, failoverStrategy,
            session = None))

    }

    case _ if failIfNotStarted =>
      Future.failed[DBType](GenericDriverException(
        "Cannot end not started session"))

    case _ =>
      Future.successful(this) // NoOp
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
