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

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.protocol.MongoWireVersion

import reactivemongo.core.errors.GenericDriverException

import reactivemongo.api.commands.{
  Command,
  CommandException,
  EndSessions,
  StartSession,
  StartSessionResult,
  SuccessfulAuthentication,
  EndTransaction
}

/**
 * The reference to a MongoDB database,
 * obtained from a [[reactivemongo.api.MongoConnection]].
 *
 * {{{
 * import scala.concurrent.ExecutionContext
 * import reactivemongo.api.MongoConnection
 *
 * def foo(connection: MongoConnection)(implicit ec: ExecutionContext) = {
 *   val db = connection.database("plugin")
 *   val _ = db.map(_("acoll")) // Collection reference
 * }
 * }}}
 *
 * @param name this database name
 * @param connection the [[reactivemongo.api.MongoConnection]] that will be used to query this database
 * @param failoverStrategy the default failover strategy for sending requests
 *
 * @define resolveDescription Returns a [[reactivemongo.api.Collection]] reference from this database
 * @define nameParam the name of the collection to resolve
 * @define failoverStrategyParam the failover strategy to override the default one
 * @define startSessionDescription Starts a [[https://docs.mongodb.com/manual/reference/command/startSession/ new session]] (since MongoDB 3.6)
 * @define startTxDescription Starts a transaction (since MongoDB 4.0)
 * @define abortTxDescription [[https://docs.mongodb.com/manual/reference/command/abortTransaction Aborts the transaction]] associated with the current client session (since MongoDB 4.0)
 * @define commitTxDescription [[https://docs.mongodb.com/manual/reference/command/commitTransaction Commits the transaction]] associated with the current client session (since MongoDB 4.0)
 * @define killSessionDescription [[https://docs.mongodb.com/manual/reference/command/killSessions Kills (aborts) the session]] associated with this database reference (since MongoDB 3.6)
 * @define endSessionDescription [[https://docs.mongodb.com/manual/reference/command/endSessions Ends (closes) the session]] associated with this database reference (since MongoDB 3.6)
 * @define commandTParam the [[reactivemongo.api.commands.Command]] type
 * @define commandParam the command to be executed on the current database
 * @define failoverStrategyParam the failover strategy to override the default one
 * @define readPrefParam the read preference for the result
 * @define writerParam the writer for the command
 * @define readerParam the reader for the result of command execution
 * @define resultType the result type
 * @define cursorFetcher A cursor for the command results
 * @define singleResult A single result from command execution
 * @define txWriteConcernParam the write concern for the transaction operation
 */
final class DB private[api] (
  val name: String,
  val connection: MongoConnection,
  private[api] val connectionState: ConnectionState,
  val failoverStrategy: FailoverStrategy = FailoverStrategy.default,
  private[reactivemongo] val session: Option[Session] = Option.empty)
  extends DBMetaCommands with PackSupport[Serialization.Pack] {

  import Serialization.{ internalSerializationPack, unitReader }

  val pack: Serialization.Pack = internalSerializationPack

  /**
   * $resolveDescription (alias for the [[collection]] method).
   *
   * @tparam C the [[Collection]] type
   * @param name $nameParam
   * @param failoverStrategy $failoverStrategyParam
   */
  @SuppressWarnings(Array("VariableShadowing"))
  def apply[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = Serialization.defaultCollectionProducer): C = collection(name, failoverStrategy)

  /**
   * $resolveDescription.
   *
   * @tparam C the [[Collection]] type
   * @param name $nameParam
   * @param failoverStrategy $failoverStrategyParam
   *
   * {{{
   * import reactivemongo.api.DB
   *
   * def resolveColl(db: DB) = db.collection("acoll")
   * }}}
   */
  @SuppressWarnings(Array("VariableShadowing"))
  def collection[C <: Collection](name: String, failoverStrategy: FailoverStrategy = failoverStrategy)(implicit producer: CollectionProducer[C] = Serialization.defaultCollectionProducer): C = producer(this, name, failoverStrategy)

  /**
   * Authenticates the connection on this database.
   *
   * @param user the name of the user
   * @param password the user password
   *
   * @see `MongoConnection.authenticate`
   */
  def authenticate(user: String, password: String)(implicit ec: ExecutionContext): Future[SuccessfulAuthentication] = connection.authenticate(name, user, password, failoverStrategy)

  /**
   * $startSessionDescription, does nothing if a session has already being started .
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   * import reactivemongo.api.DB
   *
   * def equivalentTo(db: DB)(implicit ec: ExecutionContext) =
   *   db.startSession(failIfAlreadyStarted = false)
   * }}}
   *
   * @return The database reference updated with a new session
   */
  @inline def startSession()(implicit ec: ExecutionContext): Future[DB] = startSession(failIfAlreadyStarted = false)

  /**
   * $startSessionDescription.
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   * import reactivemongo.api.DB
   *
   * def startIt(db: DB)(implicit ec: ExecutionContext) =
   *   db.startSession(failIfAlreadyStarted = true)
   * }}}
   *
   * @param failIfAlreadyStarted if true fails if a session is already started
   *
   * @return The database reference updated with a new session,
   * if none is already started with the current reference.
   */
  def startSession(failIfAlreadyStarted: Boolean)(implicit ec: ExecutionContext): Future[DB] = session match {
    case Some(s) if failIfAlreadyStarted =>
      Future.failed[DB](new GenericDriverException(
        s"Session '${s.lsid}' is already started"))

    case Some(_) =>
      Future.successful(this) // NoOp

    case _ => {
      implicit def w: pack.Writer[StartSession.type] =
        StartSession.commandWriter(internalSerializationPack)

      implicit def r: pack.Reader[StartSessionResult] =
        StartSessionResult.reader(internalSerializationPack)

      Command.run(internalSerializationPack, failoverStrategy).
        apply(this, StartSession, defaultReadPreference).map { res =>
          withNewSession(res)
        }
    }
  }

  private def withNewSession(r: StartSessionResult): DB = withSession {
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

  @SuppressWarnings(Array("VariableShadowing"))
  @inline private def withSession(session: Session): DB = new DB(
    name, connection, connectionState, failoverStrategy, Some(session))

  /**
   * $startTxDescription, if none is already started with
   * the current client session otherwise does nothing.
   *
   * It fails if no session is previously started (see `startSession`).
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   * import reactivemongo.api.{ DB, WriteConcern }
   *
   * def equivalentTo(db: DB, aWriteConcern: Option[WriteConcern])(
   *   implicit ec: ExecutionContext) =
   *   db.startTransaction(aWriteConcern, failIfAlreadyStarted = false)
   * }}}
   *
   * @param writeConcern $txWriteConcernParam
   *
   * @return The database reference with transaction.
   */
  @inline def startTransaction(writeConcern: Option[WriteConcern])(implicit ec: ExecutionContext): Future[DB] = startTransaction(writeConcern, false)

  private def transactionNode()(implicit ec: ExecutionContext): Future[Option[String]] = {
    if (connectionState.isMongos) { // node required to pin transaction
      connection.pickNode(defaultReadPreference).map(Option(_))
    } else {
      Future.successful(Option.empty[String])
    }
  }

  /**
   * $startTxDescription, if none is already started with
   * the current client session otherwise does nothing.
   *
   * It fails if no session is previously started (see `startSession`).
   *
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   * import reactivemongo.api.{ DB, WriteConcern }
   *
   * def doIt(db: DB, aWriteConcern: Option[WriteConcern])(
   *   implicit ec: ExecutionContext) =
   *   db.startTransaction(aWriteConcern, failIfAlreadyStarted = true)
   * }}}
   *
   * @param writeConcern $txWriteConcernParam
   *
   * @return The database reference with transaction.
   */
  def startTransaction(writeConcern: Option[WriteConcern], failIfAlreadyStarted: Boolean)(implicit ec: ExecutionContext): Future[DB] = session match {
    case Some(s) => transactionNode().flatMap { txNode =>
      val wc = writeConcern getOrElse defaultWriteConcern

      s.startTransaction(wc, txNode) match {
        case Failure(cause) =>
          Future.failed[DB](cause)

        case Success((tx, false)) if failIfAlreadyStarted =>
          Future.failed[DB](new GenericDriverException(s"Transaction ${tx.txnNumber} was already started with session '${s.lsid}'"))

        case Success(_) =>
          // Dummy find, to make sure a CRUD command is sent
          // with 'startTransaction' right now
          collection(s"startx${Thread.currentThread().getId}").
            find(pack.newBuilder.document(Seq.empty)).
            one[pack.Document].map(_ => this)
      }
    }

    case _ =>
      Future.failed[DB](new GenericDriverException(
        "Cannot start a transaction without a started session"))
  }

  /**
   * $abortTxDescription, if any otherwise does nothing .
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   * import reactivemongo.api.DB
   *
   * def equivalentTo(db: DB)(implicit ec: ExecutionContext) =
   *   db.abortTransaction(failIfNotStarted = false)
   * }}}
   *
   * @return The database reference with transaction aborted (but not session)
   */
  @inline def abortTransaction()(implicit ec: ExecutionContext): Future[DB] = abortTransaction(failIfNotStarted = false)

  /**
   * $abortTxDescription, if any otherwise does nothing .
   *
   * @return The database reference with transaction aborted (but not session)
   */
  def abortTransaction(failIfNotStarted: Boolean)(implicit ec: ExecutionContext): Future[DB] = endTransaction(failIfNotStarted) { (s, wc) =>
    EndTransaction.abort(s, wc)
  }

  /**
   * $commitTxDescription, if any otherwise does nothing .
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   * import reactivemongo.api.DB
   *
   * def equivalentTo(db: DB)(implicit ec: ExecutionContext) =
   *   db.commitTransaction(failIfNotStarted = false)
   * }}}
   *
   * @return The database reference with transaction commited (but not session)
   */
  @inline def commitTransaction()(implicit ec: ExecutionContext): Future[DB] = commitTransaction(failIfNotStarted = false)

  /**
   * $commitTxDescription, if any otherwise does nothing .
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   * import reactivemongo.api.DB
   *
   * def commitIt(db: DB)(implicit ec: ExecutionContext) =
   *   db.commitTransaction(failIfNotStarted = true)
   * }}}
   *
   * @return The database reference with transaction commited (but not session)
   */
  def commitTransaction(failIfNotStarted: Boolean)(implicit ec: ExecutionContext): Future[DB] = endTransaction(failIfNotStarted) { (s, wc) =>
    EndTransaction.commit(s, wc)
  }

  private def endTransaction(failIfNotStarted: Boolean)(
    command: (Session, WriteConcern) => EndTransaction)(
    implicit
    ec: ExecutionContext): Future[DB] = session match {
    case Some(s) => s.transaction match {
      case Failure(cause) if failIfNotStarted =>
        Future.failed[DB](new GenericDriverException(
          s"Cannot end failed transaction (${cause.getMessage})"))

      case Failure(_) =>
        Future.successful(this)

      case Success(tx) => tx.writeConcern match {
        case Some(wc) => {
          implicit def w: Serialization.Pack#Writer[EndTransaction] =
            EndTransaction.commandWriter

          connection.database("admin").flatMap { adminDb =>
            Command.run(internalSerializationPack, failoverStrategy).apply(
              adminDb.withSession(s), command(s, wc), defaultReadPreference).
              map(_ => {}).recoverWith {
                case CommandException.Code(251) =>
                  // Transaction isn't in progress (started but no op within)
                  Future.successful({})
              }.flatMap { _ =>
                s.endTransaction() match {
                  case Some(_) =>
                    Future.successful(this)

                  case _ if failIfNotStarted =>
                    Future.failed[DB](
                      new GenericDriverException("Cannot end transaction"))

                  case _ =>
                    Future.successful(this)
                }
              }
          }
        }

        case _ if failIfNotStarted =>
          Future.failed[DB](new GenericDriverException(s"Cannot end transaction without write concern (session '${s.lsid}')"))

        case _ =>
          Future.successful(this)
      }
    }

    case _ if failIfNotStarted =>
      Future.failed[DB](new GenericDriverException(
        "Cannot end transaction without a started session"))

    case _ =>
      Future.successful(this)
  }

  /**
   * $endSessionDescription, if any otherwise does nothing .
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   * import reactivemongo.api.DB
   *
   * def equivalentTo(db: DB)(implicit ec: ExecutionContext) =
   *   db.endSession(failIfNotStarted = false)
   * }}}
   *
   * @return The database reference with session ended
   */
  @inline def endSession()(implicit ec: ExecutionContext): Future[DB] = endSession(failIfNotStarted = false)

  /**
   * $endSessionDescription, if any otherwise does nothing .
   *
   * @return The database reference with session ended
   */
  def endSession(failIfNotStarted: Boolean)(implicit ec: ExecutionContext): Future[DB] = endSessionById(failIfNotStarted) { lsid => EndSessions.end(lsid) }

  /**
   * $killSessionDescription, if any otherwise does nothing .
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   * import reactivemongo.api.DB
   *
   * def equivalentTo(db: DB)(implicit ec: ExecutionContext) =
   *   db.killSession(failIfNotStarted = false)
   * }}}
   *
   * @return The database reference with session aborted
   */
  @inline def killSession()(implicit ec: ExecutionContext): Future[DB] = killSession(failIfNotStarted = false)

  /**
   * $killSessionDescription, if any otherwise does nothing .
   *
   * @return The database reference with session aborted
   */
  def killSession(failIfNotStarted: Boolean)(implicit ec: ExecutionContext): Future[DB] = endSessionById(failIfNotStarted) { lsid => EndSessions.kill(lsid) }

  private def endSessionById(failIfNotStarted: Boolean)(command: java.util.UUID => EndSessions)(implicit ec: ExecutionContext): Future[DB] = session.map(_.lsid) match {
    case Some(lsid) => {
      implicit def w: pack.Writer[EndSessions] =
        EndSessions.commandWriter(internalSerializationPack)

      Command.run(internalSerializationPack, failoverStrategy).
        apply(this, command(lsid), defaultReadPreference).map(_ =>
          new DB(
            name, connection, connectionState, failoverStrategy,
            session = None))

    }

    case _ if failIfNotStarted =>
      Future.failed[DB](new GenericDriverException(
        "Cannot end not started session"))

    case _ =>
      Future.successful(this) // NoOp
  }

  /**
   * '''EXPERIMENTAL:''' API may change without notice.
   */
  @SuppressWarnings(Array("VariableShadowing"))
  def getMore[P <: SerializationPack, T](
    pack: P,
    reference: Cursor.Reference,
    readPreference: ReadPreference = defaultReadPreference,
    failoverStrategy: FailoverStrategy = this.failoverStrategy,
    maxTimeMS: Option[Long] = None)(
    implicit
    reader: pack.Reader[T],
    cp: CursorProducer[T]): cp.ProducedCursor = {
    @inline def r = reader

    val cur = new DefaultCursor.GetMoreCursor[T](
      db = this,
      _ref = reference,
      readPreference = readPreference,
      failover = failoverStrategy,
      maxTimeMS = maxTimeMS) {
      type P = pack.type
      val _pack: P = pack
      val reader = r
    }

    cp.produce(cur)
  }

  import reactivemongo.api.commands._

  /**
   * @tparam R $resultType
   * @tparam C $commandTParam
   * @param command $commandParam
   * @param failoverStrategy $failoverStrategyParam
   * @param readPreference $readPrefParam
   * @param writer $writerParam
   * @param reader $readerParam
   * @return $singleResult
   */
  @SuppressWarnings(Array("VariableShadowing"))
  def runCommand[R, C <: Command with CommandWithResult[R]](command: C with CommandWithResult[R], failoverStrategy: FailoverStrategy = FailoverStrategy.default, readPreference: ReadPreference = this.defaultReadPreference)(implicit writer: pack.Writer[C], reader: pack.Reader[R], ec: ExecutionContext): Future[R] = Command.run(pack, failoverStrategy).apply(this, command, readPreference)

  /**
   * @tparam C $commandTParam
   * @param command $commandParam
   * @param failoverStrategy $failoverStrategyParam
   * @param writer $writerParam
   * @param reader $readerParam
   * @return $cursorFetcher
   */
  @SuppressWarnings(Array("VariableShadowing"))
  def runCommand[C <: Command](command: C, failoverStrategy: FailoverStrategy)(implicit writer: pack.Writer[C]): CursorFetcher[pack.type, Cursor] = Command.run(pack, failoverStrategy).apply(this, command)

  /**
   * Run a raw command (represented by a document).
   *
   * {{{
   * import reactivemongo.api.FailoverStrategy
   * import reactivemongo.api.bson.BSONDocument
   *
   * def getUsers(db: reactivemongo.api.DB) =
   *   db.runCommand(BSONDocument("usersInfo" -> 1), FailoverStrategy.default)
   * }}}
   *
   * @param command $commandParam
   * @param failoverStrategy $failoverStrategyParam
   * @param readPreference $readPrefParam
   * @return $cursorFetcher
   */
  @SuppressWarnings(Array("VariableShadowing"))
  def runCommand(
    command: pack.Document,
    failoverStrategy: FailoverStrategy): CursorFetcher[pack.type, Cursor] = {
    val runner = Command.run[pack.type](pack, failoverStrategy)
    implicit def w: pack.Writer[pack.Document] = pack.IdentityWriter
    import runner.RawCommand.writer

    runner(this, runner.rawCommand(command))
  }

  // ---

  import connection.options

  @inline private[api] def defaultReadPreference: ReadPreference =
    options.readPreference

  @inline private[api] def defaultWriteConcern: WriteConcern =
    options.writeConcern

  override def toString = s"DB($name)"
}
