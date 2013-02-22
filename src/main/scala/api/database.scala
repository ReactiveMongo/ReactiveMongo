package reactivemongo.api

import reactivemongo.api.indexes.IndexesManager
import reactivemongo.bson._
import reactivemongo.core.commands.{Update => UpdateCommand, _}
import reactivemongo.core.protocol._
import reactivemongo.utils.EitherMappableFuture._

import org.jboss.netty.buffer.ChannelBuffer
import play.api.libs.iteratee._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/**
 * A Mongo Database.
 *
 * Example:
{{{
import reactivemongo.api._

val connection = MongoConnection( List( "localhost:27016" ) )
val db = connection("plugin")
val collection = db("acoll")

// more explicit way
val db2 = connection.db("plugin")
val collection2 = db2.collection("plugin")
}}}
 */
trait DB {
  /** The [[reactivemongo.api.MongoConnection]] that will be used to query this database. */
  val connection: MongoConnection
  /** This database name. */
  val name: String

  /**
   * Gets a [[reactivemongo.api.Collection]] from this database (alias for the `collection` method).
   *
   * @param name The name of the collection to open.
   */
    def apply[C <: Collection](name: String, failoverStrategy: FailoverStrategy = FailoverStrategy())(implicit producer: CollectionProducer[C] = collections.default.BSONCollectionProducer): C = collection(name, failoverStrategy)


  /**
   * Gets a [[reactivemongo.api.Collection]] from this database.
   *
   * @param name The name of the collection to open.
   */
  def collection[C <: Collection](name: String, failoverStrategy: FailoverStrategy = FailoverStrategy())(implicit producer: CollectionProducer[C] = collections.default.BSONCollectionProducer): C = {
    producer.apply(this, name, failoverStrategy)
  }

  /**
   * Sends a command and get the future result of the command.
   *
   * @param command The command to send.
   *
   * @return a future containing the result of the command.
   */
  def command[T](command: Command[T])(implicit ec: ExecutionContext) :Future[T]

  /** Authenticates the connection on this database. */
  def authenticate(user: String, password: String)(implicit timeout: FiniteDuration) :Future[SuccessfulAuthentication] = connection.authenticate(name, user, password)
  
  
  
}

/** A mixin for making failover requests on the database. */
trait FailoverDB {
  self: DB=>

  /** A failover strategy for sending requests. */
  val failoverStrategy: FailoverStrategy

  def command[T](command: Command[T])(implicit ec: ExecutionContext) :Future[T] =
    Failover(command.apply(name).maker, connection.mongosystem, failoverStrategy).future.mapEither(command.ResultMaker(_))
}

/** A mixin that provides commands about this database itself. */
trait DBMetaCommands {
  self: DB =>

  /** Drops this database. */
  def drop()(implicit ec: ExecutionContext) :Future[Boolean] = command(new DropDatabase())

  /** Returns an index manager for this database. */
  def indexesManager(implicit ec: ExecutionContext) = new IndexesManager(self)
}

/** The default DB implementation, that mixes in the database traits. */
case class DefaultDB(
  name: String,
  connection: MongoConnection,
  failoverStrategy: FailoverStrategy = FailoverStrategy()
) extends DB with DBMetaCommands with FailoverDB {
  //def collection(name: String) = DefaultCollection(name, this, failoverStrategy)
}

object DB {
  def apply(name: String, connection: MongoConnection, failoverStrategy: FailoverStrategy = FailoverStrategy()) = DefaultDB(name, connection, failoverStrategy)
}