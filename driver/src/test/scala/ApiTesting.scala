package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.SECONDS

import akka.util.Timeout
import akka.actor.ActorRef

import reactivemongo.core.nodeset.{ Authenticate, NodeSet }
import reactivemongo.core.actors.{
  ChannelClosed,
  ConnectAll,
  MongoDBSystem,
  StandardDBSystem
}
import reactivemongo.api.commands.bson.BSONIsMasterCommand.IsMasterResult

package object tests {
  // Test alias
  def _failover2[A](c: MongoConnection, s: FailoverStrategy)(p: () => Future[A])(implicit ec: ExecutionContext): Failover2[A] = Failover2.apply(c, s)(p)(ec)

  def isAvailable(con: MongoConnection)(implicit ec: ExecutionContext): Future[Boolean] = con.probe.map(_.isEmpty)

  def waitIsAvailable(con: MongoConnection, failoverStrategy: FailoverStrategy)(implicit ec: ExecutionContext): Future[Unit] = con.waitIsAvailable(failoverStrategy)

  def standardDBSystem(supervisor: String, name: String, nodes: Seq[String], authenticates: Seq[Authenticate], options: MongoConnectionOptions) = new StandardDBSystem(supervisor, name, nodes, authenticates, options)

  def addConnection(d: MongoDriver, name: String, nodes: Seq[String], options: MongoConnectionOptions, mongosystem: ActorRef): Future[Any] = {
    import akka.pattern.ask

    def message = d.AddConnection(name, nodes, options, mongosystem)
    implicit def timeout = Timeout(10, SECONDS)

    d.supervisorActor ? message
  }

  def history(sys: MongoDBSystem): Traversable[(Long, String)] =
    sys.history.toArray.toList.collect {
      case (time: Long, event: String) => time -> event
    }

  def nodeSet(sys: MongoDBSystem): NodeSet = sys.getNodeSet

  def channelClosed(id: Int) = ChannelClosed(id)
}
