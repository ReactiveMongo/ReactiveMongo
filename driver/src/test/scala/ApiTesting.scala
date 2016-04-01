package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }

package object tests {
  // Test alias
  def _failover2[A](c: MongoConnection, s: FailoverStrategy)(p: () => Future[A])(implicit ec: ExecutionContext): Failover2[A] = Failover2.apply(c, s)(p)(ec)

  def isAvailable(con: MongoConnection): Future[Boolean] = con.isAvailable

  def waitIsAvailable(con: MongoConnection, failoverStrategy: FailoverStrategy)(implicit ec: ExecutionContext): Future[Unit] = con.waitIsAvailable(failoverStrategy)
}
