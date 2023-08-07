package reactivemongo.api

import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.{ Duration, FiniteDuration }

import reactivemongo.actors.pattern.after
import reactivemongo.util.LazyLogger

private[reactivemongo] final class Failover[A](
    producer: () => Future[A],
    connection: MongoConnection,
    failoverStrategy: FailoverStrategy
  )(implicit
    ec: ExecutionContext) {

  import Failover.logger, logger.trace

  private val lnm = s"${connection.supervisor}/${connection.name}" // log name

  /**
   * A future that is completed with a response,
   * after 1 or more attempts (specified in the given strategy).
   */
  val future: Future[A] = send(0) // promise.future

  // Wraps any exception from the producer
  // as a result Future.failed that can be recovered.
  private def next(): Future[A] =
    try {
      producer()
    } catch {
      case NonFatal(producerErr) => Future.failed[A](producerErr)
    }

  private def send(n: Int): Future[A] =
    next()
      .map[Try[A]](Success(_))
      .recover[Try[A]] { case err => Failure(err) }
      .flatMap {
        case RetryableFailure(e) => {
          if (n < failoverStrategy.retries) {
            val `try` = n + 1
            val delayFactor = failoverStrategy.delayFactor(`try`)
            val delay = Duration
              .unapply(failoverStrategy.initialDelay * delayFactor)
              .fold(failoverStrategy.initialDelay)(t =>
                FiniteDuration(t._1, t._2)
              )

            trace(
              s"[$lnm] Got an error, retrying... (try #${`try`} is scheduled in ${delay.toMillis} ms)",
              e
            )

            after(delay, connection.actorSystem.scheduler)(send(`try`))
          } else {
            // Generally that means that the primary is not available
            // or the nodeset is unreachable
            logger.error(
              s"[$lnm] Got an error, no more attempts to do. Completing with a failure... ",
              e
            )

            Future.failed(e)
          }
        }

        case Failure(e) => {
          trace(
            s"[$lnm] Got an non retryable error, completing with a failure... ",
            e
          )
          Future.failed(e)
        }

        case Success(response) => {
          trace(s"[$lnm] Got a successful result, completing...")
          Future.successful(response)
        }
      }

  // send(0)
}

private[api] object RetryableFailure {
  import reactivemongo.core.protocol.Response
  import reactivemongo.core.errors._
  import reactivemongo.core.actors.Exceptions._

  def unapply[T](result: Try[T]): Option[Throwable] = result match {
    case Failure(cause) if isRetryable(cause) =>
      Some(cause)

    case Success(Response.CommandError(_, _, _, cause: Throwable))
        if (isRetryable(cause)) =>
      Some(cause)

    case _ =>
      None
  }

  private def isRetryable(throwable: Throwable) = throwable match {
    case e: ChannelNotFoundException     => e.retriable
    case _: NotAuthenticatedException    => true
    case _: PrimaryUnavailableException  => true
    case _: NodeSetNotReachableException => true
    case _: ConnectionException          => true
    case e: DatabaseException =>
      e.isNotAPrimaryError || e.isUnauthorized

    case _ => false
  }
}

private[reactivemongo] object Failover {
  private[api] val logger = LazyLogger("reactivemongo.api.Failover")

  def apply[A](
      connection: MongoConnection,
      failoverStrategy: FailoverStrategy
    )(producer: () => Future[A]
    )(implicit
      ec: ExecutionContext
    ): Failover[A] =
    new Failover(producer, connection, failoverStrategy)
}
