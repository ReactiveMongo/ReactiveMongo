package reactivemongo.api

import scala.util.{ Try, Failure, Success }

import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.{ Duration, FiniteDuration }

import akka.pattern.after

import reactivemongo.core.actors.{
  CheckedWriteRequestExpectingResponse,
  ExpectingResponse,
  RequestMakerExpectingResponse
}
import reactivemongo.core.errors.ConnectionException
import reactivemongo.core.protocol.{
  CheckedWriteRequest,
  RequestMaker,
  Response
}
import reactivemongo.util.LazyLogger

/**
 * A helper that sends the given message to the given actor,
 * following a failover strategy.
 * This helper holds a future reference that is completed with a response,
 * after 1 or more attempts (specified in the given strategy).
 * If the all the tryouts configured by the given strategy were unsuccessful,
 * the future reference is completed with a Throwable.
 *
 * Should not be used directly for most use cases.
 *
 * @tparam T Type of the message to send.
 * @param message The message to send to the given actor. This message will be wrapped into an ExpectingResponse message by the `expectingResponseMaker` function.
 * @param connection The reference to the MongoConnection the given message will be sent to.
 * @param strategy The Failover strategy.
 * @param expectingResponseMaker A function that takes a message of type `T` and wraps it into an ExpectingResponse message.
 */
class Failover[T](message: T, connection: MongoConnection, strategy: FailoverStrategy)(expectingResponseMaker: T => ExpectingResponse)(implicit ec: ExecutionContext) {
  import Failover2.logger
  import reactivemongo.core.errors._
  import reactivemongo.core.actors.Exceptions._

  private val promise = Promise[Response]()

  /**
   * A future that is completed with a response,
   * after 1 or more attempts (specified in the given strategy).
   */
  val future: Future[Response] = promise.future

  private def send(n: Int) {
    val expectingResponse = expectingResponseMaker(message)
    connection.mongosystem ! expectingResponse
    expectingResponse.future.onComplete {
      case Failure(e) if isRetryable(e) =>
        if (n < strategy.retries) {
          val `try` = n + 1
          val delayFactor = strategy.delayFactor(`try`)
          val delay = Duration.unapply(strategy.initialDelay * delayFactor).map(t => FiniteDuration(t._1, t._2)).getOrElse(strategy.initialDelay)

          logger.debug(s"Got an error, retrying... (try #${`try`} is scheduled in ${delay.toMillis} ms)", e)

          connection.actorSystem.scheduler.scheduleOnce(delay)(send(`try`))
        } else {
          // generally that means that the primary is not available or the nodeset is unreachable
          logger.error("Got an error, no more attempts to do. Completing with a failure...", e)
          promise.failure(e)
        }

      case Failure(e) => {
        logger.trace(
          "Got an non retryable error, completing with a failure...", e)
        promise.failure(e)
      }

      case Success(response) => {
        logger.trace("Got a successful result, completing...")
        promise.success(response)
      }
    }
  }

  private def isRetryable(throwable: Throwable) = throwable match {
    case e: ChannelNotFound             => e.retriable
    case _: PrimaryUnavailableException => true
    case _: NodeSetNotReachable         => true
    case _: ConnectionException         => true
    case _: ConnectionNotInitialized    => true
    case e: DatabaseException =>
      e.isNotAPrimaryError || e.isUnauthorized

    case _ => false
  }

  send(0)
}

@deprecated(message = "Will be made private", since = "0.11.11")
class Failover2[A](producer: () => Future[A], connection: MongoConnection, strategy: FailoverStrategy)(implicit ec: ExecutionContext) {
  import Failover2.logger, logger.trace
  import reactivemongo.core.errors._
  import reactivemongo.core.actors.Exceptions._

  // TODO: Pass an explicit stack trace, to be able to raise with possible err

  private val lnm = s"${connection.supervisor}/${connection.name}" // log name

  /**
   * A future that is completed with a response,
   * after 1 or more attempts (specified in the given strategy).
   */
  val future: Future[A] = send(0) //promise.future

  // Wraps any exception from the producer
  // as a result Future.failed that can be recovered.
  private def next(): Future[A] = try {
    producer()
  } catch {
    case producerErr: Throwable => Future.failed[A](producerErr)
  }

  private def send(n: Int): Future[A] =
    next().map[Try[A]](Success(_)).recover[Try[A]] {
      case err => Failure(err)
    }.flatMap {
      case Failure(e) if isRetryable(e) => {
        if (n < strategy.retries) {
          val `try` = n + 1
          val delayFactor = strategy.delayFactor(`try`)
          val delay = Duration.unapply(strategy.initialDelay * delayFactor).
            fold(strategy.initialDelay)(t => FiniteDuration(t._1, t._2))

          trace(s"[$lnm] Got an error, retrying... (try #${`try`} is scheduled in ${delay.toMillis} ms)", e)

          after(delay, connection.actorSystem.scheduler)(send(`try`))
        } else {
          // generally that means that the primary is not available
          // or the nodeset is unreachable
          logger.error(s"[$lnm] Got an error, no more attempts to do. Completing with a failure... ", e)

          Future.failed(e)
        }
      }

      case Failure(e) => {
        trace(s"[$lnm] Got an non retryable error, completing with a failure... ", e)
        Future.failed(e)
      }

      case Success(response) => {
        trace(s"[$lnm] Got a successful result, completing...")
        Future.successful(response)
      }
    }

  private def isRetryable(throwable: Throwable) = throwable match {
    case e: ChannelNotFound             => e.retriable
    case _: PrimaryUnavailableException => true
    case _: NodeSetNotReachable         => true
    case _: ConnectionException         => true
    case _: ConnectionNotInitialized    => true
    case e: DatabaseException =>
      e.isNotAPrimaryError || e.isUnauthorized

    case _ => false
  }

  //send(0)
}

object Failover2 {
  private[api] val logger = LazyLogger("reactivemongo.api.Failover2")

  def apply[A](connection: MongoConnection, strategy: FailoverStrategy)(producer: () => Future[A])(implicit ec: ExecutionContext): Failover2[A] =
    new Failover2(producer, connection, strategy)
}

@deprecated(message = "Unused", since = "0.11.10")
object Failover {
  /**
   * Produces a [[reactivemongo.api.Failover]] holding a future reference that is completed with a result, after 1 or more attempts (specified in the given strategy).
   *
   * @param checkedWriteRequest The checkedWriteRequest to send to the given actor.
   * @param connection The reference to the MongoConnection the given message will be sent to.
   * @param strategy The Failover strategy.
   */
  @deprecated(message = "Unused", since = "0.11.10")
  def apply(checkedWriteRequest: CheckedWriteRequest, connection: MongoConnection, strategy: FailoverStrategy)(implicit ec: ExecutionContext): Failover[CheckedWriteRequest] =
    new Failover(checkedWriteRequest, connection, strategy)(CheckedWriteRequestExpectingResponse.apply)

  /**
   * Produces a [[reactivemongo.api.Failover]] holding a future reference that is completed with a result, after 1 or more attempts (specified in the given strategy).
   *
   * @param requestMaker The requestMaker to send to the given actor.
   * @param connection The reference to the MongoConnection actor the given message will be sent to.
   * @param strategy The Failover strategy.
   */
  @deprecated(message = "Unused", since = "0.11.10")
  def apply(requestMaker: RequestMaker, connection: MongoConnection, strategy: FailoverStrategy)(implicit ec: ExecutionContext): Failover[RequestMaker] =
    new Failover(requestMaker, connection, strategy)(RequestMakerExpectingResponse(_, false))
}
