package reactivemongo.core

import scala.util.{ Failure, Success, Try }

import scala.concurrent.Future
import scala.concurrent.duration.{ Duration, FiniteDuration }

import reactivemongo.actors.actor.ActorSystem
import reactivemongo.util.sameThreadExecutionContext

private[reactivemongo] sealed trait SystemControl

private[reactivemongo] case class TimedSystemControl(
    close: Option[FiniteDuration] => Try[Unit])
    extends SystemControl

private[reactivemongo] case class AsyncSystemControl(
    close: () => Future[Unit])
    extends SystemControl

private[reactivemongo] object SystemControl {
  import scala.language.reflectiveCalls

  /** Default timeout (10s) */
  private val defaultTimeout = FiniteDuration(10, "seconds")

  private type Legacy = {
    def isTerminated: Boolean
    def awaitTermination(timeout: Duration): Unit
    def shutdown(): Unit
  }

  private type Modern = {
    def terminate(): Future[Any]
  }

  @SuppressWarnings(Array("AsInstanceOf"))
  def apply(underlying: ActorSystem): Try[SystemControl] =
    Try {
      val legacy = underlying.asInstanceOf[Legacy]

      legacy.isTerminated

      legacyControl(legacy)
    }.recoverWith {
      case _: NoSuchMethodException =>
        Try(modernControl(underlying.asInstanceOf[Modern]))
    }

  // ---

  private def legacyControl(system: Legacy) = TimedSystemControl { timeout =>
    if (system.isTerminated) Success({})
    else {
      val time = timeout.getOrElse(defaultTimeout)

      // When the actorSystem is shutdown,
      // it means that supervisorActor has exited (run its postStop).
      // So, wait for that event.

      Try {
        system.shutdown()
        system.awaitTermination(time)
      }.recoverWith {
        case e: Throwable if !system.isTerminated =>
          Failure(e)

        case _: Exception => Success({})
      }
    }
  }

  private def modernControl(system: Modern) = AsyncSystemControl { () =>
    system.terminate().map(_ => {})(sameThreadExecutionContext)
  }
}
