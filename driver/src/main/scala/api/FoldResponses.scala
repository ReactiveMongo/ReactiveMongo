package reactivemongo.api

import scala.util.{ Failure, Success }
import scala.util.control.NonFatal

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.{ Duration, FiniteDuration }

import reactivemongo.core.protocol.Response

import reactivemongo.actors.actor.ActorSystem

private[api] final class FoldResponses[T](
    failoverStrategy: FailoverStrategy,
    nextResponse: (ExecutionContext, Response) => Future[Option[Response]],
    killCursors: (Long, String) => Unit,
    maxDocs: Int,
    suc: (T, Response) => Future[Cursor.State[T]],
    err: Cursor.ErrorHandler[T]
  )(implicit
    actorSys: ActorSystem,
    ec: ExecutionContext) { self =>
  import Cursor.{ Cont, Done, Fail, State, logger }
  import CursorOps.UnrecoverableException

  private val promise = scala.concurrent.Promise[T]()
  lazy val result: Future[T] = promise.future

  private val handle: Msg => Unit = {
    case ProcResponses(makeReq, cur, c, id) =>
      procResponses(makeReq(), cur, c, id)

    case HandleResponse(last, cur, c) =>
      handleResponse(last, cur, c)

    case ProcNext(last, cur, next, c) =>
      procNext(last, cur, next, c)

    case OnError(last, cur, error, c) =>
      onError(last, cur, error, c)

    case s =>
      logger.warn(s"unexpected fold message: $s")
  }

  @inline private def kill(cursorID: Long): Unit =
    try {
      killCursors(cursorID, "FoldResponses")
    } catch {
      case NonFatal(cause) =>
        logger.warn(s"Fails to kill cursor: $cursorID", cause)
    }

  @inline private def ok(r: Response, v: T): Unit = {
    kill(r.reply.cursorID) // Releases cursor before ending
    promise.success(v)
    ()
  }

  @inline private def ko(r: Response, f: Throwable): Unit = {
    kill(r.reply.cursorID) // Releases cursor before ending
    promise.failure(f)
    ()
  }

  @inline private def handleResponse(last: Response, cur: T, c: Int): Unit = {
    logger.trace(s"Process response: $last")

    val handled: Future[State[T]] =
      try {
        suc(cur, last)
      } catch {
        case NonFatal(unsafe) /* see makeIterator */ =>
          Future.failed[State[T]](unsafe)
      }

    def nc = c + last.reply.numberReturned

    handled.onComplete({
      case Success(next)  => self ! ProcNext(last, cur, next, nc)
      case Failure(error) => self ! OnError(last, cur, error, nc)
    })(ec)
  }

  @inline
  private def onError(last: Response, cur: T, error: Throwable, c: Int): Unit =
    error match {
      case UnrecoverableException(e) =>
        ko(last, e) // already marked recoverable

      case _ =>
        err(cur, error) match {
          case Done(d) => ok(last, d)

          case Fail(f) => ko(last, f)

          case next @ Cont(v) =>
            self ! ProcNext(last, v /*cur*/, next, c)

          case _ =>
            ko(last, error)
        }
    }

  @inline private def fetch(
      c: Int,
      fec: ExecutionContext,
      fr: Response
    ): Future[Option[Response]] = {
    // Enforce maxDocs check as r.reply.startingFrom (checked in hasNext),
    // will always be set to 0 by the server for tailable cursor/capped coll
    if (c < maxDocs) {
      // nextResponse will take care of cursorID, ...
      nextResponse(fec, fr)
    } else {
      Future.successful(Option.empty[Response])
    }
  }

  @inline private def procNext(
      last: Response,
      cur: T,
      next: State[T],
      c: Int
    ): Unit = next match {
    case Done(d) => ok(last, d)

    case Fail(f) => self ! OnError(last, cur, f, c)

    case Cont(v) =>
      fetch(c, ec, last).onComplete({
        case Success(Some(r)) =>
          self ! ProcResponses(
            () => Future.successful(r),
            v,
            c,
            r.reply.cursorID
          )

        case Success(_) => ok(last, v)
        case Failure(e) => ko(last, e)
      })(ec)

    case s =>
      logger.warn(s"Unexpected cursor state: $s")
  }

  @inline private def procResponses(
      last: Future[Response],
      cur: T,
      c: Int,
      lastID: Long
    ): Unit = last.onComplete({
    case Success(r) => self ! HandleResponse(r, cur, c)

    case Failure(error) => {
      logger.error("Fails to send request", error)

      err(cur, error) match {
        case Done(v) => {
          if (lastID > 0) kill(lastID)

          promise.success(v)
        }

        case Fail(e) => {
          if (lastID > 0) kill(lastID)

          promise.failure(e)
        }

        case Cont(v) => {
          logger.warn("cannot continue after fatal request error", error)

          promise.success(v)
        }

        case _ => { // Should not happen
          if (lastID > 0) kill(lastID)

          promise.failure(error)
        }
      }
    }
  })(ec)

  /**
   * Enqueues a `message` to be processed while fold the cursor results.
   */
  private def ![M <: Msg](
      message: M
    )(implicit
      delay: Delay.Aux[M]
    ): Unit = {
    actorSys.scheduler.scheduleOnce(delay.value)(handle(message))(ec)

    ()
  }

  // Messages
  private[api] trait Msg

  /**
   * @param requester the function the perform the next request
   * @param cur $curParam
   * @param c $cParam
   * @param lastID the last ID for the cursor (or `-1` if unknown)
   */
  private[api] case class ProcResponses(
      requester: () => Future[Response],
      cur: T,
      c: Int,
      lastID: Long)
      extends Msg

  /**
   * @param last $lastParam
   * @param cur $curParam
   * @param c $cParam
   */
  private case class HandleResponse(last: Response, cur: T, c: Int) extends Msg

  /**
   * @param last $lastParam
   * @param cur $curParam
   * @param next the next state
   * @param c $cParam
   */
  private case class ProcNext(
      last: Response,
      cur: T,
      next: State[T],
      c: Int)
      extends Msg

  /**
   * @param last $lastParam
   * @param cur $curParam
   * @param error the error details
   * @param c $cParam
   */
  private case class OnError(
      last: Response,
      cur: T,
      error: Throwable,
      c: Int)
      extends Msg

  // ---

  private sealed trait Delay {
    type Message

    def value: FiniteDuration

    final override def equals(that: Any): Boolean = that match {
      case other: Delay =>
        this.value == other.value

      case _ => false
    }

    final override def hashCode: Int = value.hashCode

    final override def toString = s"Delay(${value.toString})"
  }

  private object Delay extends LowPriorityDelay {
    type Aux[M] = Delay { type Message = M }

    implicit val errorDelay: Delay.Aux[OnError] = new Delay {
      type Message = OnError
      val value = failoverStrategy.initialDelay
    }
  }

  private sealed trait LowPriorityDelay { _self: Delay.type =>

    private val unsafe = new Delay {
      type Message = Nothing
      val value = Duration.Zero
    }

    // @com.github.ghik.silencer.silent
    @SuppressWarnings(Array("AsInstanceOf"))
    implicit def defaultDelay[M]: Delay.Aux[M] =
      unsafe.asInstanceOf[Delay.Aux[M]]
  }
}

private[api] object FoldResponses {

  def apply[T](
      failoverStrategy: FailoverStrategy,
      z: => T,
      makeRequest: ExecutionContext => Future[Response],
      nextResponse: (ExecutionContext, Response) => Future[Option[Response]],
      killCursors: (Long, String) => Unit,
      suc: (T, Response) => Future[Cursor.State[T]],
      err: Cursor.ErrorHandler[T],
      maxDocs: Int
    )(implicit
      actorSys: ActorSystem,
      ec: ExecutionContext
    ): Future[T] = {
    Future(z)(ec).flatMap({ v =>
      val max = if (maxDocs > 0) maxDocs else Int.MaxValue
      val f = new FoldResponses[T](
        failoverStrategy,
        nextResponse,
        killCursors,
        max,
        suc,
        err
      )(actorSys, ec)

      f ! f.ProcResponses(() => makeRequest(ec), v, 0, -1L)

      f.result
    })(ec)
  }
}
