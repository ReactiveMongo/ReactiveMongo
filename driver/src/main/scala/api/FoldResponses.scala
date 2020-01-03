package reactivemongo.api

import scala.util.{ Failure, Success }

import scala.util.control.NonFatal

import scala.concurrent.{ ExecutionContext, Future }

import akka.actor.ActorSystem

import reactivemongo.core.protocol.Response

private[api] final class FoldResponses[T](
  nextResponse: (ExecutionContext, Response) => Future[Option[Response]],
  killCursors: (Long, String) => Unit,
  maxDocs: Int,
  suc: (T, Response) => Future[Cursor.State[T]],
  err: Cursor.ErrorHandler[T])(implicit actorSys: ActorSystem, ec: ExecutionContext) { self =>
  import Cursor.{ Cont, Done, Fail, State, logger }
  import CursorOps.Unrecoverable

  private val promise = scala.concurrent.Promise[T]()
  lazy val result: Future[T] = promise.future

  private val handle: Any => Unit = {
    case ProcResponses(makeReq, cur, c, id) =>
      procResponses(makeReq(), cur, c, id)

    case HandleResponse(last, cur, c) =>
      handleResponse(last, cur, c)

    case ProcNext(last, cur, next, c) =>
      procNext(last, cur, next, c)

    case OnError(last, cur, error, c) =>
      onError(last, cur, error, c)
  }

  @inline private def kill(cursorID: Long): Unit = try {
    killCursors(cursorID, "FoldResponses")
  } catch {
    case NonFatal(cause) =>
      logger.warn(s"fails to kill cursor: $cursorID", cause)
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

    val handled: Future[State[T]] = try {
      suc(cur, last)
    } catch {
      case unsafe: Exception /* see makeIterator */ =>
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
      case Unrecoverable(e) =>
        ko(last, e) // already marked recoverable

      case _ => err(cur, error) match {
        case Done(d) => ok(last, d)

        case Fail(f) => ko(last, f)

        case next @ Cont(v) =>
          self ! ProcNext(last, v /*cur*/ , next, c)
      }
    }

  @inline private def fetch(
    c: Int,
    ec: ExecutionContext,
    r: Response): Future[Option[Response]] = {
    // Enforce maxDocs check as r.reply.startingFrom (checked in hasNext),
    // will always be set to 0 by the server for tailable cursor/capped coll
    if (c < maxDocs) {
      // nextResponse will take care of cursorID, ...
      nextResponse(ec, r)
    } else {
      Future.successful(Option.empty[Response])
    }
  }

  @inline private def procNext(last: Response, cur: T, next: State[T], c: Int): Unit = next match {
    case Done(d) => ok(last, d)

    case Fail(f) => self ! OnError(last, cur, f, c)

    case Cont(v) => fetch(c, ec, last).onComplete({
      case Success(Some(r)) => self ! ProcResponses(
        () => Future.successful(r), v, c, r.reply.cursorID)

      case Success(_) => ok(last, v)
      case Failure(e) => ko(last, e)
    })(ec)
  }

  @inline private def procResponses(last: Future[Response], cur: T, c: Int, lastID: Long): Unit = last.onComplete({
    case Success(r) => self ! HandleResponse(r, cur, c)

    case Failure(error) => {
      logger.error("fails to send request", error)

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
      }
    }
  })(ec)

  /**
   * Enqueues a `message` to be processed while fold the cursor results.
   */
  def !(message: Any): Unit = {
    actorSys.scheduler.scheduleOnce(
      // TODO#1.1: on retry, add some delay according FailoverStrategy
      scala.concurrent.duration.Duration.Zero)(handle(message))(ec)

    ()
  }

  // Messages

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

  /**
   * @param last $lastParam
   * @param cur $curParam
   * @param c $cParam
   */
  private case class HandleResponse(last: Response, cur: T, c: Int)

  /**
   * @param last $lastParam
   * @param cur $curParam
   * @param next the next state
   * @param c $cParam
   */
  private case class ProcNext(last: Response, cur: T, next: State[T], c: Int)

  /**
   * @param last $lastParam
   * @param cur $curParam
   * @param error the error details
   * @param c $cParam
   */
  private case class OnError(last: Response, cur: T, error: Throwable, c: Int)
}

private[api] object FoldResponses {
  def apply[T](
    z: => T,
    makeRequest: ExecutionContext => Future[Response],
    nextResponse: (ExecutionContext, Response) => Future[Option[Response]],
    killCursors: (Long, String) => Unit,
    suc: (T, Response) => Future[Cursor.State[T]],
    err: Cursor.ErrorHandler[T],
    maxDocs: Int)(implicit actorSys: ActorSystem, ec: ExecutionContext): Future[T] = {
    Future(z)(ec).flatMap({ v =>
      val max = if (maxDocs > 0) maxDocs else Int.MaxValue
      val f = new FoldResponses[T](
        nextResponse, killCursors, max, suc, err)(actorSys, ec)

      f ! f.ProcResponses(() => makeRequest(ec), v, 0, -1L)

      f.result
    })(ec)
  }
}
