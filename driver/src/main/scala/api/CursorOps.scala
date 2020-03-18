package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.protocol.Response

/** Internal cursor operations. */
trait CursorOps[T] { cursor: Cursor[T] =>
  /** Sends the initial request. */
  private[reactivemongo] def makeRequest(maxDocs: Int)(implicit ec: ExecutionContext): Future[Response]

  /**
   * Returns a function that can be used to get the next response,
   * if allowed according the `maxDocs` and the cursor options
   * (cursor not exhausted, tailable, ...)
   */
  private[reactivemongo] def nextResponse(maxDocs: Int): (ExecutionContext, Response) => Future[Option[Response]]

  /**
   * Returns an iterator to read the response documents,
   * according the provided read for the element type `T`.
   */
  private[reactivemongo] def documentIterator(response: Response): Iterator[T]

  /**
   * Kills the server resources associated with the specified cursor.
   *
   * @param id the cursor ID
   */
  private[reactivemongo] def killCursor(id: Long)(implicit ec: ExecutionContext): Unit

  /** Indicates whether the underlying cursor is [[https://docs.mongodb.com/manual/core/tailable-cursors/ tailable]]. */
  def tailable: Boolean

  /** The underlying connection */
  def connection: MongoConnection

  /** The strategy to failover the cursor operations */
  def failoverStrategy: FailoverStrategy
}

object CursorOps {
  /**
   * Wraps exception that has already been passed to the current error handler
   * and should not be recovered.
   */
  private[reactivemongo] case class Unrecoverable(cause: Throwable)
    extends scala.RuntimeException(cause)
    with scala.util.control.NoStackTrace

}
