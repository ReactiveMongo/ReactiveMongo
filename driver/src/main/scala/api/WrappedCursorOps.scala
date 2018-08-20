package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.protocol.Response

/** Internal cursor operations. */
trait WrappedCursorOps[T] extends CursorOps[T] { cursor: Cursor[T] =>
  /** The underlying cursor ops */
  protected def opsWrappee: CursorOps[T]

  private[reactivemongo] def makeRequest(maxDocs: Int)(implicit ec: ExecutionContext): Future[Response] = opsWrappee.makeRequest(maxDocs)

  private[reactivemongo] def nextResponse(maxDocs: Int): (ExecutionContext, Response) => Future[Option[Response]] = opsWrappee.nextResponse(maxDocs)

  private[reactivemongo] def documentIterator(response: Response): Iterator[T] =
    opsWrappee.documentIterator(response)

  @deprecated("Use `killCursor`", "0.16.0")
  def kill(cursorID: Long): Unit = opsWrappee.kill(cursorID)

  private[reactivemongo] def killCursor(id: Long)(implicit ec: ExecutionContext): Unit = opsWrappee.killCursor(id)

  def tailable: Boolean = opsWrappee.tailable

  def connection: MongoConnection = opsWrappee.connection

  def failoverStrategy: FailoverStrategy = opsWrappee.failoverStrategy
}
