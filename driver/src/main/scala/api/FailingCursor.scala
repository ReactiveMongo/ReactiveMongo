package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.protocol.Response

private[api] final class FailingCursor[T](
  val connection: MongoConnection,
  cause: Exception)
  extends Cursor[T] with CursorOps[T] with FailingCursorCompat[T] {

  protected lazy val failure = Future.failed(cause)

  val failoverStrategy = FailoverStrategy.default

  private[reactivemongo] def documentIterator(response: Response): Iterator[T] = Iterator.empty

  def kill(cursorID: Long): Unit = ()

  private[reactivemongo] def killCursor(id: Long)(implicit ec: ExecutionContext): Unit = ()

  private[reactivemongo] def makeRequest(maxDocs: Int)(implicit ec: ExecutionContext): Future[Response] = failure

  private[reactivemongo] def nextResponse(maxDocs: Int): (ExecutionContext, Response) => Future[Option[Response]] = (_, _) => failure

  def tailable: Boolean = false

  def foldBulks[A](z: => A, maxDocs: Int)(suc: (A, Iterator[T]) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ec: ExecutionContext): Future[A] = failure

  def foldBulksM[A](z: => A, maxDocs: Int)(suc: (A, Iterator[T]) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ec: ExecutionContext): Future[A] = failure

  def foldResponses[A](z: => A, maxDocs: Int)(suc: (A, Response) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ec: ExecutionContext): Future[A] = failure

  def foldResponsesM[A](z: => A, maxDocs: Int)(suc: (A, Response) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ec: ExecutionContext): Future[A] = failure

  def foldWhile[A](z: => A, maxDocs: Int)(suc: (A, T) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ec: ExecutionContext): Future[A] = failure

  def foldWhileM[A](z: => A, maxDocs: Int)(suc: (A, T) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ec: ExecutionContext): Future[A] = failure

  def head(implicit ec: ExecutionContext): Future[T] = failure

  def headOption(implicit ec: ExecutionContext): Future[Option[T]] = failure

}

private[api] object FailingCursor {
  def apply[T](
    connection: MongoConnection,
    cause: Exception)(implicit cp: CursorProducer[T]): cp.ProducedCursor =
    cp.produce(new FailingCursor[T](connection, cause))
}
