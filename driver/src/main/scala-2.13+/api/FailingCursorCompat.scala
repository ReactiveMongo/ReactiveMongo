package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }

import scala.collection.Factory

private[api] trait FailingCursorCompat[T] { cursor: FailingCursor[T] =>
  def collect[M[_]](maxDocs: Int, err: Cursor.ErrorHandler[M[T]])(implicit cbf: Factory[T, M[T]], ec: ExecutionContext): Future[M[T]] = failure

  def peek[M[_]](maxDocs: Int)(implicit cbf: Factory[T, M[T]], ec: ExecutionContext): Future[Cursor.Result[M[T]]] = failure
}
