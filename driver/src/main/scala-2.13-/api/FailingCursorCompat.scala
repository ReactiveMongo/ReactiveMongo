package reactivemongo.api

import scala.collection.generic.CanBuildFrom

import scala.concurrent.{ ExecutionContext, Future }

private[api] trait FailingCursorCompat[T] { cursor: FailingCursor[T] =>

  @annotation.nowarn
  def collect[M[_]](
      maxDocs: Int,
      err: Cursor.ErrorHandler[M[T]]
    )(implicit
      cbf: CanBuildFrom[M[_], T, M[T]],
      ec: ExecutionContext
    ): Future[M[T]] = failure

  @annotation.nowarn
  def peek[M[_]](
      maxDocs: Int
    )(implicit
      cbf: CanBuildFrom[M[_], T, M[T]],
      ec: ExecutionContext
    ): Future[Cursor.Result[M[T]]] = failure
}
