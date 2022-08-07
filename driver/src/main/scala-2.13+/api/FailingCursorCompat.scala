package reactivemongo.api

import scala.annotation.unused

import scala.concurrent.{ ExecutionContext, Future }

import scala.collection.Factory

private[api] trait FailingCursorCompat[T] { cursor: FailingCursor[T] =>

  def collect[M[_]](
      @unused maxDocs: Int,
      @unused err: Cursor.ErrorHandler[M[T]]
    )(implicit
      @unused cbf: Factory[T, M[T]],
      @unused ec: ExecutionContext
    ): Future[M[T]] = failure

  def peek[M[_]](
      @unused maxDocs: Int
    )(implicit
      @unused cbf: Factory[T, M[T]],
      @unused ec: ExecutionContext
    ): Future[Cursor.Result[M[T]]] = failure
}
