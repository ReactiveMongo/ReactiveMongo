package reactivemongo.api

import scala.collection.generic.CanBuildFrom

import scala.concurrent.{ ExecutionContext, Future }

import com.github.ghik.silencer.silent

private[api] trait FailingCursorCompat[T] { cursor: FailingCursor[T] =>

  @silent(".*\\ (maxDocs|ec|err|cbf).*\\ never\\ used.*")
  def collect[M[_]](
      maxDocs: Int,
      err: Cursor.ErrorHandler[M[T]]
    )(implicit
      cbf: CanBuildFrom[M[_], T, M[T]],
      ec: ExecutionContext
    ): Future[M[T]] = failure

  @silent(".*\\ (maxDocs|ec|cbf).*\\ never\\ used.*")
  def peek[M[_]](
      maxDocs: Int
    )(implicit
      cbf: CanBuildFrom[M[_], T, M[T]],
      ec: ExecutionContext
    ): Future[Cursor.Result[M[T]]] = failure
}
