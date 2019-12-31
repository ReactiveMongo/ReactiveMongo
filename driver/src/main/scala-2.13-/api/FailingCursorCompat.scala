package reactivemongo.api

import scala.language.higherKinds

import scala.concurrent.{ ExecutionContext, Future }

import scala.collection.generic.CanBuildFrom

private[api] trait FailingCursorCompat[T] { cursor: FailingCursor[T] =>
  def collect[M[_]](maxDocs: Int, err: Cursor.ErrorHandler[M[T]])(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext): Future[M[T]] = failure
}
