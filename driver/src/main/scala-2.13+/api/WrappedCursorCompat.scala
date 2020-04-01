package reactivemongo.api

import scala.collection.Factory
import scala.concurrent.{ ExecutionContext, Future }

private[api] trait WrappedCursorCompat[T] { _: WrappedCursor[T] =>
  def collect[M[_]](maxDocs: Int, err: Cursor.ErrorHandler[M[T]])(implicit cbf: Factory[T, M[T]], ec: ExecutionContext): Future[M[T]] = wrappee.collect[M](maxDocs, err)

  def peek[M[_]](maxDocs: Int)(implicit cbf: Factory[T, M[T]], ec: ExecutionContext): Future[Cursor.Result[M[T]]] = wrappee.peek(maxDocs)
}
