package reactivemongo.api

import scala.collection.Factory
import scala.concurrent.{ ExecutionContext, Future }

private[api] trait FlattenedCursorCompat[T] { _: FlattenedCursor[T] =>

  def collect[M[_]](maxDocs: Int, err: Cursor.ErrorHandler[M[T]])(implicit cbf: Factory[T, M[T]], ec: ExecutionContext): Future[M[T]] = cursor.flatMap(_.collect[M](maxDocs, err))

}
