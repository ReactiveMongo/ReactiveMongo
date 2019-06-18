package reactivemongo.api

import scala.language.higherKinds

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{ ExecutionContext, Future }

private[api] trait FlattenedCursorCompat[T] { _: FlattenedCursor[T] =>

  final def collect[M[_]](maxDocs: Int, err: Cursor.ErrorHandler[M[T]])(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext): Future[M[T]] = cursor.flatMap(_.collect[M](maxDocs, err))

}
