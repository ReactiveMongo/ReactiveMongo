package reactivemongo.api

import scala.language.higherKinds

import scala.collection.generic.CanBuildFrom

import scala.collection.mutable.Builder

import scala.concurrent.{ ExecutionContext, Future }

private[api] trait CursorCompat[T] { _: Cursor[T] with CursorCompatAPI[T] =>
  import Cursor.{ Cont, ErrorHandler }

  def collect[M[_]](maxDocs: Int, err: ErrorHandler[M[T]])(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext): Future[M[T]] =
    foldWhile[Builder[T, M[T]]](cbf(), maxDocs)(
      { (builder, a) => Cont(builder += a) },
      { (b: Builder[T, M[T]], t: Throwable) =>
        err(b.result(), t).map[Builder[T, M[T]]](_ => b)
      }).map(_.result())
}
