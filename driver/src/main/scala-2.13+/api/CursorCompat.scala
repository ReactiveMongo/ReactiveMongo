package reactivemongo.api

import scala.collection.Factory

import scala.concurrent.{ ExecutionContext, Future }

private[api] trait CursorCompat[T] { _: Cursor[T] with CursorCompatAPI[T] =>
  import Cursor.{ Cont, ErrorHandler }

  def collect[M[_]](maxDocs: Int, err: ErrorHandler[M[T]])(implicit cbf: Factory[T, M[T]], ec: ExecutionContext): Future[M[T]] =
    foldWhile(cbf.newBuilder, maxDocs)(
      { (builder, a) => Cont(builder += a) },
      { (b, t: Throwable) =>
        err(b.result(), t).map(_ => b)
      }).map(_.result())
}
