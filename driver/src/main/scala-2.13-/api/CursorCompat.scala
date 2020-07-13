package reactivemongo.api

import scala.collection.generic.CanBuildFrom

import scala.collection.mutable.Builder

import scala.concurrent.{ ExecutionContext, Future }

private[api] trait CursorCompat[T] {
  _: DefaultCursor.Impl[T] with CursorCompatAPI[T] =>

  import Cursor.{ Cont, ErrorHandler }

  def collect[M[_]](maxDocs: Int, err: ErrorHandler[M[T]])(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext): Future[M[T]] = {
    if (maxDocs == 0 || maxDocs < -1) {
      Future(cbf().result())
    } else {
      foldWhile[Builder[T, M[T]]](cbf(), maxDocs)(
        { (builder, a) => Cont(builder += a) },
        { (b: Builder[T, M[T]], t: Throwable) =>
          err(b.result(), t).map[Builder[T, M[T]]](_ => b)
        }).map(_.result())
    }
  }

  override def peek[M[_]](maxDocs: Int)(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext): Future[Cursor.Result[M[T]]] = {
    if (maxDocs == 0 || maxDocs < -1) {
      def ref = new Cursor.Reference(
        collectionName = fullCollectionName,
        cursorId = 0,
        numberToReturn = 0,
        tailable = this.tailable,
        pinnedNode = None)

      Future(new Cursor.Result[M[T]](cbf().result(), ref))
    } else {
      makeRequest(maxDocs).map { resp =>
        def builder = documentIterator(resp).foldLeft(cbf()) { _ += _ }

        val ref = new Cursor.Reference(
          collectionName = fullCollectionName,
          cursorId = resp.reply.cursorID,
          numberToReturn = this.numberToReturn,
          tailable = this.tailable,
          pinnedNode = transaction.flatMap(_.pinnedNode))

        new Cursor.Result[M[T]](builder.result(), ref)
      }
    }
  }
}
