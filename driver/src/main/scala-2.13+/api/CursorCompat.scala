package reactivemongo.api

import scala.collection.Factory

import scala.concurrent.{ ExecutionContext, Future }

private[api] trait CursorCompat[T] {
  _self: DefaultCursor.Impl[T] with CursorCompatAPI[T] =>

  import Cursor.{ Cont, ErrorHandler }

  def collect[M[_]](
      maxDocs: Int,
      err: ErrorHandler[M[T]]
    )(implicit
      cbf: Factory[T, M[T]],
      ec: ExecutionContext
    ): Future[M[T]] = {
    if (maxDocs == 0 || maxDocs < -1) {
      Future(cbf.newBuilder.result())
    } else {
      foldWhile(cbf.newBuilder, maxDocs)(
        { (builder, a) => Cont(builder += a) },
        { (b, t: Throwable) => err(b.result(), t).map(_ => b) }
      ).map(_.result())
    }
  }

  override def peek[M[_]](
      maxDocs: Int
    )(implicit
      cbf: Factory[T, M[T]],
      ec: ExecutionContext
    ): Future[Cursor.Result[M[T]]] = {
    if (maxDocs == 0 || maxDocs < -1) {
      def ref = new Cursor.Reference(
        collectionName = fullCollectionName,
        cursorId = 0,
        numberToReturn = 0,
        tailable = this.tailable,
        pinnedNode = None
      )

      Future(new Cursor.Result[M[T]](cbf.newBuilder.result(), ref))
    } else {
      makeRequest(maxDocs).map { resp =>
        def builder =
          documentIterator(resp).foldLeft(cbf.newBuilder) { _ += _ }

        val ref = new Cursor.Reference(
          collectionName = fullCollectionName,
          cursorId = resp.reply.cursorID,
          numberToReturn = this.numberToReturn,
          tailable = this.tailable,
          pinnedNode = transaction.flatMap(_.pinnedNode)
        )

        new Cursor.Result[M[T]](builder.result(), ref)
      }
    }
  }

}
