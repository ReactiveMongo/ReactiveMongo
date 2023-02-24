package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }

/**
 * Cursor wrapper, to help to define custom cursor classes.
 * @see CursorProducer
 */
trait WrappedCursor[T] extends Cursor[T] with WrappedCursorCompat[T] {

  /** The underlying cursor */
  protected def wrappee: Cursor[T]

  final def foldBulks[A](
      z: => A,
      maxDocs: Int = -1
    )(suc: (A, Iterator[T]) => Cursor.State[A],
      err: Cursor.ErrorHandler[A]
    )(implicit
      ec: ExecutionContext
    ): Future[A] = wrappee.foldBulks(z, maxDocs)(suc, err)

  final def foldBulksM[A](
      z: => A,
      maxDocs: Int = -1
    )(suc: (A, Iterator[T]) => Future[Cursor.State[A]],
      err: Cursor.ErrorHandler[A]
    )(implicit
      ec: ExecutionContext
    ): Future[A] = wrappee.foldBulksM(z, maxDocs)(suc, err)

  final def foldWhile[A](
      z: => A,
      maxDocs: Int = -1
    )(suc: (A, T) => Cursor.State[A],
      err: Cursor.ErrorHandler[A]
    )(implicit
      ec: ExecutionContext
    ): Future[A] = wrappee.foldWhile(z, maxDocs)(suc, err)

  final def foldWhileM[A](
      z: => A,
      maxDocs: Int = -1
    )(suc: (A, T) => Future[Cursor.State[A]],
      err: Cursor.ErrorHandler[A]
    )(implicit
      ec: ExecutionContext
    ): Future[A] = wrappee.foldWhileM(z, maxDocs)(suc, err)

  final def head(
      implicit
      ec: ExecutionContext
    ): Future[T] = wrappee.head

  final def headOption(
      implicit
      ec: ExecutionContext
    ): Future[Option[T]] =
    wrappee.headOption
}
