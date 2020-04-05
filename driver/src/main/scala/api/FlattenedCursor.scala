package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }

class FlattenedCursor[T](
  protected val cursor: Future[Cursor[T]])
  extends Cursor[T] with FlattenedCursorCompat[T] {

  final def head(implicit ec: ExecutionContext): Future[T] = cursor.flatMap(_.head)

  final override def headOption(implicit ec: ExecutionContext): Future[Option[T]] = cursor.flatMap(_.headOption)

  final def foldBulks[A](z: => A, maxDocs: Int = -1)(suc: (A, Iterator[T]) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ec: ExecutionContext): Future[A] = cursor.flatMap(_.foldBulks(z, maxDocs)(suc, err))

  final def foldBulksM[A](z: => A, maxDocs: Int = -1)(suc: (A, Iterator[T]) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ec: ExecutionContext): Future[A] = cursor.flatMap(_.foldBulksM(z, maxDocs)(suc, err))

  final def foldWhile[A](z: => A, maxDocs: Int = -1)(suc: (A, T) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ec: ExecutionContext): Future[A] = cursor.flatMap(_.foldWhile(z, maxDocs)(suc, err))

  final def foldWhileM[A](z: => A, maxDocs: Int = -1)(suc: (A, T) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ec: ExecutionContext): Future[A] = cursor.flatMap(_.foldWhileM(z, maxDocs)(suc, err))

}
