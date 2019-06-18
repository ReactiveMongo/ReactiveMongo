package reactivemongo.api

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.protocol.Response

class FlattenedCursor[T](protected val cursor: Future[Cursor[T]])
  extends Cursor[T] with FlattenedCursorCompat[T] {

  def head(implicit @deprecatedName('ctx) ec: ExecutionContext): Future[T] = cursor.flatMap(_.head)

  override def headOption(implicit @deprecatedName('ctx) ec: ExecutionContext): Future[Option[T]] =
    cursor.flatMap(_.headOption)

  def foldResponses[A](z: => A, maxDocs: Int = -1)(suc: (A, Response) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit @deprecatedName('ctx) ec: ExecutionContext): Future[A] = cursor.flatMap(_.foldResponses(z, maxDocs)(suc, err))

  def foldResponsesM[A](z: => A, maxDocs: Int = -1)(suc: (A, Response) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit @deprecatedName('ctx) ec: ExecutionContext): Future[A] = cursor.flatMap(_.foldResponsesM(z, maxDocs)(suc, err))

  def foldBulks[A](z: => A, maxDocs: Int = -1)(suc: (A, Iterator[T]) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit @deprecatedName('ctx) ec: ExecutionContext): Future[A] = cursor.flatMap(_.foldBulks(z, maxDocs)(suc, err))

  def foldBulksM[A](z: => A, maxDocs: Int = -1)(suc: (A, Iterator[T]) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit @deprecatedName('ctx) ec: ExecutionContext): Future[A] = cursor.flatMap(_.foldBulksM(z, maxDocs)(suc, err))

  def foldWhile[A](z: => A, maxDocs: Int = -1)(suc: (A, T) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit @deprecatedName('ctx) ec: ExecutionContext): Future[A] = cursor.flatMap(_.foldWhile(z, maxDocs)(suc, err))

  def foldWhileM[A](z: => A, maxDocs: Int = -1)(suc: (A, T) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit @deprecatedName('ctx) ec: ExecutionContext): Future[A] = cursor.flatMap(_.foldWhileM(z, maxDocs)(suc, err))

}
