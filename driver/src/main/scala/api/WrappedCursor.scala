package reactivemongo.api

import scala.language.higherKinds

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.protocol.Response

/**
 * Cursor wrapper, to help to define custom cursor classes.
 * @see CursorProducer
 */
trait WrappedCursor[T] extends Cursor[T] {
  /** The underlying cursor */
  def wrappee: Cursor[T]

  def collect[M[_]](maxDocs: Int, err: Cursor.ErrorHandler[M[T]])(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext): Future[M[T]] = wrappee.collect[M](maxDocs, err)

  def foldResponses[A](z: => A, maxDocs: Int = -1)(suc: (A, Response) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = wrappee.foldResponses(z, maxDocs)(suc, err)

  def foldResponsesM[A](z: => A, maxDocs: Int = -1)(suc: (A, Response) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = wrappee.foldResponsesM(z, maxDocs)(suc, err)

  def foldBulks[A](z: => A, maxDocs: Int = -1)(suc: (A, Iterator[T]) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = wrappee.foldBulks(z, maxDocs)(suc, err)

  def foldBulksM[A](z: => A, maxDocs: Int = -1)(suc: (A, Iterator[T]) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = wrappee.foldBulksM(z, maxDocs)(suc, err)

  def foldWhile[A](z: => A, maxDocs: Int = -1)(suc: (A, T) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = wrappee.foldWhile(z, maxDocs)(suc, err)

  def foldWhileM[A](z: => A, maxDocs: Int = -1)(suc: (A, T) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = wrappee.foldWhileM(z, maxDocs)(suc, err)

  def head(implicit ctx: ExecutionContext): Future[T] = wrappee.head

  // TODO: Remove override
  override def headOption(implicit ctx: ExecutionContext): Future[Option[T]] =
    wrappee.headOption
}
