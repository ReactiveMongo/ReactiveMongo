package reactivemongo.api

import scala.collection.Factory

import scala.concurrent.{ ExecutionContext, Future }

/**
 * '''EXPERIMENTAL:''' Base class to implement test-only/mocked [[Cursor]].
 *
 * All functions failed future by default,
 * make sure to override the required functions with appropriate results
 * to execute the tests.
 *
 * {{{
 * import scala.concurrent.{ ExecutionContext, Future }
 *
 * import reactivemongo.api.{ Cursor, TestCursor }
 *
 * final class MyTestCursor[T](h: T) extends TestCursor[T] {
 *   override def head(implicit ctx: ExecutionContext): Future[T] =
 *     Future.successful(h)
 * }
 *
 * val cursor: Cursor[String] = new MyTestCursor("foo")
 *
 * def foo(implicit ec: ExecutionContext): Unit = {
 *   cursor.headOption // Future.failed by default
 *
 *   cursor.head // Future.successful("foo")
 *
 *   ()
 * }
 * }}}
 */
class TestCursor[T] extends Cursor[T] {
  import Cursor.NoSuchResultException

  def collect[M[_]](
      maxDocs: Int,
      err: Cursor.ErrorHandler[M[T]]
    )(implicit
      cbf: Factory[T, M[T]],
      ec: ExecutionContext
    ): Future[M[T]] = Future.failed[M[T]](NoSuchResultException)

  def foldBulks[A](
      z: => A,
      maxDocs: Int
    )(suc: (A, Iterator[T]) => Cursor.State[A],
      err: Cursor.ErrorHandler[A]
    )(implicit
      ctx: ExecutionContext
    ): Future[A] = Future.failed[A](NoSuchResultException)

  def foldBulksM[A](
      z: => A,
      maxDocs: Int
    )(suc: (A, Iterator[T]) => Future[Cursor.State[A]],
      err: Cursor.ErrorHandler[A]
    )(implicit
      ctx: ExecutionContext
    ): Future[A] =
    Future.failed[A](NoSuchResultException)

  def foldWhile[A](
      z: => A,
      maxDocs: Int
    )(suc: (A, T) => Cursor.State[A],
      err: Cursor.ErrorHandler[A]
    )(implicit
      ctx: ExecutionContext
    ): Future[A] = Future.failed[A](NoSuchResultException)

  def foldWhileM[A](
      z: => A,
      maxDocs: Int
    )(suc: (A, T) => Future[Cursor.State[A]],
      err: Cursor.ErrorHandler[A]
    )(implicit
      ctx: ExecutionContext
    ): Future[A] = Future.failed[A](NoSuchResultException)

  def head(implicit ctx: ExecutionContext): Future[T] =
    Future.failed[T](NoSuchResultException)

  def headOption(implicit ctx: ExecutionContext): Future[Option[T]] =
    Future.failed[Option[T]](NoSuchResultException)

  def peek[M[_]](
      maxDocs: Int
    )(implicit
      cbf: Factory[T, M[T]],
      ec: ExecutionContext
    ): Future[Cursor.Result[M[T]]] =
    Future.failed[Cursor.Result[M[T]]](NoSuchResultException)
}
