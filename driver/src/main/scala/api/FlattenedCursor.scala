package reactivemongo.api

import scala.language.higherKinds

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{ ExecutionContext, Future }

import play.api.libs.iteratee.Enumerator

import reactivemongo.core.protocol.Response

class FlattenedCursor[T](cursor: Future[Cursor[T]]) extends Cursor[T] {
  @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
  def enumerate(maxDocs: Int = -1, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[T] =
    Enumerator.flatten(cursor.map(_.enumerate(maxDocs, stopOnError)))

  @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
  def enumerateBulks(maxDocs: Int = -1, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Iterator[T]] =
    Enumerator.flatten(cursor.map(_.enumerateBulks(maxDocs, stopOnError)))

  @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
  def enumerateResponses(maxDocs: Int = -1, stopOnError: Boolean = false)(implicit ctx: ExecutionContext): Enumerator[Response] =
    Enumerator.flatten(cursor.map(_.enumerateResponses(maxDocs, stopOnError)))

  def head(implicit ctx: ExecutionContext): Future[T] = cursor.flatMap(_.head)

  // TODO: Remove override
  override def headOption(implicit ctx: ExecutionContext): Future[Option[T]] =
    cursor.flatMap(_.headOption)

  def collect[M[_]](maxDocs: Int, err: Cursor.ErrorHandler[M[T]])(implicit cbf: CanBuildFrom[M[_], T, M[T]], ec: ExecutionContext): Future[M[T]] = cursor.flatMap(_.collect[M](maxDocs, err))

  def foldResponses[A](z: => A, maxDocs: Int = -1)(suc: (A, Response) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = cursor.flatMap(_.foldResponses(z, maxDocs)(suc, err))

  def foldResponsesM[A](z: => A, maxDocs: Int = -1)(suc: (A, Response) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = cursor.flatMap(_.foldResponsesM(z, maxDocs)(suc, err))

  def foldBulks[A](z: => A, maxDocs: Int = -1)(suc: (A, Iterator[T]) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = cursor.flatMap(_.foldBulks(z, maxDocs)(suc, err))

  def foldBulksM[A](z: => A, maxDocs: Int = -1)(suc: (A, Iterator[T]) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = cursor.flatMap(_.foldBulksM(z, maxDocs)(suc, err))

  def foldWhile[A](z: => A, maxDocs: Int = -1)(suc: (A, T) => Cursor.State[A], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = cursor.flatMap(_.foldWhile(z, maxDocs)(suc, err))

  def foldWhileM[A](z: => A, maxDocs: Int = -1)(suc: (A, T) => Future[Cursor.State[A]], err: Cursor.ErrorHandler[A])(implicit ctx: ExecutionContext): Future[A] = cursor.flatMap(_.foldWhileM(z, maxDocs)(suc, err))

  @deprecated(message = "Use the Play Iteratees module", since = "0.11.10")
  def rawEnumerateResponses(maxDocs: Int = -1)(implicit ctx: ExecutionContext): Enumerator[Response] =
    Enumerator.flatten(cursor.map(_.rawEnumerateResponses(maxDocs)))

}
