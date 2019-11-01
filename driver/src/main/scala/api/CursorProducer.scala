package reactivemongo.api

import scala.language.higherKinds

import scala.concurrent.Future

/** Allows to enrich a base cursor. */
trait CursorProducer[T] {
  type ProducedCursor <: Cursor[T]

  /** Produces a custom cursor from the `base` one. */
  def produce(base: Cursor.WithOps[T]): ProducedCursor
}

object CursorProducer {
  private[api] type Aux[T, C[_] <: Cursor[_]] = CursorProducer[T] {
    type ProducedCursor = C[T]
  }

  implicit def defaultCursorProducer[T]: CursorProducer.Aux[T, Cursor.WithOps] =
    new CursorProducer[T] {
      type ProducedCursor = Cursor.WithOps[T]
      def produce(base: Cursor.WithOps[T]) = base
    }
}

/**
 * Flattening strategy for cursor.
 *
 * {{{
 * trait FooCursor[T] extends Cursor[T] { def foo: String }
 *
 * implicit def fooFlattener[T] = new CursorFlattener[FooCursor] {
 *   def flatten[T](future: Future[FooCursor[T]]): FooCursor[T] =
 *     new FlattenedCursor[T](future) with FooCursor[T] {
 *       def foo = "Flattened"
 *     }
 * }
 * }}}
 */
trait CursorFlattener[C[_] <: Cursor[_]] {
  /** Flatten a future of cursor as cursor. */
  def flatten[T](future: Future[C[T]]): C[T]
}

/** Flatteners helper */
object CursorFlattener {
  implicit object defaultCursorFlattener extends CursorFlattener[Cursor] {
    def flatten[T](future: Future[Cursor[T]]): Cursor[T] =
      new FlattenedCursor[T](future)
  }
}
