package reactivemongo.play

import scala.concurrent.Future
import reactivemongo.api.{ Cursor, CursorProducer }

package object iteratees {
  /** Provides Play Iteratees instances for CursorProducer typeclass. */
  implicit def cursorProducer[T] = new CursorProducer[T] {
    type ProducedCursor = PlayIterateesCursor[T]

    // Returns a cursor with Play Iteratees operations.
    def produce(base: Cursor[T]): PlayIterateesCursor[T] =
      new PlayIterateesCursorImpl[T](base)

  }

  /** Provides flattener for Play Iteratees cursor. */
  implicit object cursorFlattener
      extends reactivemongo.api.CursorFlattener[PlayIterateesCursor] {

    def flatten[T](future: Future[PlayIterateesCursor[T]]): PlayIterateesCursor[T] = new PlayIterateesFlattenedCursor(future)
  }
}
