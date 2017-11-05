package reactivemongo.bson

import scala.util.Try

/**
 * A writer that produces a subtype of [[BSONValue]] from an instance of `T`.
 */
trait BSONWriter[T, B <: BSONValue] {
  /**
   * Writes an instance of `T` as a BSON value.
   *
   * This method may throw exceptions at runtime.
   * If used outside a reader, one should consider `writeTry(bson: B): Try[T]` or `writeOpt(bson: B): Option[T]`.
   */
  def write(t: T): B

  /** Tries to produce a BSON value from an instance of `T`, returns `None` if an error occurred. */
  def writeOpt(t: T): Option[B] = writeTry(t).toOption

  /** Tries to produce a BSON value from an instance of `T`. */
  def writeTry(t: T): Try[B] = Try(write(t))

  /**
   * Returns a BSON writer that returns the result of applying `f`
   * on the BSON value from this writer.
   *
   * @param f the function to apply
   */
  final def afterWrite[U <: BSONValue](f: B => U): BSONWriter[T, U] =
    BSONWriter[T, U]((write _) andThen f)

  final def beforeWrite[U](f: U => T): BSONWriter[U, B] =
    BSONWriter[U, B](f andThen (write _))
}

object BSONWriter {
  private class Default[T, B <: BSONValue](
    _write: T => B) extends BSONWriter[T, B] {
    def write(value: T): B = _write(value)
  }

  def apply[T, B <: BSONValue](write: T => B): BSONWriter[T, B] =
    new Default[T, B](write)
}
