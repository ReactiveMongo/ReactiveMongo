package reactivemongo.bson

import scala.util.{ Failure, Success, Try }

sealed trait UnsafeBSONReader[T] {
  def readTry(value: BSONValue): Try[T]
}

/**
 * A reader that produces an instance of `T` from a subtype of [[BSONValue]].
 */
trait BSONReader[B <: BSONValue, T] { self =>

  /**
   * Reads a BSON value and produce an instance of `T`.
   *
   * This method may throw exceptions at runtime.
   * If used outside a reader, one should consider `readTry(bson: B): Try[T]` or `readOpt(bson: B): Option[T]`.
   */
  def read(bson: B): T

  /** Tries to produce an instance of `T` from the `bson` value, returns `None` if an error occurred. */
  def readOpt(bson: B): Option[T] = readTry(bson).toOption

  /** Tries to produce an instance of `T` from the `bson` value. */
  def readTry(bson: B): Try[T] = Try(read(bson))

  /**
   * Returns a BSON reader that returns the result of applying `f`
   * on the result of this reader.
   *
   * @param f the function to apply
   */
  final def afterRead[U](f: T => U): BSONReader[B, U] =
    BSONReader[B, U]((read _) andThen f)

  final def beforeRead[U <: BSONValue](f: U => B): BSONReader[U, T] =
    BSONReader[U, T](f andThen (read _))

  private[reactivemongo] def widenReader[U >: T]: UnsafeBSONReader[U] =
    new UnsafeBSONReader[U] {
      def readTry(value: BSONValue): Try[U] =
        Try(value.asInstanceOf[B]) match {
          case Failure(_) => Failure(exceptions.TypeDoesNotMatch(
            s"Cannot convert $value: ${value.getClass} with ${self.getClass}"))

          case Success(bson) => self.readTry(bson)
        }
    }
}

object BSONReader {
  private class Default[B <: BSONValue, T](
    _read: B => T) extends BSONReader[B, T] {
    def read(bson: B): T = _read(bson)
  }

  def apply[B <: BSONValue, T](read: B => T): BSONReader[B, T] =
    new Default[B, T](read)
}
