package reactivemongo.bson

trait BSONHandler[B <: BSONValue, T]
  extends BSONReader[B, T] with BSONWriter[T, B] {

  def as[R](to: T => R, from: R => T): BSONHandler[B, R] =
    new BSONHandler.MappedHandler(this, to, from)
}

object BSONHandler {
  private[bson] class MappedHandler[B <: BSONValue, T, U](
    parent: BSONHandler[B, T],
    to: T => U,
    from: U => T) extends BSONHandler[B, U] {
    def write(u: U) = parent.write(from(u))
    def read(b: B) = to(parent.read(b))
  }

  private[bson] class DefaultHandler[B <: BSONValue, T](r: B => T, w: T => B)
    extends BSONHandler[B, T] {
    def read(x: B): T = r(x)
    def write(x: T): B = w(x)
  }

  /**
   * Handler factory.
   *
   * {{{
   * import reactivemongo.bson.{ BSONHandler, BSONString }
   *
   * case class Foo(value: String)
   *
   * val foo: BSONHandler[BSONString, Foo] = BSONHandler(
   *   { read: BSONString => Foo(read.value) },
   *   { write: Foo => BSONString(write.value) }
   * )
   * }}}
   */
  def apply[B <: BSONValue, T](read: B => T, write: T => B): BSONHandler[B, T] =
    new DefaultHandler(read, write)

  /**
   * Returns a BSON handler for a type `T`, provided there are
   * a writer and a reader for it, both using the same kind of `BSONValue`.
   */
  implicit def provided[B <: BSONValue, T](implicit writer: BSONWriter[T, B], reader: BSONReader[B, T]): BSONHandler[B, T] = BSONHandler(reader.read _, writer.write _)
}
