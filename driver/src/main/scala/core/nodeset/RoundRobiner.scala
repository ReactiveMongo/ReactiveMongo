package reactivemongo.core.nodeset

import scala.language.higherKinds

private[reactivemongo] class RoundRobiner[A, M[T] <: Iterable[T]](
  val subject: M[A]) {

  private val iterator = new ContinuousIterator(subject)
  private val length = subject.size

  def pick: Option[A] = if (iterator.hasNext) Some(iterator.next) else None

  def pickWithFilter(filter: A => Boolean): Option[A] =
    pickWithFilter(filter, 0)

  @annotation.tailrec
  private def pickWithFilter(filter: A => Boolean, tested: Int): Option[A] = {
    if (length > 0 && tested < length) {
      pick match {
        case a @ Some(v) if filter(v) => a
        case Some(_)                  => pickWithFilter(filter, tested + 1)
        case _                        => Option.empty[A]
      }
    } else Option.empty[A]
  }
}
