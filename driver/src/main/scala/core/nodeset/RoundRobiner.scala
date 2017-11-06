package reactivemongo.core.nodeset

@deprecated(message = "Will be made private", since = "0.11.10")
class RoundRobiner[A, M[T] <: Iterable[T]](val subject: M[A], startAtIndex: Int = 0) {
  private val iterator = new ContinuousIterator(subject)
  private val length = subject.size

  def pick: Option[A] = if (iterator.hasNext) Some(iterator.next) else None

  def pickWithFilter(filter: A => Boolean): Option[A] =
    pickWithFilter(filter, 0)

  @annotation.tailrec
  private def pickWithFilter(filter: A => Boolean, tested: Int): Option[A] =
    if (length > 0 && tested < length) {
      val a = pick
      if (!a.isDefined) None
      else if (filter(a.get)) a
      else pickWithFilter(filter, tested + 1)
    } else None

  def copy(subject: M[A], startAtIndex: Int = iterator.nextIndex) =
    new RoundRobiner(subject, startAtIndex)
}
