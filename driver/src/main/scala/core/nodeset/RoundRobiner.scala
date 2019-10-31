package reactivemongo.core.nodeset

import scala.math.Ordering

private[reactivemongo] abstract class RoundRobiner[A, M[T] <: Iterable[T]] {
  private[reactivemongo] def toList: List[A]

  def pick: Option[A]

  def pickWithFilterAndPriority(filter: A => Boolean, unpriorized: Int)(implicit ord: Ordering[A]): Option[A]

  def pickWithFilter(filter: A => Boolean): Option[A]

  def size: Int

  def isEmpty: Boolean
}

private[reactivemongo] object RoundRobiner {
  def apply[A, M[T] <: Iterable[T]](subject: M[A]): RoundRobiner[A, M] =
    if (subject.isEmpty) empty[A, M] else wrapped(subject)

  def empty[A, M[T] <: Iterable[T]]: RoundRobiner[A, M] =
    new RoundRobiner[A, M] {
      def toList = List.empty[A]

      val pick: Option[A] = None

      def pickWithFilterAndPriority(filter: A => Boolean, unpriorized: Int)(implicit ord: Ordering[A]): Option[A] = None

      def pickWithFilter(filter: A => Boolean): Option[A] = None

      val size: Int = 0

      val isEmpty: Boolean = true

      override val toString = f"RoundRobiner$$Empty"
    }

  private def wrapped[A, M[T] <: Iterable[T]](subject: M[A]): RoundRobiner[A, M] = new RoundRobiner[A, M] {
    private var underlying = subject.iterator

    def toList: List[A] = subject.toList

    private def next(): A = {
      if (!underlying.hasNext) {
        underlying = subject.iterator
      }

      underlying.next()
    }

    def pick: Option[A] = Some(next())

    def pickWithFilterAndPriority(
      filter: A => Boolean,
      unpriorized: Int)(
      implicit
      ord: Ordering[A]): Option[A] = {
      if (unpriorized < size) {
        pickWithFilterAndPriority(filter, 0, unpriorized, List.empty)
      } else {
        subject.toList
      }
    }.sorted.headOption

    @annotation.tailrec
    private def pickWithFilterAndPriority(
      filter: A => Boolean,
      tested: Int,
      unpriorized: Int,
      acc: List[A])(implicit ord: Ordering[A]): List[A] = {
      if (unpriorized > 0 && tested < size) {
        val v = next()

        if (filter(v)) {
          pickWithFilterAndPriority(
            filter, tested + 1, unpriorized - 1, v :: acc)

        } else {
          pickWithFilterAndPriority(
            filter, tested + 1, unpriorized, acc)
        }
      } else acc
    }

    def pickWithFilter(filter: A => Boolean): Option[A] =
      pickWithFilter(filter, 0)

    @annotation.tailrec
    private def pickWithFilter(filter: A => Boolean, tested: Int): Option[A] = {
      if (tested < size) {
        val v = next()

        if (filter(v)) Some(v)
        else pickWithFilter(filter, tested + 1)
      } else Option.empty[A]
    }

    @inline def size: Int = subject.size

    val isEmpty: Boolean = false

    override def toString = s"RoundRobiner($subject)"
  }
}
