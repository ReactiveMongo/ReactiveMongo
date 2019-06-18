package reactivemongo.core.nodeset

private[reactivemongo] class ContinuousIterator[A](iterable: Iterable[A], private var toDrop: Int = 0) extends Iterator[A] {
  private var underlying = iterable.iterator
  private var i = 0

  val hasNext = underlying.hasNext

  if (hasNext) drop(toDrop)

  def next =
    if (!hasNext) throw new NoSuchElementException("empty iterator")
    else {
      if (!underlying.hasNext) {
        underlying = iterable.iterator
        i = 0
      }
      val a = underlying.next
      i += 1
      a
    }

  def nextIndex = i
}
