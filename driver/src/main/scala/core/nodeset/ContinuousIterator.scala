package reactivemongo.core.nodeset

private[reactivemongo] object ContinuousIterator {
  @inline def apply[A](iterable: Iterable[A]): Iterator[A] =
    if (iterable.isEmpty) empty[A] else wrapped(iterable)

  def empty[A]: Iterator[A] = new Iterator[A] {
    val hasNext: Boolean = false
    override val isTraversableAgain: Boolean = false

    def next(): A = throw new NoSuchElementException("empty iterator")
  }

  private def wrapped[A](iterable: Iterable[A]): Iterator[A] =
    new Iterator[A] {
      private var underlying = iterable.iterator

      override val isTraversableAgain: Boolean = true
      val hasNext: Boolean = true

      def next(): A = {
        if (!underlying.hasNext) {
          underlying = iterable.iterator
        }

        underlying.next()
      }
    }
}
