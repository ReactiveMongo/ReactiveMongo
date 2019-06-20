package reactivemongo.bson

private[bson] object Compat {
  @inline def toMap[T, K, V](in: Iterable[T])(f: T => (K, V)): Map[K, V] =
    in.map(f)(scala.collection.breakOut)
}
