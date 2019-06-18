package reactivemongo.bson

private[bson] object Compat {
  @inline def toMap[T, K, V](in: Iterable[T])(f: T => (K, V)): Map[K, V] =
    in.view.map(f).to(Map)
}
