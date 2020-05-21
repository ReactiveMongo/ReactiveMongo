package reactivemongo.core.nodeset

import scala.collection.generic.CanBuildFrom

package object utils {
  @SuppressWarnings(Array("UnsafeTraversableMethods"))
  private[nodeset] def update[A, M[T] <: Iterable[T]](coll: M[A])(f: PartialFunction[A, A])(implicit cbf: CanBuildFrom[M[_], A, M[A]]): (M[A], Boolean) = {
    val builder = cbf.apply
    val (head, tail) = coll.span(!f.isDefinedAt(_))

    builder ++= head

    if (tail.nonEmpty) {
      builder += f(tail.head)
      builder ++= tail.drop(1)
    }

    builder.result() -> tail.nonEmpty
  }
}
