package reactivemongo.core.nodeset

import scala.collection.Factory

package object utils {
  def update[A, M[T] <: Iterable[T]](coll: M[A])(f: PartialFunction[A, A])(implicit cbf: Factory[A, M[A]]): (M[A], Boolean) = {
    val builder = cbf.newBuilder
    val (head, tail) = coll.span(!f.isDefinedAt(_))

    builder ++= head

    if (!tail.isEmpty) {
      builder += f(tail.head)
      builder ++= tail.drop(1)
    }

    builder.result() -> !tail.isEmpty
  }
}
