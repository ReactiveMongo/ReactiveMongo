package reactivemongo.core.nodeset

import scala.language.higherKinds

import scala.collection.generic.CanBuildFrom

package object utils {
  @deprecated("Unused", "0.13.0")
  def updateFirst[A, M[T] <: Iterable[T]](coll: M[A])(f: A => Option[A])(implicit cbf: CanBuildFrom[M[_], A, M[A]]): M[A] = {
    val builder = cbf.apply

    builder.result
  }

  def update[A, M[T] <: Iterable[T]](coll: M[A])(f: PartialFunction[A, A])(implicit cbf: CanBuildFrom[M[_], A, M[A]]): (M[A], Boolean) = {
    val builder = cbf.apply
    val (head, tail) = coll.span(!f.isDefinedAt(_))

    builder ++= head

    if (!tail.isEmpty) {
      builder += f(tail.head)
      builder ++= tail.drop(1)
    }

    builder.result() -> !tail.isEmpty
  }
}
