package reactivemongo.bson

import scala.language.higherKinds

import scala.util.Try

import scala.collection.generic.CanBuildFrom

trait LowPrioBSONHandlers {
  // Collections Handlers
  class BSONArrayCollectionReader[M[_], T](implicit cbf: CanBuildFrom[M[_], T, M[T]], reader: BSONReader[_ <: BSONValue, T]) extends BSONReader[BSONArray, M[T]] {
    def read(array: BSONArray) =
      array.stream.filter(_.isSuccess).map { v =>
        reader.asInstanceOf[BSONReader[BSONValue, T]].read(v.get)
      }.to[M]
  }

  class BSONArrayCollectionWriter[T, Repr <% Traversable[T]](implicit writer: BSONWriter[T, _ <: BSONValue]) extends VariantBSONWriter[Repr, BSONArray] {
    def write(repr: Repr) = {
      new BSONArray(repr.map(s => Try(writer.write(s))).to[Stream])
    }
  }

  implicit def collectionToBSONArrayCollectionWriter[T, Repr <% Traversable[T]](implicit writer: BSONWriter[T, _ <: BSONValue]): VariantBSONWriter[Repr, BSONArray] = new BSONArrayCollectionWriter[T, Repr]

  implicit def bsonArrayToCollectionReader[M[_], T](implicit cbf: CanBuildFrom[M[_], T, M[T]], reader: BSONReader[_ <: BSONValue, T]): BSONReader[BSONArray, M[T]] = new BSONArrayCollectionReader

}
