package reactivemongo.bson

import scala.language.higherKinds

import scala.util.Try
import scala.collection.Factory

trait LowPrioBSONHandlers {
  // Collections Handlers
  class BSONArrayCollectionReader[M[_], T](implicit cbf: Factory[T, M[T]], reader: BSONReader[_ <: BSONValue, T]) extends BSONReader[BSONArray, M[T]] {
    def read(array: BSONArray) =
      array.stream.filter(_.isSuccess).map { v =>
        reader.asInstanceOf[BSONReader[BSONValue, T]].read(v.get)
      }.to(cbf)
  }

  class BSONArrayCollectionWriter[T, Repr <% Iterable[T]](implicit writer: BSONWriter[T, _ <: BSONValue]) extends VariantBSONWriter[Repr, BSONArray] {
    def write(repr: Repr) = {
      new BSONArray(repr.view.map(s => Try(writer.write(s))).to(Stream))
    }
  }

  implicit def collectionToBSONArrayCollectionWriter[T, Repr <% Iterable[T]](implicit writer: BSONWriter[T, _ <: BSONValue]): VariantBSONWriter[Repr, BSONArray] = new BSONArrayCollectionWriter[T, Repr]

  implicit def bsonArrayToCollectionReader[M[_], T](implicit cbf: Factory[T, M[T]], reader: BSONReader[_ <: BSONValue, T]): BSONReader[BSONArray, M[T]] = new BSONArrayCollectionReader

}
