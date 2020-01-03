package reactivemongo.api.collections

import scala.language.higherKinds

import scala.collection.generic.CanBuildFrom

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.api.{ Collation, ReadConcern, SerializationPack }

private[collections] trait DistinctOpCompat[P <: SerializationPack with Singleton] { collection: GenericCollection[P] with DistinctOp[P] =>

  final protected def distinctDocuments[T, M[_] <: Iterable[_]](
    key: String,
    query: Option[pack.Document],
    readConcern: ReadConcern,
    collation: Option[Collation])(implicit
    reader: pack.NarrowValueReader[T],
    ec: ExecutionContext,
    cbf: CanBuildFrom[M[_], T, M[T]]): Future[M[T]] =
    distinctDocuments(key, query, readConcern, collation, cbf())
}
