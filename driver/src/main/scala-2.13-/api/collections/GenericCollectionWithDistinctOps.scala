package reactivemongo.api.collections

import scala.language.higherKinds

import scala.collection.generic.CanBuildFrom

import scala.concurrent.{ Future, ExecutionContext }

import reactivemongo.api.{ Collation, ReadConcern, SerializationPack }

private[collections] trait GenericCollectionWithDistinctOps[P <: SerializationPack with Singleton] { self: GenericCollection[P] with DistinctOp[P] with HintFactory[P] =>

  @deprecated("Use `distinct` with `Collation`", "0.16.0")
  def distinct[T, M[_] <: Iterable[_]](
    key: String,
    selector: Option[pack.Document] = None,
    readConcern: ReadConcern = self.readConcern)(implicit reader: pack.NarrowValueReader[T], ec: ExecutionContext, cbf: CanBuildFrom[M[_], T, M[T]]): Future[M[T]] = distinctDocuments[T, M](key, selector, readConcern, collation = None)

  /**
   * Returns the distinct values for a specified field
   * across a single collection.
   *
   * @tparam T the element type of the distinct values
   * @tparam M the container, that must be a [[scala.collection.Iterable]]
   *
   * @param key the field for which to return distinct values
   * @param selector $selectorParam, that specifies the documents from which to retrieve the distinct values.
   * @param readConcern $readConcernParam
   * @param collation $collationParam
   *
   * {{{
   * import scala.concurrent.ExecutionContext
   *
   * import reactivemongo.api.ReadConcern
   * import reactivemongo.api.bson.collection.BSONCollection
   *
   * def distinctStates(coll: BSONCollection)(implicit ec: ExecutionContext) =
   *   coll.distinct[String, Set]("state", None, ReadConcern.Local, None)
   * }}}
   */
  def distinct[T, M[_] <: Iterable[_]](
    key: String,
    @deprecatedName('query) selector: Option[pack.Document],
    readConcern: ReadConcern,
    collation: Option[Collation])(implicit
    reader: pack.NarrowValueReader[T],
    ec: ExecutionContext, cbf: CanBuildFrom[M[_], T, M[T]]): Future[M[T]] =
    distinctDocuments[T, M](key, selector, readConcern, collation)

}
