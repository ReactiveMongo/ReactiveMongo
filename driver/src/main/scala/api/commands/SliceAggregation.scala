package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

private[commands] trait SliceAggregation[P <: SerializationPack] {
  aggregation: AggregationFramework[P] =>

  /**
   * Returns a subset of an array.
   * https://docs.mongodb.com/manual/reference/operator/aggregation/slice/
   *
   * @param array any valid expression that resolves to an array
   * @param position any valid expression that resolves to an integer
   * @param n any valud expression that resolves to an integer
   */
  final class Slice private (
    array: pack.Value,
    position: Option[pack.Value],
    n: Option[pack.Value]) extends PipelineOperator {

    import builder.{ document, elementProducer => element }

    val makePipe: pack.Document = {
      val els = Seq.newBuilder[pack.Value]

      position.foreach { els += _ }
      n.foreach { els += _ }

      document(Seq(
        element(f"$$slice", builder.array(array, els.result()))))
    }
  }

  /**
   * {{{
   * Project(BSONDocument(
   *   "name" -> 1,
   *   "favorites" -> Slice(
   *     array = BSONString(f"$$favorites"),
   *     n = BSONInteger(3)).makePipe))
   * }}}
   */
  object Slice {
    /** Create a \$slice stage without the `position` argument. */
    def apply(
      array: pack.Value,
      n: pack.Value): Slice =
      new Slice(array, Option.empty[pack.Value], Some(n))

    /** Create a \$slice stage with `position` and `n` arguments. */
    def apply(
      array: pack.Value,
      position: pack.Value,
      n: pack.Value): Slice = new Slice(array, Some(position), Some(n))
  }
}
