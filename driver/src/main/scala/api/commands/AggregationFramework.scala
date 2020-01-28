package reactivemongo.api.commands

import scala.runtime.{
  AbstractFunction1,
  AbstractFunction2,
  AbstractFunction3,
  AbstractFunction4,
  AbstractFunction5
}

import reactivemongo.api.{ ChangeStreams, ReadConcern, SerializationPack }
import reactivemongo.core.protocol.MongoWireVersion

/**
 * Implements the [[http://docs.mongodb.org/manual/applications/aggregation/ Aggregation Framework]].
 *
 * @see [[PipelineOperator]]
 */
trait AggregationFramework[P <: SerializationPack]
  extends ImplicitCommandHelpers[P]
  with GroupAggregation[P] with SliceAggregation[P] with SortAggregation[P]
  with AggregationPipeline[P] { self =>

  protected final lazy val builder = pack.newBuilder

  @inline protected final def pipe(name: String, arg: pack.Value): pack.Document = builder.document(Seq(builder.elementProducer(name, arg)))

  /**
   * @param batchSize the initial batch size for the cursor
   */
  @deprecated("Use `api.collections.Aggregator`", "0.12.7")
  class Cursor private[api] (val batchSize: Int) extends Product1[Int] {
    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = batchSize

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Cursor => true
      case _         => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Cursor =>
        this.batchSize == other.batchSize

      case _ =>
        false
    }

    override def hashCode: Int = batchSize

    override def toString: String = s"Cursor(${batchSize})"
  }

  object Cursor extends AbstractFunction1[Int, Cursor] {
    def apply(batchSize: Int): Cursor = new Cursor(batchSize)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(cursor: Cursor): Option[Int] = Option(cursor).map(_.batchSize)
  }

  /**
   * @since MongoDB 3.2
   * @param pipeline the sequence of MongoDB aggregation operations
   * @param explain specifies to return the information on the processing of the pipeline
   * @param allowDiskUse enables writing to temporary files
   * @param cursor the cursor object for aggregation
   * @param bypassDocumentValidation available only if you specify the \$out aggregation operator
   * @param readConcern the read concern
   */
  @deprecated("Use `api.collections.Aggregator`", "0.12.7")
  case class Aggregate(
    pipeline: Seq[PipelineOperator],
    explain: Boolean = false,
    allowDiskUse: Boolean,
    cursor: Option[Cursor],
    wireVersion: MongoWireVersion,
    bypassDocumentValidation: Boolean,
    readConcern: Option[ReadConcern]) extends CollectionCommand with CommandWithPack[pack.type]
    with CommandWithResult[AggregationResult]

  /**
   * @param firstBatch the documents of the first batch
   * @param cursor the cursor from the result, if any
   * @see [[Cursor]]
   */
  @deprecated("Use `api.collections.Aggregator`", "0.12.7")
  case class AggregationResult(
    firstBatch: List[pack.Document],
    cursor: Option[ResultCursor] = None) {

    /**
     * Returns the first batch as a list, using the given `reader`.
     */
    def head[T](implicit reader: pack.Reader[T]): List[T] =
      firstBatch.map(pack.deserialize(_, reader))

  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/addFields/ \$addFields]] stage.
   *
   * @since MongoDB 3.4
   * @param specifications The fields to include.
   * The resulting objects will also contain these fields.
   */
  class AddFields private[api] (val specifications: pack.Document)
    extends PipelineOperator
    with Product1[pack.Document] with Serializable {

    val makePipe = pipe(f"$$addFields", specifications)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = specifications

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: AddFields => true
      case _            => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: AddFields =>
        (this.specifications == null && other.specifications == null) || (
          this.specifications != null && this.specifications.
          equals(other.specifications))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (specifications == null) -1 else specifications.hashCode

    override def toString: String = s"AddFields(${pack pretty specifications})"
  }

  object AddFields extends AbstractFunction1[pack.Document, AddFields] {
    def apply(specifications: pack.Document): AddFields =
      new AddFields(specifications)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(addFields: AddFields): Option[pack.Document] =
      Option(addFields).map(_.specifications)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/bucket/ \$bucket]] aggregation stage.
   */
  class Bucket private[api] (
    val groupBy: pack.Value,
    val boundaries: Seq[pack.Value],
    val default: String)(
    val output: (String, GroupFunction)*) extends PipelineOperator with Product4[pack.Value, Seq[pack.Value], String, Seq[(String, GroupFunction)]] {

    import builder.{ document, elementProducer => element }

    val makePipe: pack.Document = {
      val opts = Seq.newBuilder[pack.ElementProducer]

      opts ++= Seq(
        element("groupBy", groupBy),
        element("default", builder.string(default)),
        element("output", document(output.map({
          case (field, op) => element(field, op.makeFunction)
        }))))

      boundaries.headOption.foreach { first =>
        opts += element("boundaries", builder.array(first, boundaries.tail))
      }

      pipe(f"$$bucket", document(opts.result()))
    }

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = groupBy

    @deprecated("No longer a case class", "0.20.3")
    @inline def _2 = boundaries

    @deprecated("No longer a case class", "0.20.3")
    @inline def _3 = default

    @deprecated("No longer a case class", "0.20.3")
    @inline def _4 = output

    def canEqual(that: Any): Boolean = that match {
      case _: Bucket => true
      case _         => false
    }

    private[api] lazy val tupled =
      Tuple4(groupBy, boundaries, default, output)

    override def equals(that: Any): Boolean = that match {
      case other: Bucket =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"Bucket${tupled.toString}"
  }

  object Bucket {
    def apply(
      groupBy: pack.Value,
      boundaries: Seq[pack.Value],
      default: String)(
      output: (String, GroupFunction)*): Bucket =
      new Bucket(groupBy, boundaries, default)(output: _*)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(res: Bucket) = Option(res).map(_.tupled)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/bucketAuto/ \$bucket]] aggregation stage.
   *
   * @since MongoDB 3.4
   *
   * Categorizes incoming documents into a specific number of groups,
   * called buckets, based on a specified expression.
   *
   * Bucket boundaries are automatically determined in an attempt
   * to evenly distribute the documents into the specified number of buckets.
   *
   * Document fields identifier must be prefixed with `$`.
   */
  class BucketAuto private[api] (
    val groupBy: pack.Value,
    val buckets: Int,
    val granularity: Option[String])(
    val output: (String, GroupFunction)*)
    extends PipelineOperator with Product4[pack.Value, Int, Option[String], Seq[(String, GroupFunction)]] with Serializable {

    import builder.{ document, elementProducer => element }

    val makePipe: pack.Document = {
      val opts = Seq.newBuilder[pack.ElementProducer] ++= Seq(
        element("groupBy", groupBy),
        element("buckets", builder.int(buckets)),
        element("output", document(output.map({
          case (field, op) => element(field, op.makeFunction)
        }))))

      granularity.foreach { g =>
        opts += element("granularity", builder.string(g))
      }

      pipe(f"$$bucketAuto", document(opts.result()))
    }

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = groupBy

    @deprecated("No longer a case class", "0.20.3")
    @inline def _2 = buckets

    @deprecated("No longer a case class", "0.20.3")
    @inline def _3 = granularity

    @deprecated("No longer a case class", "0.20.3")
    @inline def _4 = output

    def canEqual(that: Any): Boolean = that match {
      case _: BucketAuto => true
      case _             => false
    }

    private[api] lazy val tupled =
      Tuple4(groupBy, buckets, granularity, output)

    override def equals(that: Any): Boolean = that match {
      case other: BucketAuto =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"BucketAuto${tupled.toString}"
  }

  object BucketAuto {
    def apply(
      groupBy: pack.Value,
      buckets: Int,
      granularity: Option[String])(
      output: (String, GroupFunction)*): BucketAuto =
      new BucketAuto(groupBy, buckets, granularity)(output: _*)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(res: BucketAuto) = Option(res).map(_.tupled).collect {
      case (a, b, c, _) => Tuple3(a, b, c)
    }
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/collStats/ \$collStats]] aggregation stage.
   */
  class CollStats private[api] (
    val latencyStatsHistograms: Boolean,
    val storageStatsScale: Option[Double],
    val count: Boolean) extends PipelineOperator
    with Product3[Boolean, Option[Double], Boolean] with Serializable {

    import builder.{ document, elementProducer => element }

    def makePipe: pack.Document = {
      val opts = Seq.newBuilder[pack.ElementProducer]

      opts += element("latencyStats", document(
        Seq(element("histograms", builder.boolean(latencyStatsHistograms)))))

      storageStatsScale.foreach { scale =>
        opts += element("storageStats", document(
          Seq(element("scale", builder.double(scale)))))
      }

      if (count) {
        opts += element("count", document(Seq.empty))
      }

      pipe(f"$$collStats", document(opts.result()))
    }

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = latencyStatsHistograms

    @deprecated("No longer a case class", "0.20.3")
    @inline def _2 = storageStatsScale

    @deprecated("No longer a case class", "0.20.3")
    @inline def _3 = count

    def canEqual(that: Any): Boolean = that match {
      case _: CollStats => true
      case _            => false
    }

    private[api] lazy val tupled =
      Tuple3(latencyStatsHistograms, storageStatsScale, count)

    override def equals(that: Any): Boolean = that match {
      case other: CollStats =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"CollStats${tupled.toString}"
  }

  object CollStats extends AbstractFunction3[Boolean, Option[Double], Boolean, CollStats] {
    def apply(
      latencyStatsHistograms: Boolean,
      storageStatsScale: Option[Double],
      count: Boolean): CollStats =
      new CollStats(latencyStatsHistograms, storageStatsScale, count)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(collStats: CollStats) = Option(collStats).map(_.tupled)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/count/ \$count]] of the number of documents input.
   *
   * @since MongoDB 3.4
   * @param outputName the name of the output field which has the count as its value
   */
  class Count private[api] (val outputName: String)
    extends PipelineOperator with Product1[String] with Serializable {

    val makePipe: pack.Document = pipe(f"$$count", builder.string(outputName))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = outputName

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Count => true
      case _        => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Count =>
        (this.outputName == null && other.outputName == null) || (
          this.outputName != null && this.outputName.
          equals(other.outputName))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (outputName == null) -1 else outputName.hashCode

    override def toString: String = s"Count(${outputName})"
  }

  object Count extends AbstractFunction1[String, Count] {
    def apply(outputName: String): Count = new Count(outputName)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(count: Count): Option[String] = Option(count).map(_.outputName)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/currentOp/ \$currentOp]].
   *
   * @since MongoDB 3.6
   * @param allUsers (Defaults to `false`)
   * @param idleConnections if set to true, all operations including idle connections will be returned (Defaults to `false`)
   * @param idleCursors (Defaults to `false`; new in 4.2)
   * @param idleSessions (Defaults to `true`; new in 4.0)
   * @param localOps (Defaults to false; new in 4.0)
   */
  class CurrentOp private[api] (
    val allUsers: Boolean,
    val idleConnections: Boolean,
    val idleCursors: Boolean,
    val idleSessions: Boolean,
    val localOps: Boolean) extends PipelineOperator with Product5[Boolean, Boolean, Boolean, Boolean, Boolean] {
    import builder.{ boolean, elementProducer => element }
    val makePipe: pack.Document = pipe(f"$$currentOp", builder.document(Seq(
      element("allUsers", boolean(allUsers)),
      element("idleConnections", boolean(idleConnections)),
      element("idleCursors", boolean(idleCursors)),
      element("idleSessions", boolean(idleSessions)),
      element("localOps", boolean(localOps)))))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = allUsers

    @deprecated("No longer a case class", "0.20.3")
    @inline def _2 = idleConnections

    @deprecated("No longer a case class", "0.20.3")
    @inline def _3 = idleCursors

    @deprecated("No longer a case class", "0.20.3")
    @inline def _4 = idleSessions

    @deprecated("No longer a case class", "0.20.3")
    @inline def _5 = localOps

    def canEqual(that: Any): Boolean = that match {
      case _: CurrentOp => true
      case _            => false
    }

    private[api] lazy val tupled =
      Tuple5(allUsers, idleConnections, idleCursors, idleSessions, localOps)

    override def equals(that: Any): Boolean = that match {
      case other: CurrentOp =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"CurrentOp${tupled.toString}"
  }

  object CurrentOp extends AbstractFunction5[Boolean, Boolean, Boolean, Boolean, Boolean, CurrentOp] {
    def apply(
      allUsers: Boolean = false,
      idleConnections: Boolean = false,
      idleCursors: Boolean = false,
      idleSessions: Boolean = true,
      localOps: Boolean = false): CurrentOp = new CurrentOp(
      allUsers, idleConnections, idleCursors, idleSessions, localOps)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(res: CurrentOp) = Option(res).map(_.tupled)
  }

  /**
   * Processes multiple aggregation pipelines within a single stage
   * on the same set of input documents.
   *
   * Each sub-pipeline has its own field in the output document
   * where its results are stored as an array of documents.
   *
   * @param specifications the subpipelines to run
   * @see https://docs.mongodb.com/manual/reference/operator/aggregation/facet/
   */
  class Facet private[api] (
    val specifications: Iterable[(String, Pipeline)])
    extends PipelineOperator with Product1[Iterable[(String, Pipeline)]]
    with Serializable {

    val makePipe: pack.Document = {
      import builder.{ document, elementProducer => elem }

      val specDoc = document(specifications.map {
        case (name, (firstOp, subOps)) => elem(name, builder.array(
          firstOp.makePipe, subOps.map(_.makePipe)))

      }.toSeq)

      pipe(f"$$facet", specDoc)
    }

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = specifications

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Facet => true
      case _        => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Facet =>
        (this.specifications == null && other.specifications == null) || (
          this.specifications != null && this.specifications.
          equals(other.specifications))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (specifications == null) -1 else specifications.hashCode

    override def toString: String = s"Facet(${specifications})"
  }

  object Facet extends AbstractFunction1[Iterable[(String, Pipeline)], Facet] {
    def apply(specifications: Iterable[(String, Pipeline)]): Facet =
      new Facet(specifications)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(facet: Facet): Option[Iterable[(String, Pipeline)]] =
      Option(facet).map(_.specifications)
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/operator/aggregation/geoNear/#pipe._S_geoNear \$geoNear]] aggregation stage, that outputs documents in order of nearest to farthest from a specified point.
   *
   * @param near the point for which to find the closest documents
   * @param spherical if using a 2dsphere index
   * @param limit the maximum number of documents to return (no longer supported since MongoDB 4.2)
   * @param maxDistance the maximum distance from the center point that the documents can be
   * @param query limits the results to the matching documents
   * @param distanceMultiplier the factor to multiply all distances returned by the query
   * @param uniqueDocs if this value is true, the query returns a matching document once
   * @param distanceField the output field that contains the calculated distance
   * @param includeLocs this specifies the output field that identifies the location used to calculate the distance
   */
  class GeoNear private[reactivemongo] (
    val near: pack.Value,
    val spherical: Boolean,
    val limit: Option[Long],
    val minDistance: Option[Long],
    val maxDistance: Option[Long],
    val query: Option[pack.Document],
    val distanceMultiplier: Option[Double],
    val uniqueDocs: Boolean,
    val distanceField: Option[String],
    val includeLocs: Option[String]) extends PipelineOperator with Product with Serializable {
    @deprecated("Use the constructor with optional `limit`", "0.18.5")
    def this(
      near: pack.Value,
      spherical: Boolean,
      limit: Long,
      minDistance: Option[Long],
      maxDistance: Option[Long],
      query: Option[pack.Document],
      distanceMultiplier: Option[Double],
      uniqueDocs: Boolean,
      distanceField: Option[String],
      includeLocs: Option[String]) = this(near, spherical, Some(limit), minDistance, maxDistance, query, distanceMultiplier, uniqueDocs, distanceField, includeLocs)

    import builder.{
      boolean,
      elementProducer => element,
      document,
      long,
      string
    }

    def makePipe: pack.Document = pipe(f"$$geoNear", document(Seq(
      element("near", near),
      element("spherical", boolean(spherical))) ++ Seq(
        limit.map(l => element("limit", long(l))),
        minDistance.map(l => element("minDistance", long(l))),
        maxDistance.map(l => element("maxDistance", long(l))),
        query.map(s => element("query", s)),
        distanceMultiplier.map(d => element(
          "distanceMultiplier", builder.double(d))),
        Some(element("uniqueDocs", boolean(uniqueDocs))),
        distanceField.map(s =>
          element("distanceField", string(s))),
        includeLocs.map(s =>
          element("includeLocs", string(s)))).flatten))

    @deprecated("No longer a ReactiveMongo case class", "0.18.5")
    def canEqual(that: Any): Boolean = that match {
      case _: GeoNear => true
      case _          => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: GeoNear => tupled == other.tupled
      case _              => false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString: String = s"GeoNear${tupled.toString}"

    @deprecated("No longer a ReactiveMongo case class", "0.18.5")
    val productArity: Int = 10

    @deprecated("No longer a ReactiveMongo case class", "0.18.5")
    def productElement(n: Int): Any = (n: @annotation.switch) match {
      case 0 => near
      case 1 => spherical
      case 2 => limit
      case 3 => minDistance
      case 4 => maxDistance
      case 5 => query
      case 6 => distanceMultiplier
      case 7 => uniqueDocs
      case 8 => distanceField
      case _ => includeLocs
    }

    private[reactivemongo] lazy val tupled = Tuple10(near, spherical, limit, minDistance, maxDistance, query, distanceMultiplier, uniqueDocs, distanceField, includeLocs)
  }

  object GeoNear {
    @deprecated("Use the factory with optional `limit` (no longer supported since MongoDB 4.2", "0.18.5")
    def apply(
      near: pack.Value,
      spherical: Boolean,
      limit: Long,
      minDistance: Option[Long],
      maxDistance: Option[Long],
      query: Option[pack.Document],
      distanceMultiplier: Option[Double],
      uniqueDocs: Boolean,
      distanceField: Option[String],
      includeLocs: Option[String]): GeoNear = new GeoNear(near, spherical, Some(limit), minDistance, maxDistance, query, distanceMultiplier, uniqueDocs, distanceField, includeLocs)

    def apply(
      near: pack.Value,
      spherical: Boolean = false,
      limit: Option[Long] = None,
      minDistance: Option[Long] = None,
      maxDistance: Option[Long] = None,
      query: Option[pack.Document] = None,
      distanceMultiplier: Option[Double] = None,
      uniqueDocs: Boolean = false,
      distanceField: Option[String] = None,
      includeLocs: Option[String] = None): GeoNear = new GeoNear(near, spherical, limit, minDistance, maxDistance, query, distanceMultiplier, uniqueDocs, distanceField, includeLocs)

    @deprecated("No longer a ReactiveMongo case class", "0.18.5")
    def unapply(stage: GeoNear): Option[Tuple10[pack.Value, Boolean, Long, Option[Long], Option[Long], Option[pack.Document], Option[Double], Boolean, Option[String], Option[String]]] = Some(Tuple10(stage.near, stage.spherical, stage.limit.getOrElse(100L), stage.minDistance, stage.maxDistance, stage.query, stage.distanceMultiplier, stage.uniqueDocs, stage.distanceField, stage.includeLocs))
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/group/#_S_group \$group]]s documents together to calculate aggregates on document collections.
   * This command aggregates on arbitrary identifiers.
   * Document fields identifier must be prefixed with `$`.
   *
   * @param identifiers any BSON value acceptable by mongodb as identifier
   * @param ops the sequence of operators specifying aggregate calculation
   */
  class Group private[api] (val identifiers: pack.Value)(val ops: (String, GroupFunction)*)
    extends PipelineOperator
    with Product2[pack.Value, Seq[(String, GroupFunction)]]
    with Serializable {

    import builder.{ document, elementProducer => element }

    val makePipe: pack.Document = pipe(f"$$group", document(
      element("_id", identifiers) +: ops.map({
        case (field, op) => element(field, op.makeFunction)
      })))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = identifiers

    @deprecated("No longer a case class", "0.20.3")
    @inline def _2 = ops

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Group => true
      case _        => false
    }

    private[api] lazy val tupled = identifiers -> ops

    override def equals(that: Any): Boolean = that match {
      case other: Group =>
        this.tupled == other.tupled

      case _ => false
    }

    @inline override def hashCode: Int = tupled.hashCode

    override def toString = s"Group${tupled.toString}"
  }

  object Group {
    def apply(identifiers: pack.Value)(ops: (String, GroupFunction)*): Group =
      new Group(identifiers)(ops: _*)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(other: Group): Option[pack.Value] =
      Option(other).map(_.identifiers)

  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/group/#_S_group \$group]]s documents together to calculate aggregates on document collections.
   * This command aggregates on one field.
   *
   * @param idField the name of the field to aggregate on
   * @param ops the sequence of operators specifying aggregate calculation
   */
  class GroupField private[api] (val idField: String)(val ops: (String, GroupFunction)*)
    extends PipelineOperator
    with Product2[String, Seq[(String, GroupFunction)]]
    with Serializable {

    val makePipe = Group(builder.string("$" + idField))(ops: _*).makePipe

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = idField

    @deprecated("No longer a case class", "0.20.3")
    @inline def _2 = ops

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: GroupField => true
      case _             => false
    }

    private[api] lazy val tupled = idField -> ops

    override def equals(that: Any): Boolean = that match {
      case other: GroupField =>
        this.tupled == other.tupled

      case _ => false
    }

    @inline override def hashCode: Int = tupled.hashCode

    override def toString = s"GroupField${tupled.toString}"
  }

  object GroupField {
    def apply(idField: String)(ops: (String, GroupFunction)*): GroupField =
      new GroupField(idField)(ops: _*)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(other: GroupField): Option[String] =
      Option(other).map(_.idField)

  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/group/#_S_group \$group]]s documents together to calculate aggregates on document collections.
   * This command aggregates on multiple fields, and they must be named.
   *
   * @param idFields The fields to aggregate on, and the names they should be aggregated under.
   * @param ops the sequence of operators specifying aggregate calculation
   */
  class GroupMulti private[api] (val idFields: (String, String)*)(
    val ops: (String, GroupFunction)*) extends PipelineOperator
    with Product2[Seq[(String, String)], Seq[(String, GroupFunction)]]
    with Serializable {

    val makePipe = Group(builder.document(idFields.map {
      case (alias, attribute) =>
        builder.elementProducer(alias, builder.string("$" + attribute))
    }))(ops: _*).makePipe

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = idFields

    @deprecated("No longer a case class", "0.20.3")
    @inline def _2 = ops

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: GroupMulti => true
      case _             => false
    }

    private[api] lazy val tupled = idFields -> ops

    override def equals(that: Any): Boolean = that match {
      case other: GroupMulti =>
        this.tupled == other.tupled

      case _ => false
    }

    @inline override def hashCode: Int = tupled.hashCode

    override def toString = s"GroupMulti${tupled.toString}"
  }

  object GroupMulti {

    def apply(idFields: Seq[(String, String)])(ops: (String, GroupFunction)*): GroupMulti = new GroupMulti(idFields: _*)(ops: _*)

    @deprecated("No longer a case class", "0.20.3")
    def unapplySeq(other: GroupMulti): Seq[(String, String)] =
      other.idFields

  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/indexStats/ \$indexStats]] aggregation stage.
   *
   * @since MongoDB 3.2
   */
  case object IndexStats extends PipelineOperator {
    val makePipe: pack.Document = pipe(f"$$indexStats", builder.document(Nil))
  }

  /**
   * @param ops the number of operations that used the index
   * @param since the time from which MongoDB gathered the statistics
   */
  class IndexStatAccesses private[api] (
    val ops: Long,
    val since: Long) extends Product2[Long, Long] with Serializable {

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = ops

    @deprecated("No longer a case class", "0.20.3")
    @inline def _2 = since

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: IndexStatAccesses => true
      case _                    => false
    }

    private[api] lazy val tupled = ops -> since

    override def equals(that: Any): Boolean = that match {
      case other: IndexStatAccesses =>
        this.tupled == other.tupled

      case _ => false
    }

    @inline override def hashCode: Int = tupled.hashCode

    override def toString = s"IndexStatAccesses${tupled.toString}"
  }

  object IndexStatAccesses extends AbstractFunction2[Long, Long, IndexStatAccesses] {

    def apply(ops: Long, since: Long): IndexStatAccesses =
      new IndexStatAccesses(ops, since)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(other: IndexStatAccesses): Option[(Long, Long)] =
      Option(other).map { i => i.ops -> i.since }

  }

  /**
   * @param name the index name
   * @param key the key specification
   * @param host the hostname and port of the mongod
   * @param accesses the index statistics
   */
  class IndexStatsResult private[api] (
    val name: String,
    val key: pack.Document,
    val host: String,
    val accesses: IndexStatAccesses) extends Product4[String, pack.Document, String, IndexStatAccesses] with Serializable {

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = name

    @deprecated("No longer a case class", "0.20.3")
    @inline def _2 = key

    @deprecated("No longer a case class", "0.20.3")
    @inline def _3 = host

    @deprecated("No longer a case class", "0.20.3")
    @inline def _4 = accesses

    def canEqual(that: Any): Boolean = that match {
      case _: IndexStatsResult => true
      case _                   => false
    }

    private[api] lazy val tupled =
      Tuple4(name, key, host, accesses)

    override def equals(that: Any): Boolean = that match {
      case other: IndexStatsResult =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"IndexStatsResult${tupled.toString}"
  }

  object IndexStatsResult extends AbstractFunction4[String, pack.Document, String, IndexStatAccesses, IndexStatsResult] {
    def apply(
      name: String,
      key: pack.Document,
      host: String,
      accesses: IndexStatAccesses): IndexStatsResult =
      new IndexStatsResult(name, key, host, accesses)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(res: IndexStatsResult) = Option(res).map(_.tupled)
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/limit/#_S_limit \$limit]]s the number of documents that pass through the stream.
   *
   * @param limit the number of documents to allow through
   */
  class Limit private[api] (val limit: Int) extends PipelineOperator
    with Product1[Int] with Serializable {

    val makePipe: pack.Document = pipe(f"$$limit", builder.int(limit))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = limit

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Limit => true
      case _        => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Limit =>
        this.limit == other.limit

      case _ =>
        false
    }

    override def hashCode: Int = this.limit

    override def toString: String = s"Limit(${limit})"
  }

  object Limit extends AbstractFunction1[Int, Limit] {
    def apply(limit: Int): Limit = new Limit(limit)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(limit: Limit): Option[Int] = Option(limit).map(_.limit)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/listLocalSessions/ \$listLocalSessions]] aggregation stage.
   *
   * @since MongoDB 3.6
   * @param expression
   */
  class ListLocalSessions(
    val expression: pack.Document) extends PipelineOperator {
    def makePipe: pack.Document = pipe(f"$$listLocalSessions", expression)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = expression

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: ListLocalSessions => true
      case _                    => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: ListLocalSessions =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression.
          equals(other.expression))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"ListLocalSessions(${pack pretty expression})"
  }

  object ListLocalSessions extends AbstractFunction1[pack.Document, ListLocalSessions] {
    def apply(expression: pack.Document): ListLocalSessions =
      new ListLocalSessions(expression)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(listLocalSessions: ListLocalSessions): Option[pack.Document] =
      Option(listLocalSessions).map(_.expression)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/listSessions/ \$listSessions]] aggregation stage.
   *
   * @since MongoDB 3.6
   * @param expression
   */
  class ListSessions(
    val expression: pack.Document) extends PipelineOperator {
    def makePipe: pack.Document = pipe(f"$$listSessions", expression)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = expression

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: ListSessions => true
      case _               => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: ListSessions =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression.
          equals(other.expression))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"ListSessions(${pack pretty expression})"
  }

  object ListSessions extends AbstractFunction1[pack.Document, ListSessions] {
    def apply(expression: pack.Document): ListSessions =
      new ListSessions(expression)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(listSessions: ListSessions): Option[pack.Document] =
      Option(listSessions).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/graphLookup/ \$graphLookup]] aggregation stage.
   *
   * @since MongoDB 3.4
   * @param from the target collection for the \$graphLookup operation to search
   * @param startWith the expression that specifies the value of the `connectFromField` with which to start the recursive search
   * @param connectFromField the field name whose value `\$graphLookup` uses to recursively match against the `connectToField` of other documents in the collection
   * @param connectToField the field name in other documents against which to match the value of the field specified by the `connectFromField` parameter
   * @param as the name of the array field added to each output document
   * @param maxDepth an optional non-negative integral number specifying the maximum recursion depth
   * @param depthField an optional name for a field to add to each traversed document in the search path
   * @param restrictSearchWithMatch an optional filter expression
   */
  class GraphLookup private[api] (
    val from: String,
    val startWith: pack.Value,
    val connectFromField: String,
    val connectToField: String,
    val as: String,
    val maxDepth: Option[Int],
    val depthField: Option[String],
    val restrictSearchWithMatch: Option[pack.Value]) extends PipelineOperator with Product8[String, pack.Value, String, String, String, Option[Int], Option[String], Option[pack.Value]] with Serializable {
    import builder.{ document, elementProducer => element, string }

    val makePipe: pack.Document = pipe(f"$$graphLookup", document(options))

    @inline private def options = {
      val opts = Seq.newBuilder[pack.ElementProducer]

      opts ++= Seq(
        element("from", string(from)),
        element("startWith", startWith),
        element("connectFromField", string(connectFromField)),
        element("connectToField", string(connectToField)),
        element("as", string(as)))

      maxDepth.foreach { i =>
        opts += element("maxDepth", builder.int(i))
      }

      depthField.foreach { f =>
        opts += element("depthField", string(f))
      }

      restrictSearchWithMatch.foreach { e =>
        opts += element("restrictSearchWithMatch", e)
      }

      opts.result()
    }

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = from

    @deprecated("No longer a case class", "0.20.3")
    @inline def _2 = startWith

    @deprecated("No longer a case class", "0.20.3")
    @inline def _3 = connectFromField

    @deprecated("No longer a case class", "0.20.3")
    @inline def _4 = connectToField

    @deprecated("No longer a case class", "0.20.3")
    @inline def _5 = as

    @deprecated("No longer a case class", "0.20.3")
    @inline def _6 = maxDepth

    @deprecated("No longer a case class", "0.20.3")
    @inline def _7 = depthField

    @deprecated("No longer a case class", "0.20.3")
    @inline def _8 = restrictSearchWithMatch

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: GraphLookup => true
      case _              => false
    }

    private[api] lazy val tupled = Tuple8(from, startWith, connectFromField, connectToField, as, maxDepth, depthField, restrictSearchWithMatch)

    override def equals(that: Any): Boolean = that match {
      case other: GraphLookup =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    @inline override def hashCode: Int = tupled.hashCode

    override def toString = s"GraphLookup${tupled.toString}"
  }

  object GraphLookup extends scala.runtime.AbstractFunction8[String, pack.Value, String, String, String, Option[Int], Option[String], Option[pack.Value], GraphLookup] {
    def apply(
      from: String,
      startWith: pack.Value,
      connectFromField: String,
      connectToField: String,
      as: String,
      maxDepth: Option[Int] = None,
      depthField: Option[String] = None,
      restrictSearchWithMatch: Option[pack.Value] = None): GraphLookup =
      new GraphLookup(from, startWith, connectFromField, connectToField,
        as, maxDepth, depthField, restrictSearchWithMatch)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(stage: GraphLookup) = Option(stage).map(_.tupled)
  }

  /**
   * Performs a [[https://docs.mongodb.com/v3.2/reference/operator/aggregation/lookup/#pipe._S_lookup left outer join]] to an unsharded collection in the same database to filter in documents from the "joined" collection for processing.
   *
   * @since MongoDB 3.2
   * @param from the collection to perform the join with
   * @param localField the field from the documents input
   * @param foreignField the field from the documents in the `from` collection
   * @param as the name of the new array field to add to the input documents
   */
  class Lookup private[api] (
    val from: String,
    val localField: String,
    val foreignField: String,
    val as: String) extends PipelineOperator with Product4[String, String, String, String] with Serializable {

    import builder.{ document, elementProducer => element, string }
    val makePipe: pack.Document = document(Seq(
      element(f"$$lookup", document(Seq(
        element("from", string(from)),
        element("localField", string(localField)),
        element("foreignField", string(foreignField)),
        element("as", string(as)))))))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = from

    @deprecated("No longer a case class", "0.20.3")
    @inline def _2 = localField

    @deprecated("No longer a case class", "0.20.3")
    @inline def _3 = foreignField

    @deprecated("No longer a case class", "0.20.3")
    @inline def _4 = as

    def canEqual(that: Any): Boolean = that match {
      case _: Lookup => true
      case _         => false
    }

    private[api] lazy val tupled =
      Tuple4(from, localField, foreignField, as)

    override def equals(that: Any): Boolean = that match {
      case other: Lookup =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"Lookup${tupled.toString}"
  }

  object Lookup extends AbstractFunction4[String, String, String, String, Lookup] {
    def apply(
      from: String,
      localField: String,
      foreignField: String,
      as: String): Lookup =
      new Lookup(from, localField, foreignField, as)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(res: Lookup) = Option(res).map(_.tupled)
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/match/#_S_match Filters]] out documents from the stream that do not match the predicate.
   *
   * @param predicate the query that documents must satisfy to be in the stream
   */
  class Match private[api] (val predicate: pack.Document)
    extends PipelineOperator
    with Product1[pack.Document] with Serializable {

    val makePipe: pack.Document = pipe(f"$$match", predicate)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = predicate

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Match => true
      case _        => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Match =>
        (this.predicate == null && other.predicate == null) || (
          this.predicate != null && this.predicate.
          equals(other.predicate))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (predicate == null) -1 else predicate.hashCode

    override def toString: String = s"Match(${pack pretty predicate})"
  }

  object Match extends AbstractFunction1[pack.Document, Match] {
    def apply(predicate: pack.Document): Match = new Match(predicate)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(`match`: Match): Option[pack.Document] =
      Option(`match`).map(_.predicate)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/merge/ \$merge]] aggregation stage.
   *
   * @since MongoDB 4.2
   * @param intoDb the name of the `into` database
   * @param intoCollection the name of the `into` collection
   */
  class Merge private[api] (
    val intoDb: String,
    val intoCollection: String,
    val on: Seq[String],
    val whenMatched: Option[String],
    val let: Option[pack.Document],
    val whenNotMatched: Option[String]) extends PipelineOperator
    with Product6[String, String, Seq[String], Option[String], Option[pack.Document], Option[String]] with Serializable {

    import builder.{ elementProducer => element, string }

    def makePipe: pack.Document = {
      val opts = Seq.newBuilder[pack.ElementProducer]

      opts += element("into", string(f"${intoDb}.${intoCollection}"))

      on.headOption.foreach { o1 =>
        opts += element("on", builder.array(string(o1), on.tail.map(string)))
      }

      whenMatched.foreach { w =>
        opts += element("whenMatched", string(w))
      }

      let.foreach { l =>
        opts += element("let", l)
      }

      whenNotMatched.foreach { w =>
        opts += element("whenNotMatched", string(w))
      }

      pipe(f"$$merge", builder.document(opts.result()))
    }

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = intoDb

    @deprecated("No longer a case class", "0.20.3")
    @inline def _2 = intoCollection

    @deprecated("No longer a case class", "0.20.3")
    @inline def _3 = on

    @deprecated("No longer a case class", "0.20.3")
    @inline def _4 = whenMatched

    @deprecated("No longer a case class", "0.20.3")
    @inline def _5 = let

    @deprecated("No longer a case class", "0.20.3")
    @inline def _6 = whenNotMatched

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Merge => true
      case _        => false
    }

    private[api] lazy val tupled = Tuple6(intoDb, intoCollection, on, whenMatched, let, whenNotMatched)

    override def equals(that: Any): Boolean = that match {
      case other: Merge =>
        this.tupled == other.tupled

      case _ => false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"Merge${tupled.toString}"
  }

  object Merge extends scala.runtime.AbstractFunction6[String, String, Seq[String], Option[String], Option[pack.Document], Option[String], Merge] {
    def apply(
      intoDb: String,
      intoCollection: String,
      on: Seq[String],
      whenMatched: Option[String],
      let: Option[pack.Document],
      whenNotMatched: Option[String]): Merge =
      new Merge(intoDb, intoCollection, on, whenMatched, let, whenNotMatched)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(merge: Merge) = Option(merge).map(_.tupled)
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/operator/aggregation/out/#pipe._S_out Takes the documents]] returned by the aggregation pipeline and writes them to a specified collection
   *
   * @param collection the name of the output collection
   */
  class Out private[api] (val collection: String) extends PipelineOperator
    with Product1[String] with Serializable {

    def makePipe: pack.Document = pipe(f"$$out", builder.string(collection))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = collection

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Out => true
      case _      => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Out =>
        (this.collection == null && other.collection == null) || (
          this.collection != null && this.collection.
          equals(other.collection))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (collection == null) -1 else collection.hashCode

    override def toString: String = s"Out(${collection})"
  }

  object Out extends AbstractFunction1[String, Out] {
    def apply(collection: String): Out = new Out(collection)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(out: Out): Option[String] =
      Option(out).map(_.collection)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/planCacheStats/ \$planCacheStats]] aggregation stage.
   *
   * @since MongoDB 4.2
   */
  case object PlanCacheStats extends PipelineOperator {
    val makePipe = pipe(f"$$planCacheStats", builder.document(Seq.empty))
  }

  /**
   * Reshapes a document stream by renaming, adding, or removing fields.
   * Also uses [[http://docs.mongodb.org/manual/reference/aggregation/project/#_S_project Project]] to create computed values or sub-objects.
   *
   * @param specifications The fields to include. The resulting objects will contain only these fields.
   */
  class Project private[api] (val specifications: pack.Document)
    extends PipelineOperator with Product1[pack.Document] with Serializable {

    val makePipe: pack.Document = pipe(f"$$project", specifications)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = specifications

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Project => true
      case _          => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Project =>
        (this.specifications == null && other.specifications == null) || (
          this.specifications != null && this.specifications.
          equals(other.specifications))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (specifications == null) -1 else specifications.hashCode

    override def toString: String = s"Project(${pack pretty specifications})"
  }

  object Project extends AbstractFunction1[pack.Document, Project] {
    def apply(specifications: pack.Document): Project =
      new Project(specifications)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(project: Project): Option[pack.Document] =
      Option(project).map(_.specifications)
  }

  /**
   * Restricts the contents of the documents based on information stored in the documents themselves.
   * http://docs.mongodb.org/manual/reference/operator/aggregation/redact/#pipe._S_redact Redact
   * @param expression the redact expression
   */
  class Redact private[api] (val expression: pack.Document)
    extends PipelineOperator with Product1[pack.Document] with Serializable {

    val makePipe: pack.Document = pipe(f"$$redact", expression)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = expression

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Redact => true
      case _         => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Redact =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression.
          equals(other.expression))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"Redact(${pack pretty expression})"
  }

  object Redact extends AbstractFunction1[pack.Document, Redact] {
    def apply(expression: pack.Document): Redact =
      new Redact(expression)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(redact: Redact): Option[pack.Document] =
      Option(redact).map(_.expression)
  }

  /**
   * Promotes a specified document to the top level and replaces all other fields.
   * The operation replaces all existing fields in the input document, including the _id field.
   * https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot
   * @param newRoot The field name to become the new root
   */
  class ReplaceRootField private[api] (val newRoot: String)
    extends PipelineOperator with Product1[String] with Serializable {

    val makePipe: pack.Document =
      pipe(f"$$replaceRoot", pipe("newRoot", builder.string("$" + newRoot)))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = newRoot

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: ReplaceRootField => true
      case _                   => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: ReplaceRootField =>
        (this.newRoot == null && other.newRoot == null) || (
          this.newRoot != null && this.newRoot.
          equals(other.newRoot))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (newRoot == null) -1 else newRoot.hashCode

    override def toString: String = s"ReplaceRootField(${newRoot})"
  }

  object ReplaceRootField extends AbstractFunction1[String, ReplaceRootField] {
    def apply(newRoot: String): ReplaceRootField =
      new ReplaceRootField(newRoot)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(replaceRootField: ReplaceRootField): Option[String] =
      Option(replaceRootField).map(_.newRoot)
  }

  /**
   * Promotes a specified document to the top level and replaces all other fields.
   * The operation replaces all existing fields in the input document, including the _id field.
   * https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot
   * @param newRoot The new root object
   */
  class ReplaceRoot private[api] (val newRoot: pack.Document)
    extends PipelineOperator with Product1[pack.Document] with Serializable {

    val makePipe: pack.Document =
      pipe(f"$$replaceRoot", pipe("newRoot", newRoot))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = newRoot

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: ReplaceRoot => true
      case _              => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: ReplaceRoot =>
        (this.newRoot == null && other.newRoot == null) || (
          this.newRoot != null && this.newRoot.
          equals(other.newRoot))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (newRoot == null) -1 else newRoot.hashCode

    override def toString: String = s"ReplaceRoot(${pack pretty newRoot})"
  }

  object ReplaceRoot extends AbstractFunction1[pack.Document, ReplaceRoot] {
    def apply(newRoot: pack.Document): ReplaceRoot =
      new ReplaceRoot(newRoot)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(replaceRoot: ReplaceRoot): Option[pack.Document] =
      Option(replaceRoot).map(_.newRoot)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/replaceWith/ \$replaceWith]] aggregation stage.
   *
   * @since MongoDB 4.2
   * @param replacementDocument
   */
  class ReplaceWith private[api] (val replacementDocument: pack.Document)
    extends PipelineOperator
    with Product1[pack.Document] with Serializable {

    def makePipe: pack.Document = pipe(f"$$replaceWith", replacementDocument)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = replacementDocument

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: ReplaceWith => true
      case _              => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: ReplaceWith =>
        (this.replacementDocument == null &&
          other.replacementDocument == null) || (
            this.replacementDocument != null && this.replacementDocument.
            equals(other.replacementDocument))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (replacementDocument == null) -1 else replacementDocument.hashCode

    override def toString: String = s"ReplaceWith(${pack pretty replacementDocument})"
  }

  object ReplaceWith
    extends AbstractFunction1[pack.Document, ReplaceWith] {

    def apply(replacementDocument: pack.Document): ReplaceWith =
      new ReplaceWith(replacementDocument)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(replaceWith: ReplaceWith): Option[pack.Document] =
      Option(replaceWith).map(_.replacementDocument)
  }

  /**
   * [[https://docs.mongodb.org/master/reference/operator/aggregation/sample/ \$sample]] aggregation stage, that randomly selects the specified number of documents from its input.
   *
   * @param size the number of documents to return
   */
  class Sample private[api] (val size: Int) extends PipelineOperator
    with Product1[Int] with Serializable {

    val makePipe: pack.Document =
      pipe(f"$$sample", pipe("size", builder.int(size)))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = size

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Sample => true
      case _         => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Sample =>
        this.size == other.size

      case _ =>
        false
    }

    override def hashCode: Int = this.size

    override def toString: String = s"Sample(${size})"
  }

  object Sample extends AbstractFunction1[Int, Sample] {
    def apply(size: Int): Sample = new Sample(size)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(size: Sample): Option[Int] = Option(size).map(_.size)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/set/ \$set]] aggregation stage
   */
  class Set private[api] (val expression: pack.Document)
    extends PipelineOperator with Product1[pack.Document] with Serializable {

    def makePipe: pack.Document = pipe(f"$$set", expression)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = expression

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Set => true
      case _      => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Set =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression.
          equals(other.expression))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"Set(${pack pretty expression})"
  }

  object Set extends AbstractFunction1[pack.Document, Set] {
    def apply(expression: pack.Document): Set =
      new Set(expression)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(set: Set): Option[pack.Document] =
      Option(set).map(_.expression)
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/skip/#_S_skip \$skip]]s over a number of documents before passing all further documents along the stream.
   *
   * @param skip the number of documents to skip
   */
  class Skip private[api] (val skip: Int) extends PipelineOperator
    with Product1[Int] with Serializable {

    val makePipe: pack.Document = builder.document(Seq(
      builder.elementProducer(f"$$skip", builder.int(skip))))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = skip

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Skip => true
      case _       => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Skip =>
        this.skip == other.skip

      case _ =>
        false
    }

    override def hashCode: Int = this.skip

    override def toString: String = s"Skip(${skip})"
  }

  object Skip extends AbstractFunction1[Int, Skip] {
    def apply(skip: Int): Skip = new Skip(skip)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(skip: Skip): Option[Int] = Option(skip).map(_.skip)
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/sort/#_S_sort \$sort]]s the stream based on the given fields.
   *
   * @param fields the fields to sort by
   */
  class Sort private[api] (val fields: Seq[SortOrder]) extends PipelineOperator
    with Product1[Seq[SortOrder]] with Serializable {

    import builder.{ document, elementProducer => element }
    val makePipe: pack.Document = document(Seq(
      element(f"$$sort", document(fields.map {
        case Ascending(field)  => element(field, builder.int(1))

        case Descending(field) => element(field, builder.int(-1))

        case MetadataSort(field, keyword) => {
          val meta = document(Seq(
            element(f"$$meta", builder.string(keyword.name))))

          element(field, meta)
        }
      }))))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = fields

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Sort => true
      case _       => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: Sort =>
        (this.fields == null && other.fields == null) || (
          this.fields != null && this.fields.equals(other.fields))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (fields == null) -1 else fields.hashCode

    override def toString: String = s"Sort(${fields})"
  }

  object Sort {
    def apply(fields: SortOrder*): Sort = new Sort(fields)

    @deprecated("No longer a case class", "0.20.3")
    def unapplySeq(sort: Sort): Seq[SortOrder] = sort.fields
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/sortByCount/ \$sortByCount]] aggregation stage.
   *
   * @since MongoDB 3.4
   * @param expression
   */
  class SortByCount private[api] (val expression: pack.Value)
    extends PipelineOperator with Product1[pack.Value] with Serializable {

    def makePipe: pack.Document = pipe(f"$$sortByCount", expression)

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = expression

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: SortByCount => true
      case _              => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: SortByCount =>
        (this.expression == null && other.expression == null) || (
          this.expression != null && this.expression.
          equals(other.expression))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (expression == null) -1 else expression.hashCode

    override def toString: String = s"SortByCount(${expression})"
  }

  object SortByCount extends AbstractFunction1[pack.Value, SortByCount] {
    def apply(expression: pack.Value): SortByCount =
      new SortByCount(expression)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(sortByCount: SortByCount): Option[pack.Value] =
      Option(sortByCount).map(_.expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/sortByCount/ \$sortByCount]] aggregation stage.
   *
   * @since MongoDB 3.4
   * @param field the field name
   */
  class SortByFieldCount private[api] (val field: String)
    extends PipelineOperator
    with Product1[String] with Serializable {

    def makePipe: pack.Document =
      pipe(f"$$sortByCount", builder.string("$" + field))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = field

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: SortByFieldCount => true
      case _                   => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: SortByFieldCount =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          equals(other.field))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"SortByFieldCount(${field})"
  }

  object SortByFieldCount extends AbstractFunction1[String, SortByFieldCount] {
    def apply(field: String): SortByFieldCount =
      new SortByFieldCount(field)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(sortByFieldCount: SortByFieldCount): Option[String] =
      Option(sortByFieldCount).map(_.field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/unset/ \$unset]] aggregation stage.
   *
   * @since MongoDB 4.2
   * @param field the field name
   * @param otherFields
   */
  class Unset private[api] (
    val field: String,
    val otherFields: Seq[String]) extends PipelineOperator
    with Product2[String, Seq[String]] with Serializable {

    def makePipe: pack.Document = pipe(f"$$unset", builder.array(
      builder.string(field), otherFields.map(builder.string)))

    @deprecated("No longer a case class", "0.20.3")
    @inline def _1 = field

    @deprecated("No longer a case class", "0.20.3")
    @inline def _2 = otherFields

    @deprecated("No longer a case class", "0.20.3")
    def canEqual(that: Any): Boolean = that match {
      case _: Unset => true
      case _        => false
    }

    private[api] lazy val tupled = field -> otherFields

    override def equals(that: Any): Boolean = that match {
      case other: Unset =>
        this.tupled == other.tupled

      case _ => false
    }

    @inline override def hashCode: Int = tupled.hashCode

    override def toString = s"Unset${tupled.toString}"
  }

  object Unset extends AbstractFunction2[String, Seq[String], Unset] {

    def apply(field: String, otherFields: Seq[String]): Unset =
      new Unset(field, otherFields)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(other: Unset): Option[(String, Seq[String])] =
      Option(other).map { i => i.field -> i.otherFields }

  }

  class Unwind private[commands] (
    val productArity: Int,
    element: Int => Any,
    operator: => pack.Document) extends PipelineOperator with Product
    with Serializable {

    val makePipe: pack.Document = operator

    final def canEqual(that: Any): Boolean = that.isInstanceOf[Unwind]

    final def productElement(n: Int) = element(n)
  }

  /**
   * Turns a document with an array into multiple documents,
   * one document for each element in the array.
   * http://docs.mongodb.org/manual/reference/aggregation/unwind/#_S_unwind
   * @param field the name of the array to unwind
   */
  class UnwindField(val field: String) extends Unwind(1, { case 0 => field },
    pipe(f"$$unwind", builder.string("$" + field)))
    with Serializable {

    override def equals(that: Any): Boolean = that match {
      case other: UnwindField =>
        (this.field == null && other.field == null) || (
          this.field != null && this.field.
          equals(other.field))

      case _ =>
        false
    }

    override def hashCode: Int =
      if (field == null) -1 else field.hashCode

    override def toString: String = s"UnwindField(${field})"
  }

  object UnwindField extends AbstractFunction1[String, UnwindField] {
    def apply(field: String): UnwindField =
      new UnwindField(field)

    @deprecated("No longer a case class", "0.20.3")
    def unapply(unwindField: UnwindField): Option[String] =
      Option(unwindField).map(_.field)
  }

  object Unwind {
    /**
     * Turns a document with an array into multiple documents,
     * one document for each element in the array.
     * http://docs.mongodb.org/manual/reference/aggregation/unwind/#_S_unwind
     * @param field the name of the array to unwind
     */
    @deprecated("Use [[AggregationFramework#UnwindField]]", "0.12.0")
    def apply(field: String): Unwind = UnwindField(field)

    /**
     * Turns a document with an array into multiple documents,
     * one document for each element in the array.
     *
     * @since MongoDB 3.2
     * @param path the field path to an array field (without the `\$` prefix)
     * @param includeArrayIndex the name of a new field to hold the array index of the element
     */
    def apply(
      path: String,
      includeArrayIndex: Option[String],
      preserveNullAndEmptyArrays: Option[Boolean]): Unwind = Full(path, includeArrayIndex, preserveNullAndEmptyArrays)

    def unapply(that: Unwind): Option[String] = that match {
      case UnwindField(field) => Some(field)
      case Full(path, _, _)   => Some(path)
    }

    /**
     * @param path the field path to an array field (without the `\$` prefix)
     * @param includeArrayIndex the name of a new field to hold the array index of the element
     */
    private case class Full(
      path: String,
      includeArrayIndex: Option[String],
      preserveNullAndEmptyArrays: Option[Boolean]) extends Unwind(3, {
      case 1 => path
      case 2 => includeArrayIndex
      case 3 => preserveNullAndEmptyArrays
    }, pipe(f"$$unwind", builder.document {
      import builder.{ elementProducer => element }
      val elms = Seq.newBuilder[pack.ElementProducer]

      elms += element("path", builder.string("$" + path))

      includeArrayIndex.foreach { include =>
        elms += element("includeArrayIndex", builder.string(include))
      }

      preserveNullAndEmptyArrays.foreach { preserve =>
        elms += element(
          "preserveNullAndEmptyArrays", builder.boolean(preserve))
      }

      elms.result()
    }))
  }

  /**
   * The [[https://docs.mongodb.com/master/reference/operator/aggregation/filter/ \$filter]] aggregation stage.
   *
   * @param input the expression that resolves to an array
   * @param as The variable name for the element in the input array. The as expression accesses each element in the input array by this variable.
   * @param cond the expression that determines whether to include the element in the resulting array
   */
  @deprecated("Not a pipeline operator (stage)", "0.19.4")
  case class Filter(input: pack.Value, as: String, cond: pack.Document)
    extends PipelineOperator {
    import builder.{ document, elementProducer => element }
    val makePipe: pack.Document = pipe(f"$$filter", document(Seq(
      element("input", input),
      element("as", builder.string(as)),
      element("cond", cond))))
  }

  /** Filter companion */
  object Filter {
    implicit val writer: pack.Writer[Filter] =
      pack.writer[Filter] { _.makePipe }
  }

  // Change Stream
  //
  // Specification:
  // https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.rst#server-specification

  /**
   * Low level pipeline operator which allows to open a tailable cursor
   * against subsequent [[https://docs.mongodb.com/manual/reference/change-events/ change events]] of a given collection.
   *
   * For common use-cases, you might prefer to use the `watch`
   * operator on a collection.
   *
   * '''Note:''' the target mongo instance MUST be a replica-set
   * (even in the case of a single node deployement).
   *
   * @since MongoDB 3.6
   * @param resumeAfter the id of the last known change event, if any. The stream will resume just after that event.
   * @param startAtOperationTime the operation time before which all change events are known. Must be in the time range of the oplog. (since MongoDB 4.0)
   * @param fullDocumentStrategy if set to UpdateLookup, every update change event will be joined with the ''current'' version of the relevant document.
   */
  final class ChangeStream(
    resumeAfter: Option[pack.Value] = None,
    startAtOperationTime: Option[pack.Value] = None, // TODO#1.1: restrict to something more like a timestamp?
    fullDocumentStrategy: Option[ChangeStreams.FullDocumentStrategy] = None) extends PipelineOperator {

    def makePipe: pack.Document = pipe(f"$$changeStream", builder.document(Seq(
      resumeAfter.map(v => builder.elementProducer("resumeAfter", v)),
      startAtOperationTime.map(v => builder.elementProducer("startAtOperationTime", v)),
      fullDocumentStrategy.map(v => builder.elementProducer("fullDocument", builder.string(v.name)))).flatten))
  }
}
