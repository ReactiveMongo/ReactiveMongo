package reactivemongo.api.commands

import reactivemongo.api.{ ChangeStreams, PackSupport, SerializationPack }

/**
 * Implements the [[http://docs.mongodb.org/manual/applications/aggregation/ Aggregation Framework]].
 *
 * @see [[PipelineOperator]]
 */
private[reactivemongo] trait AggregationFramework[P <: SerializationPack]
  extends GroupAggregation[P] with SliceAggregation[P] with SortAggregation[P]
  with AggregationPipeline[P] { self: PackSupport[P] =>

  protected final lazy val builder = pack.newBuilder

  @inline protected final def pipe(name: String, arg: pack.Value): pack.Document = builder.document(Seq(builder.elementProducer(name, arg)))

  /**
   * @param batchSize the initial batch size for the cursor
   */
  protected final class Cursor private[api] (val batchSize: Int) {
    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.batchSize == other.batchSize

      case _ =>
        false
    }

    override def hashCode: Int = batchSize

    override def toString: String = s"Cursor(${batchSize})"
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/addFields/ \$addFields]] stage.
   *
   * @since MongoDB 3.4
   * @param specifications The fields to include.
   * The resulting objects will also contain these fields.
   */
  final class AddFields private[api] (val specifications: pack.Document)
    extends PipelineOperator {

    protected[reactivemongo] val makePipe = pipe(f"$$addFields", specifications)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  object AddFields {
    def apply(specifications: pack.Document): AddFields =
      new AddFields(specifications)

  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/bucket/ \$bucket]] aggregation stage.
   */
  final class Bucket private (
    val groupBy: pack.Value,
    val boundaries: Seq[pack.Value],
    val default: String)(
    val output: (String, GroupFunction)*) extends PipelineOperator {

    import builder.{ document, elementProducer => element }

    protected[reactivemongo] val makePipe: pack.Document = {
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

    private[api] lazy val tupled =
      Tuple4(groupBy, boundaries, default, output)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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
  final class BucketAuto private (
    val groupBy: pack.Value,
    val buckets: Int,
    val granularity: Option[String])(
    val output: (String, GroupFunction)*)
    extends PipelineOperator {

    import builder.{ document, elementProducer => element }

    protected[reactivemongo] val makePipe: pack.Document = {
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

    private[api] lazy val tupled =
      Tuple4(groupBy, buckets, granularity, output)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/collStats/ \$collStats]] aggregation stage.
   */
  final class CollStats private (
    val latencyStatsHistograms: Boolean,
    val storageStatsScale: Option[Double],
    val count: Boolean) extends PipelineOperator {

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

    private[api] lazy val tupled =
      Tuple3(latencyStatsHistograms, storageStatsScale, count)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"CollStats${tupled.toString}"
  }

  object CollStats {
    def apply(
      latencyStatsHistograms: Boolean,
      storageStatsScale: Option[Double],
      count: Boolean): CollStats =
      new CollStats(latencyStatsHistograms, storageStatsScale, count)

  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/count/ \$count]] of the number of documents input.
   *
   * @since MongoDB 3.4
   * @param outputName the name of the output field which has the count as its value
   */
  final class Count private (val outputName: String)
    extends PipelineOperator {

    protected[reactivemongo] val makePipe: pack.Document = pipe(f"$$count", builder.string(outputName))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  object Count {
    def apply(outputName: String): Count = new Count(outputName)
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
  final class CurrentOp private (
    val allUsers: Boolean,
    val idleConnections: Boolean,
    val idleCursors: Boolean,
    val idleSessions: Boolean,
    val localOps: Boolean) extends PipelineOperator {
    import builder.{ boolean, elementProducer => element }
    protected[reactivemongo] val makePipe: pack.Document = pipe(f"$$currentOp", builder.document(Seq(
      element("allUsers", boolean(allUsers)),
      element("idleConnections", boolean(idleConnections)),
      element("idleCursors", boolean(idleCursors)),
      element("idleSessions", boolean(idleSessions)),
      element("localOps", boolean(localOps)))))

    private[api] lazy val tupled =
      Tuple5(allUsers, idleConnections, idleCursors, idleSessions, localOps)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"CurrentOp${tupled.toString}"
  }

  object CurrentOp {
    def apply(
      allUsers: Boolean = false,
      idleConnections: Boolean = false,
      idleCursors: Boolean = false,
      idleSessions: Boolean = true,
      localOps: Boolean = false): CurrentOp = new CurrentOp(
      allUsers, idleConnections, idleCursors, idleSessions, localOps)

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
  final class Facet private (
    val specifications: Iterable[(String, Pipeline)])
    extends PipelineOperator {

    protected[reactivemongo] val makePipe: pack.Document = {
      import builder.{ document, elementProducer => elem }

      val specDoc = document(specifications.map {
        case (name, (firstOp, subOps)) => elem(name, builder.array(
          firstOp.makePipe, subOps.map(_.makePipe)))

      }.toSeq)

      pipe(f"$$facet", specDoc)
    }

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  object Facet {
    def apply(specifications: Iterable[(String, Pipeline)]): Facet =
      new Facet(specifications)
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
  final class GeoNear private (
    val near: pack.Value,
    val spherical: Boolean,
    val limit: Option[Long],
    val minDistance: Option[Long],
    val maxDistance: Option[Long],
    val query: Option[pack.Document],
    val distanceMultiplier: Option[Double],
    val uniqueDocs: Boolean,
    val distanceField: Option[String],
    val includeLocs: Option[String]) extends PipelineOperator {

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

    override def equals(that: Any): Boolean = that match {
      case other: this.type => tupled == other.tupled
      case _                => false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString: String = s"GeoNear${tupled.toString}"

    private[reactivemongo] lazy val tupled = Tuple10(near, spherical, limit, minDistance, maxDistance, query, distanceMultiplier, uniqueDocs, distanceField, includeLocs)
  }

  object GeoNear {
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

  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/group/#_S_group \$group]]s documents together to calculate aggregates on document collections.
   * This command aggregates on arbitrary identifiers.
   * Document fields identifier must be prefixed with `$`.
   *
   * @param identifiers any BSON value acceptable by mongodb as identifier
   * @param ops the sequence of operators specifying aggregate calculation
   */
  final class Group private (val identifiers: pack.Value)(val ops: (String, GroupFunction)*) extends PipelineOperator {

    import builder.{ document, elementProducer => element }

    protected[reactivemongo] val makePipe: pack.Document = pipe(f"$$group", document(
      element("_id", identifiers) +: ops.map({
        case (field, op) => element(field, op.makeFunction)
      })))

    private[api] lazy val tupled = identifiers -> ops

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.tupled == other.tupled

      case _ => false
    }

    @inline override def hashCode: Int = tupled.hashCode

    override def toString = s"Group${tupled.toString}"
  }

  object Group {
    def apply(identifiers: pack.Value)(ops: (String, GroupFunction)*): Group =
      new Group(identifiers)(ops: _*)

  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/group/#_S_group \$group]]s documents together to calculate aggregates on document collections.
   * This command aggregates on one field.
   *
   * @param idField the name of the field to aggregate on
   * @param ops the sequence of operators specifying aggregate calculation
   */
  final class GroupField private (val idField: String)(val ops: (String, GroupFunction)*) extends PipelineOperator {

    protected[reactivemongo] val makePipe = Group(builder.string("$" + idField))(ops: _*).makePipe

    private[api] lazy val tupled = idField -> ops

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.tupled == other.tupled

      case _ => false
    }

    @inline override def hashCode: Int = tupled.hashCode

    override def toString = s"GroupField${tupled.toString}"
  }

  object GroupField {
    def apply(idField: String)(ops: (String, GroupFunction)*): GroupField =
      new GroupField(idField)(ops: _*)

  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/group/#_S_group \$group]]s documents together to calculate aggregates on document collections.
   * This command aggregates on multiple fields, and they must be named.
   *
   * @param idFields The fields to aggregate on, and the names they should be aggregated under.
   * @param ops the sequence of operators specifying aggregate calculation
   */
  final class GroupMulti private (val idFields: (String, String)*)(
    val ops: (String, GroupFunction)*) extends PipelineOperator {

    protected[reactivemongo] val makePipe = Group(builder.document(idFields.map {
      case (alias, attribute) =>
        builder.elementProducer(alias, builder.string("$" + attribute))
    }))(ops: _*).makePipe

    private[api] lazy val tupled = idFields -> ops

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.tupled == other.tupled

      case _ => false
    }

    @inline override def hashCode: Int = tupled.hashCode

    override def toString = s"GroupMulti${tupled.toString}"
  }

  object GroupMulti {
    def apply(idFields: Seq[(String, String)])(ops: (String, GroupFunction)*): GroupMulti = new GroupMulti(idFields: _*)(ops: _*)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/indexStats/ \$indexStats]] aggregation stage.
   *
   * @since MongoDB 3.2
   */
  final case object IndexStats extends PipelineOperator {
    protected[reactivemongo] val makePipe: pack.Document = pipe(f"$$indexStats", builder.document(Nil))
  }

  /**
   * @param ops the number of operations that used the index
   * @param since the time from which MongoDB gathered the statistics
   */
  final class IndexStatAccesses private[api] (
    val ops: Long,
    val since: Long) {

    private[api] lazy val tupled = ops -> since

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.tupled == other.tupled

      case _ => false
    }

    @inline override def hashCode: Int = tupled.hashCode

    override def toString = s"IndexStatAccesses${tupled.toString}"
  }

  /**
   * @param name the index name
   * @param key the key specification
   * @param host the hostname and port of the mongod
   * @param accesses the index statistics
   */
  final class IndexStatsResult private (
    val name: String,
    val key: pack.Document,
    val host: String,
    val accesses: IndexStatAccesses) {

    private[api] lazy val tupled =
      Tuple4(name, key, host, accesses)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"IndexStatsResult${tupled.toString}"
  }

  object IndexStatsResult {
    def unapply(res: IndexStatsResult) = Option(res).map(_.tupled)

    implicit def reader: pack.Reader[IndexStatsResult] = {
      val decoder = pack.newDecoder

      val accessReader = pack.reader[IndexStatAccesses] { doc =>
        (for {
          ops <- decoder.long(doc, "ops")
          since <- decoder.long(doc, "since")
        } yield new IndexStatAccesses(ops, since)).get
      }

      pack.reader[IndexStatsResult] { doc =>
        (for {
          name <- decoder.string(doc, "name")
          key <- decoder.child(doc, "key")
          host <- decoder.string(doc, "host")
          accesses <- decoder.child(doc, "accesses").map {
            pack.deserialize(_, accessReader)
          }
        } yield new IndexStatsResult(name, key, host, accesses)).get
      }
    }
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/limit/#_S_limit \$limit]]s the number of documents that pass through the stream.
   *
   * @param limit the number of documents to allow through
   */
  final class Limit private (val limit: Int) extends PipelineOperator {
    protected[reactivemongo] val makePipe: pack.Document =
      pipe(f"$$limit", builder.int(limit))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.limit == other.limit

      case _ =>
        false
    }

    override def hashCode: Int = this.limit

    override def toString: String = s"Limit(${limit})"
  }

  object Limit {
    def apply(limit: Int): Limit = new Limit(limit)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/listLocalSessions/ \$listLocalSessions]] aggregation stage.
   *
   * @since MongoDB 3.6
   * @param expression
   */
  final class ListLocalSessions private (
    val expression: pack.Document) extends PipelineOperator {
    def makePipe: pack.Document = pipe(f"$$listLocalSessions", expression)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  object ListLocalSessions {
    def apply(expression: pack.Document): ListLocalSessions =
      new ListLocalSessions(expression)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/listSessions/ \$listSessions]] aggregation stage.
   *
   * @since MongoDB 3.6
   * @param expression
   */
  final class ListSessions(
    val expression: pack.Document) extends PipelineOperator {
    def makePipe: pack.Document = pipe(f"$$listSessions", expression)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  object ListSessions {
    def apply(expression: pack.Document): ListSessions =
      new ListSessions(expression)
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
  final class GraphLookup private (
    val from: String,
    val startWith: pack.Value,
    val connectFromField: String,
    val connectToField: String,
    val as: String,
    val maxDepth: Option[Int],
    val depthField: Option[String],
    val restrictSearchWithMatch: Option[pack.Value]) extends PipelineOperator {
    import builder.{ document, elementProducer => element, string }

    protected[reactivemongo] val makePipe: pack.Document = pipe(f"$$graphLookup", document(options))

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

    private[api] lazy val tupled = Tuple8(from, startWith, connectFromField, connectToField, as, maxDepth, depthField, restrictSearchWithMatch)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    @inline override def hashCode: Int = tupled.hashCode

    override def toString = s"GraphLookup${tupled.toString}"
  }

  object GraphLookup {
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
  final class Lookup private (
    val from: String,
    val localField: String,
    val foreignField: String,
    val as: String) extends PipelineOperator {

    import builder.{ document, elementProducer => element, string }
    protected[reactivemongo] val makePipe: pack.Document = document(Seq(
      element(f"$$lookup", document(Seq(
        element("from", string(from)),
        element("localField", string(localField)),
        element("foreignField", string(foreignField)),
        element("as", string(as)))))))

    private[api] lazy val tupled =
      Tuple4(from, localField, foreignField, as)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.tupled == other.tupled

      case _ =>
        false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"Lookup${tupled.toString}"
  }

  object Lookup {
    def apply(
      from: String,
      localField: String,
      foreignField: String,
      as: String): Lookup =
      new Lookup(from, localField, foreignField, as)
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/match/#_S_match Filters]] out documents from the stream that do not match the predicate.
   *
   * @param predicate the query that documents must satisfy to be in the stream
   */
  final class Match private (val predicate: pack.Document)
    extends PipelineOperator {

    protected[reactivemongo] val makePipe: pack.Document =
      pipe(f"$$match", predicate)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  object Match {
    def apply(predicate: pack.Document): Match = new Match(predicate)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/merge/ \$merge]] aggregation stage.
   *
   * @since MongoDB 4.2
   * @param intoDb the name of the `into` database
   * @param intoCollection the name of the `into` collection
   */
  final class Merge private (
    val intoDb: String,
    val intoCollection: String,
    val on: Seq[String],
    val whenMatched: Option[String],
    val let: Option[pack.Document],
    val whenNotMatched: Option[String]) extends PipelineOperator {

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

    private[api] lazy val tupled =
      Tuple6(intoDb, intoCollection, on, whenMatched, let, whenNotMatched)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.tupled == other.tupled

      case _ => false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"Merge${tupled.toString}"
  }

  object Merge {
    def apply(
      intoDb: String,
      intoCollection: String,
      on: Seq[String],
      whenMatched: Option[String],
      let: Option[pack.Document],
      whenNotMatched: Option[String]): Merge =
      new Merge(intoDb, intoCollection, on, whenMatched, let, whenNotMatched)
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/operator/aggregation/out/#pipe._S_out Takes the documents]] returned by the aggregation pipeline and writes them to a specified collection
   *
   * @param collection the name of the output collection
   */
  final class Out private (val collection: String)
    extends PipelineOperator {

    def makePipe: pack.Document = pipe(f"$$out", builder.string(collection))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  object Out {
    def apply(collection: String): Out = new Out(collection)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/planCacheStats/ \$planCacheStats]] aggregation stage.
   *
   * @since MongoDB 4.2
   */
  case object PlanCacheStats extends PipelineOperator {
    protected[reactivemongo] val makePipe = pipe(f"$$planCacheStats", builder.document(Seq.empty))
  }

  /**
   * Reshapes a document stream by renaming, adding, or removing fields.
   * Also uses [[http://docs.mongodb.org/manual/reference/aggregation/project/#_S_project Project]] to create computed values or sub-objects.
   *
   * @param specifications The fields to include. The resulting objects will contain only these fields.
   */
  final class Project private (val specifications: pack.Document)
    extends PipelineOperator {

    protected[reactivemongo] val makePipe: pack.Document = pipe(f"$$project", specifications)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  object Project {
    def apply(specifications: pack.Document): Project =
      new Project(specifications)
  }

  /**
   * Restricts the contents of the documents based on information stored in the documents themselves.
   * http://docs.mongodb.org/manual/reference/operator/aggregation/redact/#pipe._S_redact Redact
   * @param expression the redact expression
   */
  final class Redact private (val expression: pack.Document)
    extends PipelineOperator {

    protected[reactivemongo] val makePipe: pack.Document = pipe(f"$$redact", expression)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  object Redact {
    def apply(expression: pack.Document): Redact =
      new Redact(expression)
  }

  /**
   * Promotes a specified document to the top level and replaces all other fields.
   * The operation replaces all existing fields in the input document, including the _id field.
   * https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot
   * @param newRoot The field name to become the new root
   */
  final class ReplaceRootField private (val newRoot: String)
    extends PipelineOperator {

    protected[reactivemongo] val makePipe: pack.Document =
      pipe(f"$$replaceRoot", pipe("newRoot", builder.string("$" + newRoot)))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  object ReplaceRootField {
    def apply(newRoot: String): ReplaceRootField =
      new ReplaceRootField(newRoot)
  }

  /**
   * Promotes a specified document to the top level and replaces all other fields.
   * The operation replaces all existing fields in the input document, including the _id field.
   * https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot
   * @param newRoot The new root object
   */
  final class ReplaceRoot private (val newRoot: pack.Document)
    extends PipelineOperator {

    protected[reactivemongo] val makePipe: pack.Document =
      pipe(f"$$replaceRoot", pipe("newRoot", newRoot))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  object ReplaceRoot {
    def apply(newRoot: pack.Document): ReplaceRoot =
      new ReplaceRoot(newRoot)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/replaceWith/ \$replaceWith]] aggregation stage.
   *
   * @since MongoDB 4.2
   * @param replacementDocument
   */
  final class ReplaceWith private (val replacementDocument: pack.Document)
    extends PipelineOperator {

    def makePipe: pack.Document = pipe(f"$$replaceWith", replacementDocument)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  object ReplaceWith {
    def apply(replacementDocument: pack.Document): ReplaceWith =
      new ReplaceWith(replacementDocument)
  }

  /**
   * [[https://docs.mongodb.org/master/reference/operator/aggregation/sample/ \$sample]] aggregation stage, that randomly selects the specified number of documents from its input.
   *
   * @param size the number of documents to return
   */
  final class Sample private (val size: Int) extends PipelineOperator {
    protected[reactivemongo] val makePipe: pack.Document =
      pipe(f"$$sample", pipe("size", builder.int(size)))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.size == other.size

      case _ =>
        false
    }

    override def hashCode: Int = this.size

    override def toString: String = s"Sample(${size})"
  }

  object Sample {
    def apply(size: Int): Sample = new Sample(size)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/set/ \$set]] aggregation stage
   */
  final class Set private (val expression: pack.Document)
    extends PipelineOperator {

    def makePipe: pack.Document = pipe(f"$$set", expression)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  object Set {
    def apply(expression: pack.Document): Set = new Set(expression)
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/skip/#_S_skip \$skip]]s over a number of documents before passing all further documents along the stream.
   *
   * @param skip the number of documents to skip
   */
  final class Skip private (val skip: Int) extends PipelineOperator {
    protected[reactivemongo] val makePipe: pack.Document = builder.document(Seq(
      builder.elementProducer(f"$$skip", builder.int(skip))))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.skip == other.skip

      case _ =>
        false
    }

    override def hashCode: Int = this.skip

    override def toString: String = s"Skip(${skip})"
  }

  object Skip {
    def apply(skip: Int): Skip = new Skip(skip)
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/sort/#_S_sort \$sort]]s the stream based on the given fields.
   *
   * @param fields the fields to sort by
   */
  final class Sort private (val fields: Seq[SortOrder])
    extends PipelineOperator {

    import builder.{ document, elementProducer => element }
    protected[reactivemongo] val makePipe: pack.Document = document(Seq(
      element(f"$$sort", document(fields.map {
        case Ascending(field)  => element(field, builder.int(1))

        case Descending(field) => element(field, builder.int(-1))

        case MetadataSort(field, keyword) => {
          val meta = document(Seq(
            element(f"$$meta", builder.string(keyword.name))))

          element(field, meta)
        }
      }))))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/sortByCount/ \$sortByCount]] aggregation stage.
   *
   * @since MongoDB 3.4
   * @param expression
   */
  final class SortByCount private (val expression: pack.Value)
    extends PipelineOperator {

    def makePipe: pack.Document = pipe(f"$$sortByCount", expression)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  object SortByCount {
    def apply(expression: pack.Value): SortByCount =
      new SortByCount(expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/sortByCount/ \$sortByCount]] aggregation stage.
   *
   * @since MongoDB 3.4
   * @param field the field name
   */
  final class SortByFieldCount private (val field: String)
    extends PipelineOperator {

    def makePipe: pack.Document =
      pipe(f"$$sortByCount", builder.string("$" + field))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  object SortByFieldCount {
    def apply(field: String): SortByFieldCount =
      new SortByFieldCount(field)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/unset/ \$unset]] aggregation stage.
   *
   * @since MongoDB 4.2
   * @param field the field name
   * @param otherFields
   */
  final class Unset private (
    val field: String,
    val otherFields: Seq[String]) extends PipelineOperator {

    def makePipe: pack.Document = pipe(f"$$unset", builder.array(
      builder.string(field), otherFields.map(builder.string)))

    private[api] lazy val tupled = field -> otherFields

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        this.tupled == other.tupled

      case _ => false
    }

    @inline override def hashCode: Int = tupled.hashCode

    override def toString = s"Unset${tupled.toString}"
  }

  object Unset {
    def apply(field: String, otherFields: Seq[String]): Unset =
      new Unset(field, otherFields)
  }

  sealed trait Unwind extends PipelineOperator

  /**
   * Turns a document with an array into multiple documents,
   * one document for each element in the array.
   * http://docs.mongodb.org/manual/reference/aggregation/unwind/#_S_unwind
   * @param field the name of the array to unwind
   */
  final class UnwindField private (val field: String) extends Unwind {
    protected[reactivemongo] val makePipe = pipe(f"$$unwind", builder.string("$" + field))

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
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

  object UnwindField {
    def apply(field: String): UnwindField = new UnwindField(field)
  }

  object Unwind {
    /**
     * Turns a document with an array into multiple documents,
     * one document for each element in the array.
     * http://docs.mongodb.org/manual/reference/aggregation/unwind/#_S_unwind
     * @param field the name of the array to unwind
     */
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
      preserveNullAndEmptyArrays: Option[Boolean]): Unwind =
      Full(path, includeArrayIndex, preserveNullAndEmptyArrays)

    /**
     * @param path the field path to an array field (without the `\$` prefix)
     * @param includeArrayIndex the name of a new field to hold the array index of the element
     */
    private case class Full(
      path: String,
      includeArrayIndex: Option[String],
      preserveNullAndEmptyArrays: Option[Boolean]) extends Unwind {

      protected[reactivemongo] val makePipe =
        pipe(f"$$unwind", builder.document {
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
        })
    }
  }

  /**
   * The [[https://docs.mongodb.com/master/reference/operator/aggregation/filter/ \$filter]] aggregation stage.
   *
   * @param input the expression that resolves to an array
   * @param as The variable name for the element in the input array. The as expression accesses each element in the input array by this variable.
   * @param cond the expression that determines whether to include the element in the resulting array
   */
  final class Filter private[api] (
    val input: pack.Value,
    val as: String,
    val cond: pack.Document) {

    private lazy val tupled = Tuple3(input, as, cond)

    override def hashCode: Int = tupled.hashCode

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        other.tupled == this.tupled

      case _ => false
    }

    override def toString = s"Filter${tupled.toString}"
  }

  /** Filter companion */
  object Filter {
    implicit val writer: pack.Writer[Filter] = pack.writer[Filter] { f =>
      import builder.{ document, elementProducer => element }

      pipe(f"$$filter", document(Seq(
        element("input", f.input),
        element("as", builder.string(f.as)),
        element("cond", f.cond))))
    }
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
  final class ChangeStream( // TODO: Factory
    resumeAfter: Option[pack.Value] = None,
    startAtOperationTime: Option[pack.Value] = None, // TODO#1.1: restrict to something more like a timestamp?
    fullDocumentStrategy: Option[ChangeStreams.FullDocumentStrategy] = None) extends PipelineOperator {

    def makePipe: pack.Document = pipe(f"$$changeStream", builder.document(Seq(
      resumeAfter.map(v => builder.elementProducer("resumeAfter", v)),
      startAtOperationTime.map(v => builder.elementProducer("startAtOperationTime", v)),
      fullDocumentStrategy.map(v => builder.elementProducer("fullDocument", builder.string(v.name)))).flatten))

    private lazy val tupled =
      Tuple3(resumeAfter, startAtOperationTime, fullDocumentStrategy)

    override def equals(that: Any): Boolean = that match {
      case other: this.type =>
        other.tupled == this.tupled

      case _ => false
    }

    override def hashCode: Int = tupled.hashCode
  }
}
