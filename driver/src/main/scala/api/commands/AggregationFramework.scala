package reactivemongo.api.commands

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
  with AggregationPipeline[P] {

  protected final lazy val builder = pack.newBuilder

  @inline protected final def pipe(name: String, arg: pack.Value): pack.Document = builder.document(Seq(builder.elementProducer(name, arg)))

  /**
   * @param batchSize the initial batch size for the cursor
   */
  @deprecated("Use `api.collections.Aggregator`", "0.12.7")
  case class Cursor(batchSize: Int)

  /**
   * @param pipeline the sequence of MongoDB aggregation operations
   * @param explain specifies to return the information on the processing of the pipeline
   * @param allowDiskUse enables writing to temporary files
   * @param cursor the cursor object for aggregation
   * @param bypassDocumentValidation available only if you specify the \$out aggregation operator
   * @param readConcern the read concern (since MongoDB 3.2)
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
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/addFields/ \$addFields]] stage (since MongoDB 3.4)
   *
   * @param specifications The fields to include.
   * The resulting objects will also contain these fields.
   */
  case class AddFields(specifications: pack.Document) extends PipelineOperator {
    val makePipe = pipe(f"$$addFields", specifications)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/bucket/ \$bucket]] aggregation stage.
   */
  case class Bucket(
    groupBy: pack.Value,
    boundaries: Seq[pack.Value],
    default: String)(
    output: (String, GroupFunction)*) extends PipelineOperator {

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
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/bucketAuto/ \$bucket]] aggregation stage (since MongoDB 3.4).
   *
   * Categorizes incoming documents into a specific number of groups,
   * called buckets, based on a specified expression.
   *
   * Bucket boundaries are automatically determined in an attempt
   * to evenly distribute the documents into the specified number of buckets.
   *
   * Document fields identifier must be prefixed with `$`.
   */
  case class BucketAuto(
    groupBy: pack.Value,
    buckets: Int,
    granularity: Option[String])(
    output: (String, GroupFunction)*)
    extends PipelineOperator {

    import builder.{ document, elementProducer => element }

    // TODO: Refactor with builder
    val makePipe: pack.Document = pipe(f"$$bucketAuto", document(Seq(
      Some(element("groupBy", groupBy)),
      Some(element("buckets", builder.int(buckets))),
      granularity.map { g => element("granularity", builder.string(g)) },
      Some(element("output", document(output.map({
        case (field, op) => element(field, op.makeFunction)
      }))))).flatten))
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/collStats/ \$collStats]] aggregation stage.
   */
  case class CollStats(
    latencyStatsHistograms: Boolean,
    storageStatsScale: Option[Double],
    count: Boolean) {

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
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/count/ \$count]] of the number of documents input (since MongoDB 3.4).
   *
   * @param outputName the name of the output field which has the count as its value
   */
  case class Count(outputName: String) extends PipelineOperator {
    val makePipe: pack.Document = pipe(f"$$count", builder.string(outputName))
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/currentOp/ \$currentOp]] (since MongoDB 3.6).
   *
   * @param allUsers (Defaults to `false`)
   * @param idleConnections if set to true, all operations including idle connections will be returned (Defaults to `false`)
   * @param idleCursors (Defaults to `false`; new in 4.2)
   * @param idleSessions (Defaults to `true`; new in 4.0)
   * @param localOps (Defaults to false; new in 4.0)
   */
  case class CurrentOp(
    allUsers: Boolean = false,
    idleConnections: Boolean = false,
    idleCursors: Boolean = false,
    idleSessions: Boolean = true,
    localOps: Boolean = false) extends PipelineOperator {
    import builder.{ boolean, elementProducer => element }
    val makePipe: pack.Document = pipe(f"$$currentOp", builder.document(Seq(
      element("allUsers", boolean(allUsers)),
      element("idleConnections", boolean(idleConnections)),
      element("idleCursors", boolean(idleCursors)),
      element("idleSessions", boolean(idleSessions)),
      element("localOps", boolean(localOps)))))
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
  case class Facet(
    specifications: Iterable[(String, Pipeline)]) extends PipelineOperator {

    val makePipe: pack.Document = {
      import builder.{ document, elementProducer => elem }

      val specDoc = document(specifications.map {
        case (name, (firstOp, subOps)) => elem(name, builder.array(
          firstOp.makePipe, subOps.map(_.makePipe)))

      }.toSeq)

      pipe(f"$$facet", specDoc)
    }
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

    @deprecated("No longer a case class", "0.18.5")
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

    @deprecated("No longer a case class", "0.18.5")
    val productArity: Int = 10

    @deprecated("No longer a case class", "0.18.5")
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

    @deprecated("No longer a case class", "0.18.5")
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
  case class Group(identifiers: pack.Value)(ops: (String, GroupFunction)*)
    extends PipelineOperator {
    import builder.{ document, elementProducer => element }

    val makePipe: pack.Document = pipe(f"$$group", document(
      element("_id", identifiers) +: ops.map({
        case (field, op) => element(field, op.makeFunction)
      })))
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/group/#_S_group \$group]]s documents together to calculate aggregates on document collections.
   * This command aggregates on one field.
   *
   * @param idField the name of the field to aggregate on
   * @param ops the sequence of operators specifying aggregate calculation
   */
  case class GroupField(idField: String)(ops: (String, GroupFunction)*)
    extends PipelineOperator {

    val makePipe = Group(builder.string("$" + idField))(ops: _*).makePipe
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/group/#_S_group \$group]]s documents together to calculate aggregates on document collections.
   * This command aggregates on multiple fields, and they must be named.
   *
   * @param idFields The fields to aggregate on, and the names they should be aggregated under.
   * @param ops the sequence of operators specifying aggregate calculation
   */
  case class GroupMulti(idFields: (String, String)*)(
    ops: (String, GroupFunction)*) extends PipelineOperator {
    val makePipe = Group(builder.document(idFields.map {
      case (alias, attribute) =>
        builder.elementProducer(alias, builder.string("$" + attribute))
    }))(ops: _*).makePipe
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/indexStats/ \$indexStats]] aggregation stage (Since MongoDB 3.2)
   */
  case object IndexStats extends PipelineOperator {
    val makePipe: pack.Document = pipe(f"$$indexStats", builder.document(Nil))
  }

  /**
   * @param ops the number of operations that used the index
   * @param since the time from which MongoDB gathered the statistics
   */
  case class IndexStatAccesses(ops: Long, since: Long)

  /**
   * @param name the index name
   * @param key the key specification
   * @param host the hostname and port of the mongod
   * @param accesses the index statistics
   */
  case class IndexStatsResult(
    name: String,
    key: pack.Document,
    host: String,
    accesses: IndexStatAccesses)

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/limit/#_S_limit \$limit]]s the number of documents that pass through the stream.
   *
   * @param limit the number of documents to allow through
   */
  case class Limit(limit: Int) extends PipelineOperator {
    val makePipe: pack.Document = pipe(f"$$limit", builder.int(limit))
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/listLocalSessions/ \$listLocalSessions]] aggregation stage (since MongoDB 3.6)
   *
   * @param expression
   */
  case class ListLocalSessions(
    expression: pack.Document) extends PipelineOperator {
    def makePipe: pack.Document = pipe(f"$$listLocalSessions", expression)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/listSessions/ \$listSessions]] aggregation stage (since MongoDB 3.6)
   *
   * @param expression
   */
  case class ListSessions(
    expression: pack.Document) extends PipelineOperator {
    def makePipe: pack.Document = pipe(f"$$listSessions", expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/graphLookup/ \$graphLookup]] aggregation stage (since MongoDB 3.4)
   *
   * @param from the target collection for the \$graphLookup operation to search
   * @param startWith the expression that specifies the value of the `connectFromField` with which to start the recursive search
   * @param connectFromField the field name whose value `\$graphLookup` uses to recursively match against the `connectToField` of other documents in the collection
   * @param connectToField the field name in other documents against which to match the value of the field specified by the `connectFromField` parameter
   * @param as the name of the array field added to each output document
   * @param maxDepth an optional non-negative integral number specifying the maximum recursion depth
   * @param depthField an optional name for a field to add to each traversed document in the search path
   * @param restrictSearchWithMatch an optional filter expression
   */
  case class GraphLookup(
    from: String,
    startWith: pack.Value,
    connectFromField: String,
    connectToField: String,
    as: String,
    maxDepth: Option[Int] = None,
    depthField: Option[String] = None,
    restrictSearchWithMatch: Option[pack.Value] = None) extends PipelineOperator {
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
  }

  /**
   * _Since MongoDB 3.2:_ Performs a left outer join to an unsharded collection in the same database to filter in documents from the "joined" collection for processing.
   * https://docs.mongodb.com/v3.2/reference/operator/aggregation/lookup/#pipe._S_lookup
   *
   * @param from the collection to perform the join with
   * @param localField the field from the documents input
   * @param foreignField the field from the documents in the `from` collection
   * @param as the name of the new array field to add to the input documents
   */
  case class Lookup(
    from: String,
    localField: String,
    foreignField: String,
    as: String) extends PipelineOperator {
    import builder.{ document, elementProducer => element, string }
    val makePipe: pack.Document = document(Seq(
      element(f"$$lookup", document(Seq(
        element("from", string(from)),
        element("localField", string(localField)),
        element("foreignField", string(foreignField)),
        element("as", string(as)))))))
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/match/#_S_match Filters]] out documents from the stream that do not match the predicate.
   *
   * @param predicate the query that documents must satisfy to be in the stream
   */
  case class Match(predicate: pack.Document) extends PipelineOperator {
    val makePipe: pack.Document = pipe(f"$$match", predicate)
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/merge/ \$merge]] aggregation stage (since MongoDB 4.2)
   *
   * @param intoDb the name of the `into` database
   * @param intoCollection the name of the `into` collection
   */
  case class Merge(
    intoDb: String,
    intoCollection: String,
    on: Seq[String],
    whenMatched: Option[String],
    let: Option[pack.Document],
    whenNotMatched: Option[String]) extends PipelineOperator {

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
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/operator/aggregation/out/#pipe._S_out Takes the documents]] returned by the aggregation pipeline and writes them to a specified collection
   *
   * @param collection the name of the output collection
   */
  case class Out(collection: String) extends PipelineOperator {
    def makePipe: pack.Document = pipe(f"$$out", builder.string(collection))
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/planCacheStats/ \$planCacheStats]] aggregation stage (since MongoDB 4.2)
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
  case class Project(specifications: pack.Document) extends PipelineOperator {
    val makePipe: pack.Document = pipe(f"$$project", specifications)
  }

  /**
   * Restricts the contents of the documents based on information stored in the documents themselves.
   * http://docs.mongodb.org/manual/reference/operator/aggregation/redact/#pipe._S_redact Redact
   * @param expression the redact expression
   */
  case class Redact(expression: pack.Document) extends PipelineOperator {
    val makePipe: pack.Document = pipe(f"$$redact", expression)
  }

  /**
   * Promotes a specified document to the top level and replaces all other fields.
   * The operation replaces all existing fields in the input document, including the _id field.
   * https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot
   * @param newRoot The field name to become the new root
   */
  case class ReplaceRootField(newRoot: String) extends PipelineOperator {
    val makePipe: pack.Document =
      pipe(f"$$replaceRoot", pipe("newRoot", builder.string("$" + newRoot)))
  }

  /**
   * Promotes a specified document to the top level and replaces all other fields.
   * The operation replaces all existing fields in the input document, including the _id field.
   * https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot
   * @param newRoot The new root object
   */
  case class ReplaceRoot(newRoot: pack.Document) extends PipelineOperator {
    val makePipe: pack.Document =
      pipe(f"$$replaceRoot", pipe("newRoot", newRoot))
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/replaceWith/ \$replaceWith]] aggregation stage (Since MongoDB 4.2)
   *
   * @param replacementDocument
   */
  case class ReplaceWith(
    replacementDocument: pack.Document) extends PipelineOperator {

    def makePipe: pack.Document = pipe(f"$$replaceWith", replacementDocument)
  }

  /**
   * [[https://docs.mongodb.org/master/reference/operator/aggregation/sample/ \$sample]] aggregation stage, that randomly selects the specified number of documents from its input.
   *
   * @param size the number of documents to return
   */
  case class Sample(size: Int) extends PipelineOperator {
    val makePipe: pack.Document =
      pipe(f"$$sample", pipe("size", builder.int(size)))
  }

  /**
   * [[https://docs.mongodb.com/manual/reference/operator/aggregation/set/ \$set]] aggregation stage
   */
  case class Set(expression: pack.Document) extends PipelineOperator {
    def makePipe: pack.Document = pipe(f"$$set", expression)
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/skip/#_S_skip \$skip]]s over a number of documents before passing all further documents along the stream.
   *
   * @param skip the number of documents to skip
   */
  case class Skip(skip: Int) extends PipelineOperator {
    val makePipe: pack.Document = builder.document(Seq(
      builder.elementProducer(f"$$skip", builder.int(skip))))
  }

  /**
   * [[http://docs.mongodb.org/manual/reference/aggregation/sort/#_S_sort \$sort]]s the stream based on the given fields.
   *
   * @param fields the fields to sort by
   */
  case class Sort(fields: SortOrder*) extends PipelineOperator {
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
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/sortByCount/ \$sortByCount]] aggregation stage (Since MongoDB 3.4)
   *
   * @param expression
   */
  case class SortByCount(expression: pack.Value) extends PipelineOperator {
    def makePipe: pack.Document = pipe(f"$$sortByCount", expression)
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/sortByCount/ \$sortByCount]] aggregation stage (Since MongoDB 3.4)
   *
   * @param field the field name
   */
  case class SortByFieldCount(field: String) extends PipelineOperator {
    def makePipe: pack.Document =
      pipe(f"$$sortByCount", builder.string("$" + field))
  }

  /**
   * The [[https://docs.mongodb.com/manual/reference/operator/aggregation/unset/ \$unset]] aggregation stage (Since MongoDB 4.2)
   *
   * @param field the field name
   * @param otherFields
   */
  case class Unset(
    field: String,
    otherFields: Seq[String]) extends PipelineOperator {
    def makePipe: pack.Document = pipe(f"$$unset", builder.array(
      builder.string(field), otherFields.map(builder.string)))
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
  case class UnwindField(field: String) extends Unwind(1, { case 0 => field },
    pipe(f"$$unwind", builder.string("$" + field)))

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
     * (Since MongoDB 3.2)
     * Turns a document with an array into multiple documents,
     * one document for each element in the array.
     *
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
   * Low level pipeline operator which allows to open a tailable cursor against subsequent ChangeEvents of a given
   * collection (since MongoDB 3.6).
   * https://docs.mongodb.com/manual/reference/change-events/
   *
   * For common use-cases, you might prefer to use the `watch` operator on a collection.
   *
   * Note: the target mongo instance MUST be a replica-set (even in the case of a single node deployement).
   *
   * @param resumeAfter the id of the last known Change Event, if any. The stream will resume just after that event.
   * @param startAtOperationTime the operation time before which all Change Events are known. Must be in the time range
   *                             of the oplog. (since MongoDB 4.0)
   * @param fullDocumentStrategy if set to UpdateLookup, every update change event will be joined with the *current* version of the
   *                     relevant document.
   */
  final class ChangeStream(
    resumeAfter: Option[pack.Value] = None,
    startAtOperationTime: Option[pack.Value] = None, // TODO restrict to something more like a timestamp?
    fullDocumentStrategy: Option[ChangeStreams.FullDocumentStrategy] = None) extends PipelineOperator {

    def makePipe: pack.Document = pipe(f"$$changeStream", builder.document(Seq(
      resumeAfter.map(v => builder.elementProducer("resumeAfter", v)),
      startAtOperationTime.map(v => builder.elementProducer("startAtOperationTime", v)),
      fullDocumentStrategy.map(v => builder.elementProducer("fullDocument", builder.string(v.name)))).flatten))
  }

}
