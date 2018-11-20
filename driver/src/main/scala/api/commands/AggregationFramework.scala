package reactivemongo.api.commands

import reactivemongo.api.{ ChangeStreams, ReadConcern, SerializationPack }
import reactivemongo.core.protocol.MongoWireVersion

/**
 * Implements the [[http://docs.mongodb.org/manual/applications/aggregation/ Aggregation Framework]].
 */
trait AggregationFramework[P <: SerializationPack]
  extends ImplicitCommandHelpers[P]
  with GroupAggregation[P] with AggregationPipeline[P] {

  protected lazy val builder = pack.newBuilder

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
   * Reshapes a document stream by renaming, adding, or removing fields.
   * Also uses [[http://docs.mongodb.org/manual/reference/aggregation/project/#_S_project Project]] to create computed values or sub-objects.
   *
   * @param specifications The fields to include. The resulting objects will contain only these fields.
   */
  case class Project(specifications: pack.Document) extends PipelineOperator {
    val makePipe: pack.Document = builder.document(Seq(
      builder.elementProducer(f"$$project", specifications)))
  }

  /**
   * Filters out documents from the stream that do not match the predicate.
   * http://docs.mongodb.org/manual/reference/aggregation/match/#_S_match
   * @param predicate the query that documents must satisfy to be in the stream
   */
  case class Match(predicate: pack.Document) extends PipelineOperator {
    val makePipe: pack.Document = builder.document(Seq(
      builder.elementProducer(f"$$match", predicate)))
  }

  /**
   * Restricts the contents of the documents based on information stored in the documents themselves.
   * http://docs.mongodb.org/manual/reference/operator/aggregation/redact/#pipe._S_redact Redact
   * @param expression the redact expression
   */
  case class Redact(expression: pack.Document) extends PipelineOperator {
    val makePipe: pack.Document = builder.document(Seq(
      builder.elementProducer(f"$$redact", expression)))
  }

  /**
   * Limits the number of documents that pass through the stream.
   * http://docs.mongodb.org/manual/reference/aggregation/limit/#_S_limit
   * @param limit the number of documents to allow through
   */
  case class Limit(limit: Int) extends PipelineOperator {
    val makePipe: pack.Document = builder.document(Seq(
      builder.elementProducer(f"$$limit", builder.int(limit))))
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
   * The [[https://docs.mongodb.com/master/reference/operator/aggregation/filter/ \$filter]] aggregation stage.
   *
   * @param input the expression that resolves to an array
   * @param as The variable name for the element in the input array. The as expression accesses each element in the input array by this variable.
   * @param cond the expression that determines whether to include the element in the resulting array
   */
  case class Filter(input: pack.Value, as: String, cond: pack.Document)
    extends PipelineOperator {
    import builder.{ document, elementProducer => element }
    val makePipe: pack.Document = document(Seq(
      element(f"$$filter", document(Seq(
        element("input", input),
        element("as", builder.string(as)),
        element("cond", cond))))))
  }

  /** Filter companion */
  object Filter {
    implicit val writer: pack.Writer[Filter] =
      pack.writer[Filter] { _.makePipe }
  }

  /**
   * _Since MongoDB 3.4:_ The [[https://docs.mongodb.com/manual/reference/operator/aggregation/graphLookup/ \$graphLookup]] aggregation stage.
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

    val makePipe: pack.Document = document(Seq(
      element(f"$$graphLookup", document(options))))

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
   * Skips over a number of documents before passing all further documents along the stream.
   * http://docs.mongodb.org/manual/reference/aggregation/skip/#_S_skip
   * @param skip the number of documents to skip
   */
  case class Skip(skip: Int) extends PipelineOperator {
    val makePipe: pack.Document = builder.document(Seq(
      builder.elementProducer(f"$$skip", builder.int(skip))))
  }

  /**
   * Randomly selects the specified number of documents from its input.
   * https://docs.mongodb.org/master/reference/operator/aggregation/sample/
   * @param size the number of documents to return
   */
  case class Sample(size: Int) extends PipelineOperator {
    import builder.{ document, elementProducer => element }
    val makePipe: pack.Document = document(Seq(
      element(f"$$sample", document(Seq(
        element("size", builder.int(size)))))))
  }

  /**
   * Groups documents together to calculate aggregates on document collections.
   * This command aggregates on arbitrary identifiers.
   * Document fields identifier must be prefixed with `$`.
   * http://docs.mongodb.org/manual/reference/aggregation/group/#_S_group
   * @param identifiers any BSON value acceptable by mongodb as identifier
   * @param ops the sequence of operators specifying aggregate calculation
   */
  case class Group(identifiers: pack.Value)(ops: (String, GroupFunction)*)
    extends PipelineOperator {
    import builder.{ document, elementProducer => element }

    val makePipe: pack.Document = document(Seq(
      element(f"$$group", document(
        element("_id", identifiers) +: ops.map({
          case (field, op) => element(field, op.makeFunction)
        })))))
  }

  /**
   * Groups documents together to calculate aggregates on document collections.
   * This command aggregates on one field.
   * http://docs.mongodb.org/manual/reference/aggregation/group/#_S_group
   * @param idField the name of the field to aggregate on
   * @param ops the sequence of operators specifying aggregate calculation
   */
  case class GroupField(idField: String)(ops: (String, GroupFunction)*)
    extends PipelineOperator {

    val makePipe = Group(builder.string("$" + idField))(ops: _*).makePipe
  }

  /**
   * Groups documents together to calculate aggregates on document collections.
   * This command aggregates on multiple fields, and they must be named.
   * http://docs.mongodb.org/manual/reference/aggregation/group/#_S_group
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
   * [[https://docs.mongodb.org/v3.0/reference/operator/aggregation/meta/#exp._S_meta Keyword of metadata]].
   */
  sealed trait MetadataKeyword {
    /** Keyword name */
    def name: String
  }

  /**
   * Since MongoDB 3.2
   * https://docs.mongodb.com/manual/reference/operator/aggregation/indexStats/
   */
  case object IndexStats extends PipelineOperator {
    val makePipe: pack.Document = builder.document(Seq(
      builder.elementProducer(f"$$indexStats", builder.document(Nil))))
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
   * Since MongoDB 3.4
   * Categorizes incoming documents into a specific number of groups, called buckets,
   * based on a specified expression. Bucket boundaries are automatically determined
   * in an attempt to evenly distribute the documents into the specified number of buckets.
   * Document fields identifier must be prefixed with `$`.
   * https://docs.mongodb.com/manual/reference/operator/aggregation/bucketAuto/
   * @param identifiers any BSON value acceptable by mongodb as identifier
   * @param ops the sequence of operators specifying aggregate calculation
   */
  case class BucketAuto(groupBy: pack.Value, buckets: Int, granularity: Option[String])(output: (String, GroupFunction)*)
    extends PipelineOperator {
    import builder.{ document, elementProducer => element }
    val makePipe: pack.Document = document(Seq(
      element(f"$$bucketAuto", document(Seq(
        Some(element("groupBy", groupBy)),
        Some(element("buckets", builder.int(buckets))),
        granularity.map { g => element("granularity", builder.string(g)) },
        Some(element("output", document(output.map({
          case (field, op) => element(field, op.makeFunction)
        }))))).flatten))))
  }

  /** References the score associated with the corresponding [[https://docs.mongodb.org/v3.0/reference/operator/query/text/#op._S_text `\$text`]] query for each matching document. */
  case object TextScore extends MetadataKeyword {
    val name = "textScore"
  }

  /**
   * Represents that a field should be sorted on, as well as whether it
   * should be ascending or descending.
   */
  sealed trait SortOrder {
    /** The name of the field to be used to sort. */
    def field: String
  }

  /** Ascending sort order */
  case class Ascending(field: String) extends SortOrder

  /** Descending sort order */
  case class Descending(field: String) extends SortOrder

  /**
   * [[https://docs.mongodb.org/v3.0/reference/operator/aggregation/sort/#sort-pipeline-metadata Metadata sort]] order.
   *
   * @param keyword the metadata keyword to sort by
   */
  case class MetadataSort(
    field: String, keyword: MetadataKeyword) extends SortOrder

  /**
   * Sorts the stream based on the given fields.
   * http://docs.mongodb.org/manual/reference/aggregation/sort/#_S_sort
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
   * Promotes a specified document to the top level and replaces all other fields.
   * The operation replaces all existing fields in the input document, including the _id field.
   * https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot
   * @param newRoot The field name to become the new root
   */
  case class ReplaceRootField(newRoot: String) extends PipelineOperator {
    import builder.{ document, elementProducer => element }
    override val makePipe: pack.Document = document(Seq(
      element(f"$$replaceRoot", document(Seq(
        element("newRoot", builder.string("$" + newRoot)))))))
  }

  /**
   * Promotes a specified document to the top level and replaces all other fields.
   * The operation replaces all existing fields in the input document, including the _id field.
   * https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot
   * @param newRoot The new root object
   */
  case class ReplaceRoot(newRoot: pack.Document) extends PipelineOperator {
    import builder.{ document, elementProducer => element }
    override val makePipe: pack.Document = document(Seq(
      element(f"$$replaceRoot", document(Seq(
        element("newRoot", newRoot))))))
  }

  /**
   * Outputs documents in order of nearest to farthest from a specified point.
   *
   * http://docs.mongodb.org/manual/reference/operator/aggregation/geoNear/#pipe._S_geoNear
   * @param near the point for which to find the closest documents
   * @param spherical if using a 2dsphere index
   * @param limit the maximum number of documents to return
   * @param maxDistance the maximum distance from the center point that the documents can be
   * @param query limits the results to the matching documents
   * @param distanceMultiplier the factor to multiply all distances returned by the query
   * @param uniqueDocs if this value is true, the query returns a matching document once
   * @param distanceField the output field that contains the calculated distance
   * @param includeLocs this specifies the output field that identifies the location used to calculate the distance
   */
  case class GeoNear(near: pack.Value, spherical: Boolean = false, limit: Long = 100, minDistance: Option[Long] = None, maxDistance: Option[Long] = None, query: Option[pack.Document] = None, distanceMultiplier: Option[Double] = None, uniqueDocs: Boolean = false, distanceField: Option[String] = None, includeLocs: Option[String] = None) extends PipelineOperator {
    import builder.{ boolean, elementProducer => element, document, long, string }
    def makePipe: pack.Document = document(Seq(
      element(f"$$geoNear", document(Seq(
        element("near", near),
        element("spherical", boolean(spherical)),
        element("limit", long(limit))) ++ Seq(
          minDistance.map(l => element("minDistance", long(l))),
          maxDistance.map(l => element("maxDistance", long(l))),
          query.map(s => element("query", s)),
          distanceMultiplier.map(d => element(
            "distanceMultiplier", builder.double(d))),
          Some(element("uniqueDocs", boolean(uniqueDocs))),
          distanceField.map(s =>
            element("distanceField", string(s))),
          includeLocs.map(s =>
            element("includeLocs", string(s)))).flatten))))
  }

  /**
   * Takes the documents returned by the aggregation pipeline and writes them to a specified collection
   * http://docs.mongodb.org/manual/reference/operator/aggregation/out/#pipe._S_out
   * @param collection the name of the output collection
   */
  case class Out(collection: String) extends PipelineOperator {
    def makePipe: pack.Document = builder.document(Seq(
      builder.elementProducer(f"$$out", builder.string(collection))))
  }

  // Unwind

  class Unwind private[commands] (
    val productArity: Int,
    element: Int => Any,
    operator: => pack.Document) extends PipelineOperator with Product
    with Serializable with java.io.Serializable {

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
  case class UnwindField(field: String) extends Unwind(1, { case 1 => field },
    builder.document(Seq(
      builder.elementProducer(f"$$unwind", builder.string("$" + field)))))

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
    }, builder.document(Seq(builder.elementProducer(
      f"$$unwind",
      builder.document {
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
      }))))
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
    fullDocumentStrategy: Option[ChangeStreams.FullDocumentStrategy] = None
  ) extends PipelineOperator {

    def makePipe: pack.Document = builder.document(Seq(
      builder.elementProducer(f"$$changeStream", builder.document(Seq(
        resumeAfter.map(v => builder.elementProducer("resumeAfter", v)),
        startAtOperationTime.map(v => builder.elementProducer("startAtOperationTime", v)),
        fullDocumentStrategy.map(v => builder.elementProducer("fullDocument", builder.string(v.name))),
      ).flatten))
    ))
  }

}
