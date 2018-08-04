package reactivemongo.api.commands

@deprecated("Will be private/internal", "0.16.0")
case class CollStats(scale: Option[Int] = None)
  extends CollectionCommand with CommandWithResult[CollStatsResult]

/**
 * Various information about a collection.
 *
 * @param ns The fully qualified collection name.
 * @param count The number of documents in this collection.
 * @param size The size in bytes (or in bytes / scale, if any).
 * @param averageObjectSize The average object size in bytes (or in bytes / scale, if any).
 * @param storageSize Preallocated space for the collection.
 * @param numExtents Number of extents (contiguously allocated chunks of datafile space, only for mmapv1 storage engine).
 * @param nindexes Number of indexes.
 * @param lastExtentSize Size of the most recently created extent (only for mmapv1 storage engine).
 * @param paddingFactor Padding can speed up updates if documents grow (only for mmapv1 storage engine).
 * @param systemFlags System flags.
 * @param userFlags User flags.
 * @param indexSizes Size of specific indexes in bytes.
 * @param capped States if this collection is capped.
 * @param max The maximum number of documents of this collection, if capped.
 * @param maxSize The maximum size in bytes (or in bytes / scale, if any) of this collection, if capped.
 */
case class CollStatsResult( // TODO: Move to `api` package
  ns: String,
  count: Int,
  size: Double,
  averageObjectSize: Option[Double],
  storageSize: Double,
  numExtents: Option[Int],
  nindexes: Int,
  lastExtentSize: Option[Int],
  paddingFactor: Option[Double],
  systemFlags: Option[Int],
  userFlags: Option[Int],
  totalIndexSize: Int,
  sizePerIndex: List[(String, Int)],
  capped: Boolean,
  max: Option[Long],
  maxSize: Option[Double] = None) {
  @inline def indexSizes: Array[(String, Int)] = sizePerIndex.toArray

  override def toString = s"""CollStatsResult($ns, capped = $capped, count = $count, size = $size, avgObjSize = $averageObjectSize, storageSize = $storageSize, numExtents = $numExtents, nindexes = $nindexes, lastExtentSize = $lastExtentSize, paddingFactor = $paddingFactor, systemFlags = $systemFlags, userFlags = $userFlags, sizePerIndex = ${sizePerIndex.mkString("[ ", ", ", " ]")}, max = $max)"""
}
