package reactivemongo.api

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
final class CollectionStats private[api] (
    val ns: String,
    val count: Int,
    val size: Double,
    val averageObjectSize: Option[Double],
    val storageSize: Double,
    val numExtents: Option[Int],
    val nindexes: Int,
    val lastExtentSize: Option[Int],
    val paddingFactor: Option[Double],
    val systemFlags: Option[Int],
    val userFlags: Option[Int],
    val totalIndexSize: Int,
    val sizePerIndex: List[(String, Int)],
    val capped: Boolean,
    val max: Option[Long],
    val maxSize: Option[Double]) {
  @inline def indexSizes: Array[(String, Int)] = sizePerIndex.toArray

  private lazy val tupled = Tuple16(
    ns,
    count,
    size,
    averageObjectSize,
    storageSize,
    numExtents,
    nindexes,
    lastExtentSize,
    paddingFactor,
    systemFlags,
    userFlags,
    totalIndexSize,
    sizePerIndex,
    capped,
    max,
    maxSize
  )

  override def hashCode: Int = tupled.hashCode

  override def equals(that: Any): Boolean = that match {
    case other: CollectionStats =>
      other.tupled == this.tupled

    case _ =>
      false
  }

  override def toString =
    s"""CollStatsResult($ns, capped = $capped, count = $count, size = $size, avgObjSize = $averageObjectSize, storageSize = $storageSize, numExtents = $numExtents, nindexes = $nindexes, lastExtentSize = $lastExtentSize, paddingFactor = $paddingFactor, systemFlags = $systemFlags, userFlags = $userFlags, sizePerIndex = ${sizePerIndex
        .mkString("[ ", ", ", " ]")}, max = $max)"""
}
