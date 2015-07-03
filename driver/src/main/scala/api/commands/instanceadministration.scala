package reactivemongo.api.commands

object DropDatabase extends Command with CommandWithResult[UnitBox.type]

object Drop extends CollectionCommand with CommandWithResult[UnitBox.type]

object EmptyCapped extends CollectionCommand with CommandWithResult[UnitBox.type]

case class RenameCollection(
  fullyQualifiedCollectionName: String,
  fullyQualifiedTargetName: String,
  dropTarget: Boolean = false) extends Command with CommandWithResult[UnitBox.type]

case class Create(
  capped: Option[Capped] = None, // if set, "capped" -> true, size -> <int>, max -> <int>
  autoIndexId: Boolean = true, // optional
  flags: Int = 1 // defaults to 1
  ) extends CollectionCommand with CommandWithResult[UnitBox.type]

case class Capped(
  size: Long,
  max: Option[Int] = None)

case class ConvertToCapped(
  capped: Capped) extends CollectionCommand with CommandWithResult[UnitBox.type]

case class CollStats(scale: Option[Int] = None) extends CollectionCommand with CommandWithResult[CollStatsResult]

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
 */
case class CollStatsResult(
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
  indexSizes: Array[(String, Int)],
  capped: Boolean,
  max: Option[Long])

case class DropIndexes(index: String) extends CollectionCommand with CommandWithResult[DropIndexesResult]

case class DropIndexesResult(value: Int) extends BoxedAnyVal[Int]

case class CollectionNames(names: List[String])

/** List the names of DB collections. */
object ListCollectionNames extends Command with CommandWithResult[CollectionNames]

import reactivemongo.api.indexes.Index

/**
 * Lists the indexes of the specified collection.
 *
 * @param db the database name
 */
case class ListIndexes(db: String) extends CollectionCommand
  with CommandWithResult[List[Index]]

/**
 * Creates the given indexes on the specified collection.
 *
 * @param db the database name
 * @param indexes the indexes to be created
 */
case class CreateIndexes(db: String, indexes: List[Index])
  extends CollectionCommand with CommandWithResult[WriteResult]
