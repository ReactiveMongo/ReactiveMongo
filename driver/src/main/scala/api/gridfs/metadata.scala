package reactivemongo.api.gridfs

import reactivemongo.api.SerializationPack

/** Metadata that cannot be customized. */
trait ComputedMetadata {
  /** Length of the file. */
  def length: Long

  /** Size of the chunks of this file. */
  def chunkSize: Int

  /** MD5 hash of this file. */
  def md5: Option[String]
}

/**
 * Common metadata.
 * @tparam Id Type of the id of this file (generally BSON ObjectID).
 */
trait BasicMetadata[+Id] {
  /** Id of this file. */
  def id: Id

  /** Name of this file. */
  def filename: Option[String]

  /** Date when this file was uploaded. */
  def uploadDate: Option[Long]

  /** Content type of this file. */
  def contentType: Option[String]
}

/** Custom metadata (generic trait) */
trait CustomMetadata[P <: SerializationPack] {
  val pack: P

  /** A BSONDocument holding all the metadata that are not standard. */
  def metadata: pack.Document
}
