package reactivemongo.api.gridfs

/** File metadata */
private[api] trait FileMetadata[+Id, Document] {
  /** The id of this file. */
  def id: Id

  /** The name of this file. */
  def filename: Option[String]

  /** The date when this file was uploaded. */
  def uploadDate: Option[Long]

  /** The content type of this file. */
  def contentType: Option[String]

  /** The Document holding all the metadata that are not standard. */
  def metadata: Document
}

/** Computed file metadata */
private[api] trait ComputedMetadata { _: FileMetadata[_, _] =>
  /** The length of the file. */
  def length: Long

  /** The size of the chunks of this file. */
  def chunkSize: Int

  /** The MD5 hash of this file. */
  def md5: Option[String]
}
