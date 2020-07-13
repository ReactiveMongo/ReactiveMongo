package reactivemongo.api.gridfs

/**
 * A file that will be saved in a GridFS store.
 * @tparam Id Type of the id of this file (generally BSON ObjectID).
 */
final class FileToSave[Id, Document] private[api] (
  val id: Id,
  val filename: Option[String],
  val contentType: Option[String],
  val uploadDate: Option[Long],
  val metadata: Document) extends FileMetadata[Id, Document] {

  @SuppressWarnings(Array("ComparingUnrelatedTypes"))
  override def equals(that: Any): Boolean = that match {
    case other: FileToSave[_, _] =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString = s"FileToSave${tupled.toString}"

  private[api] lazy val tupled =
    Tuple5(filename, contentType, uploadDate, metadata, id)

}
