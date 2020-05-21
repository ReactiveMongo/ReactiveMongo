package reactivemongo.api.gridfs

import reactivemongo.api.SerializationPack

/**
 * A file read from a GridFS store.
 *
 * @tparam Id the type of the id of this file (BSON ObjectID).
 * @tparam Metadata the metadata document type
 *
 * @param id the id of this file
 * @param filename the name of this file
 * @param uploadDate the date when this file was uploaded
 * @param contentType the content type of this file
 * @param length the length of the file
 * @param chunkSize the size of the chunks of this file
 * @param md5 the MD5 hash of this file
 * @param metadata the document holding all the metadata that are not standard
 */
final class ReadFile[+Id, Metadata] private[api] (
  val id: Id,
  val filename: Option[String],
  val uploadDate: Option[Long],
  val contentType: Option[String],
  val length: Long,
  val chunkSize: Int,
  val md5: Option[String],
  val metadata: Metadata) extends FileMetadata[Id, Metadata] with ComputedMetadata {

  private[api] def tupled = Tuple8((id: Any), contentType, filename, uploadDate, chunkSize, length, md5, metadata)

  @SuppressWarnings(Array("ComparingUnrelatedTypes"))
  override def equals(that: Any): Boolean = that match {
    case other: ReadFile[_, _] =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString: String = s"ReadFile${tupled.toString}"
}

object ReadFile {
  /** A default `BSONReader` for `ReadFile`. */
  private[api] def reader[P <: SerializationPack, Id](pack: P)(implicit idValue: Id <:< pack.Value, idTag: scala.reflect.ClassTag[Id]): pack.Reader[ReadFile[Id, pack.Document]] = {
    val decoder = pack.newDecoder
    val builder = pack.newBuilder
    val emptyMetadata = builder.document(Seq.empty[pack.ElementProducer])

    pack.readerOpt[ReadFile[Id, pack.Document]] { doc =>
      (for {
        id <- decoder.value[Id](doc, "_id")
        cz <- decoder.int(doc, "chunkSize")
        lh <- decoder.long(doc, "length")

        ct = decoder.string(doc, "contentType")
        fn = decoder.string(doc, "filename")
        ud = decoder.long(doc, "uploadDate")
        m5 = decoder.string(doc, "md5")
        mt = decoder.child(doc, "metadata").getOrElse(emptyMetadata)
      } yield new ReadFile[Id, pack.Document](
        id, fn, ud, ct, lh, cz, m5, mt))

    }
  }
}
