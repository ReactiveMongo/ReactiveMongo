package reactivemongo.api.gridfs

import reactivemongo.bson.{ BSONDocument, BSONValue }

import reactivemongo.api.{ BSONSerializationPack, SerializationPack }

/**
 * A file read from a GridFS store.
 * @tparam Id Type of the id of this file (BSON ObjectID).
 */
trait ReadFile[P <: SerializationPack, +Id]
  extends BasicMetadata[Id] with CustomMetadata[P] with ComputedMetadata {

  private[api] def tupled = Tuple8((id: Any), contentType, filename, uploadDate, chunkSize, length, md5, metadata)
}

object ReadFile {
  private[api] def apply[P <: SerializationPack, Id](_pack: P)(
    _id: Id,
    _contentType: Option[String],
    _filename: Option[String],
    _uploadDate: Option[Long],
    _chunkSize: Int,
    _length: Long,
    _md5: Option[String],
    _metadata: _pack.Document): ReadFile[P, Id] =
    new ReadFile[P, Id] {
      @transient val pack: _pack.type = _pack

      val id = _id
      val contentType = _contentType
      val filename = _filename
      val uploadDate = _uploadDate
      val chunkSize = _chunkSize
      val length = _length
      val md5 = _md5
      val metadata = _metadata

      override def equals(that: Any): Boolean = that match {
        case other: ReadFile[_, _] =>
          this.tupled == other.tupled

        case _ =>
          false
      }

      override def hashCode: Int = tupled.hashCode

      override def toString: String = s"ReadFile${tupled.toString}"
    }

  /** A default `BSONReader` for `ReadFile`. */
  private[api] def reader[P <: SerializationPack, Id](pack: P)(implicit idValue: Id <:< pack.Value, idTag: scala.reflect.ClassTag[Id]): pack.Reader[ReadFile[P, Id]] = {
    val decoder = pack.newDecoder
    val builder = pack.newBuilder
    val emptyMetadata = builder.document(Seq.empty[pack.ElementProducer])

    pack.reader[ReadFile[P, Id]] { doc =>
      (for {
        id <- decoder.value[Id](doc, "_id")
        cz <- decoder.int(doc, "chunkSize")
        lh <- decoder.long(doc, "length")

        ct = decoder.string(doc, "contentType")
        fn = decoder.string(doc, "filename")
        ud = decoder.long(doc, "uploadDate")
        m5 = decoder.string(doc, "md5")
        mt = decoder.child(doc, "metadata").getOrElse(emptyMetadata)
      } yield ReadFile[P, Id](
        pack)(id, ct, fn, ud, cz, lh, m5, mt)).get
    }
  }
}

/** A BSON implementation of `ReadFile`. */
@SerialVersionUID(930238403L)
@deprecated("Use `ReadFile.apply`", "0.19.0")
case class DefaultReadFile(
  id: BSONValue,
  contentType: Option[String],
  filename: Option[String],
  uploadDate: Option[Long],
  chunkSize: Int,
  length: Long,
  md5: Option[String],
  metadata: BSONDocument,
  original: BSONDocument) extends ReadFile[BSONSerializationPack.type, BSONValue] {
  @transient val pack = BSONSerializationPack
}
