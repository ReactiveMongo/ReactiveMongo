package reactivemongo.api.gridfs

import reactivemongo.bson.{ BSONDocument, BSONObjectID, BSONValue }

import reactivemongo.api.{ BSONSerializationPack, SerializationPack }

/**
 * A file that will be saved in a GridFS store.
 * @tparam Id Type of the id of this file (generally BSON ObjectID).
 */
trait FileToSave[P <: SerializationPack with Singleton, +Id]
  extends BasicMetadata[Id] with CustomMetadata[P] {
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

/** A BSON implementation of `FileToSave`. */
@deprecated("Use `reactivemongo.api.bson` types", "0.19.0")
class DefaultFileToSave private[gridfs] (
  val filename: Option[String] = None,
  val contentType: Option[String] = None,
  val uploadDate: Option[Long] = None,
  val metadata: BSONDocument = BSONDocument.empty,
  val id: BSONValue = BSONObjectID.generate()) extends FileToSave[BSONSerializationPack.type, BSONValue] with Equals {

  val pack = BSONSerializationPack

  @deprecated("No longer case class", "0.19.0")
  def canEqual(that: Any): Boolean = that match {
    case _: DefaultFileToSave => true
    case _                    => false
  }

  @deprecated("No longer case class", "0.19.0")
  def copy(filename: Option[String] = this.filename, contentType: Option[String] = this.contentType, uploadDate: Option[Long] = this.uploadDate, metadata: BSONDocument = this.metadata, id: BSONValue = this.id) = new DefaultFileToSave(filename, contentType, uploadDate, metadata, id)

}

/** Factory of [[DefaultFileToSave]]. */
object DefaultFileToSave {
  def unapply(that: DefaultFileToSave): Option[(Option[String], Option[String], Option[Long], BSONDocument, BSONValue)] = Some((that.filename, that.contentType, that.uploadDate, that.metadata, that.id))

  /** For backward compatibility. */
  sealed trait FileName[T] extends (T => Option[String]) {
    def apply(name: T): Option[String]
  }

  object FileName {
    implicit object OptionalFileName extends FileName[Option[String]] {
      def apply(name: Option[String]) = name
    }

    implicit object SomeFileName extends FileName[Some[String]] {
      def apply(name: Some[String]) = name
    }

    implicit object NoFileName extends FileName[None.type] {
      def apply(name: None.type) = Option.empty[String]
    }
  }

  def apply[N](
    filename: N,
    contentType: Option[String] = None,
    uploadDate: Option[Long] = None,
    metadata: BSONDocument = BSONDocument.empty,
    id: BSONValue = BSONObjectID.generate())(implicit naming: FileName[N]): DefaultFileToSave = new DefaultFileToSave(naming(filename), contentType, uploadDate, metadata, id)

}
