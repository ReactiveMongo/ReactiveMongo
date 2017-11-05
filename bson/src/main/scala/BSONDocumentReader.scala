package reactivemongo.bson

trait BSONDocumentReader[T] extends BSONReader[BSONDocument, T]

object BSONDocumentReader {
  private class Default[T](
    _read: BSONDocument => T) extends BSONDocumentReader[T] {

    def read(value: BSONDocument): T = _read(value)
  }

  def apply[T](read: BSONDocument => T): BSONDocumentReader[T] =
    new Default[T](read)
}
