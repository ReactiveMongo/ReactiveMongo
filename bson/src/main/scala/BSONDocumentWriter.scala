package reactivemongo.bson

trait BSONDocumentWriter[T] extends BSONWriter[T, BSONDocument]

object BSONDocumentWriter {
  private class Default[T](
    _write: T => BSONDocument) extends BSONDocumentWriter[T] {

    def write(value: T): BSONDocument = _write(value)
  }

  def apply[T](write: T => BSONDocument): BSONDocumentWriter[T] =
    new Default[T](write)
}
