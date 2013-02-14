package reactivemongo.bson.exceptions

case class DocumentKeyNotFound(name: String) extends Exception {
  override def getMessage = s"The key '$name' could be found in this document or array"
}