package reactivemongo.api.commands

import concurrent.Future
import reactivemongo.api.{ BSONSerializationPack, Cursor, SerializationPack }

trait FindAndModify[P <: SerializationPack] extends CollectionCommand with CommandWithPack[P] with CommandWithResult[FindAndModifyResult[P]] {
  def query: P#Document
  // def modify: Modify // TODO
  def upsert: Boolean
  def sort: Option[P#Document]
  def fields: Option[P#Document]
}

trait FindAndModifyResult[P <: SerializationPack] {
  def result[R](implicit reader: P#Reader[R]): R
}

import reactivemongo.bson._

case class BSONFindAndModify(
  query: BSONDocument,
  upsert: Boolean = false,
  sort: Option[BSONDocument] = None,
  fields: Option[BSONDocument] = None
) extends FindAndModify[BSONSerializationPack.type]

object BSONFindAndModify {
  // readers + writers
  /*implicit val writer: BSONDocumentWriter[ResolvedCollectionCommandWithPackAndResult[BSONSerializationPack.type, FindAndModifyResult[BSONSerializationPack.type], BSONFindAndModify]] =
    new BSONDocumentWriter[ResolvedCollectionCommandWithPackAndResult[BSONSerializationPack.type, FindAndModifyResult[BSONSerializationPack.type], BSONFindAndModify]] {
      def write(command: ResolvedCollectionCommandWithPackAndResult[BSONSerializationPack.type, FindAndModifyResult[BSONSerializationPack.type], BSONFindAndModify]): BSONDocument =
        BSONDocument(
          "findAndModify" -> command.collection,
          "query" -> command.command.query,
          "sort" -> command.command.sort,
          "fields" -> command.command.fields,
          "upsert" -> (if(command.command.upsert) Some(true) else None)
        )
    }*/
  implicit val writer: BSONDocumentWriter[ResolvedCollectionCommand[BSONFindAndModify]] =
    new BSONDocumentWriter[ResolvedCollectionCommand[BSONFindAndModify]] {
      def write(command: ResolvedCollectionCommand[BSONFindAndModify]): BSONDocument =
        BSONDocument(
          "findAndModify" -> command.collection,
          "query" -> command.command.query,
          "sort" -> command.command.sort,
          "fields" -> command.command.fields,
          "upsert" -> (if(command.command.upsert) Some(true) else None)
        )
    }
  implicit val resultReader: BSONDocumentReader[FindAndModifyResult[BSONSerializationPack.type]] =
    new BSONDocumentReader[FindAndModifyResult[BSONSerializationPack.type]] {
      def read(doc: BSONDocument): FindAndModifyResult[BSONSerializationPack.type] = ???
    }
}