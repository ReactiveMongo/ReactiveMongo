package reactivemongo.api.commands.bson

import reactivemongo.api.commands._
import reactivemongo.bson._

object BSONDropDatabaseImplicits {
  implicit object DropDatabaseWriter extends BSONDocumentWriter[DropDatabase.type] {
    def write(dd: DropDatabase.type): BSONDocument =
      BSONDocument("dropDatabase" -> 1)
  }
}

object BSONDropImplicits {
  implicit object DropWriter extends BSONDocumentWriter[ResolvedCollectionCommand[Drop.type]] {
    def write(command: ResolvedCollectionCommand[Drop.type]): BSONDocument =
      BSONDocument("drop" -> command.collection)
  }
}

object BSONEmptyCappedImplicits {
  implicit object EmptyCappedWriter extends BSONDocumentWriter[ResolvedCollectionCommand[EmptyCapped.type]] {
    def write(command: ResolvedCollectionCommand[EmptyCapped.type]): BSONDocument =
      BSONDocument("emptyCapped" -> command.collection)
  }
}

object BSONRenameCollectionImplicits {
  implicit object RenameCollectionWriter extends BSONDocumentWriter[RenameCollection] {
    def write(command: RenameCollection): BSONDocument =
      BSONDocument(
        "renameCollection" -> command.fullyQualifiedCollectionName,
        "to" -> command.fullyQualifiedTargetName,
        "dropTarget" -> command.dropTarget)
  }
}

object BSONCreateImplicits {
  implicit object CappedWriter extends BSONDocumentWriter[Capped] {
    def write(capped: Capped): BSONDocument =
      BSONDocument(
        "size" -> capped.size,
        "max" -> capped.max)
  }
  implicit object CreateWriter extends BSONDocumentWriter[ResolvedCollectionCommand[Create]] {
    def write(command: ResolvedCollectionCommand[Create]): BSONDocument =
      BSONDocument(
      "create" -> command.collection,
      "autoIndexId" -> command.command.autoIndexId) ++ command.command.capped.map( capped =>
        CappedWriter.write(capped) ++ ("capped" -> true)).getOrElse(BSONDocument())
  }
}

object BSONCollStatsImplicits {
  implicit object CollStatsWriter extends BSONDocumentWriter[ResolvedCollectionCommand[CollStats]] {
    def write(command: ResolvedCollectionCommand[CollStats]): BSONDocument =
      BSONDocument(
        "collStats" -> command.collection,
        "scale" -> command.command.scale)
  }
  implicit object CollStatsResultReader extends BSONDocumentReader[CollStatsResult] {
    def read(doc: BSONDocument): CollStatsResult =
      CollStatsResult(
        doc.getAs[String]("ns").get,
        doc.getAs[Int]("count").get,
        doc.getAs[BSONNumberLike]("size").get.toDouble,
        doc.getAs[Double]("avgObjSize"),
        doc.getAs[BSONNumberLike]("storageSize").get.toDouble,
        doc.getAs[Int]("numExtents").get,
        doc.getAs[Int]("nindexes").get,
        doc.getAs[Int]("lastExtentSize").get,
        doc.getAs[Double]("paddingFactor").get,
        doc.getAs[Int]("systemFlags"),
        doc.getAs[Int]("userFlags"),
        doc.getAs[Int]("totalIndexSize").get,
        {
          val indexSizes = doc.getAs[BSONDocument]("indexSizes").get
          (for (kv <- indexSizes.elements) yield kv._1 -> kv._2.asInstanceOf[BSONInteger].value).toArray
        },
        doc.getAs[BSONBooleanLike]("capped").map(_.toBoolean).getOrElse(false),
        doc.getAs[BSONNumberLike]("max").map(_.toLong))
  }
}

object BSONConvertToCappedImplicits {
  implicit object ConvertToCappedWriter extends BSONDocumentWriter[ResolvedCollectionCommand[ConvertToCapped]] {
    def write(command: ResolvedCollectionCommand[ConvertToCapped]): BSONDocument =
      BSONDocument("convertToCapped" -> command.collection) ++ BSONCreateImplicits.CappedWriter.write(command.command.capped)
  }
}