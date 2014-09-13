package reactivemongo.api.commands.bson

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands._
import reactivemongo.bson._

private case class Implicits[W <: WriteCommandsCommon[BSONSerializationPack.type]](subject: W) {
  implicit object WriteConcernWriter extends BSONDocumentWriter[WriteConcern] {
    def write(wc: WriteConcern): BSONDocument = BSONDocument(
      "w" -> ((wc.w match {
        case GetLastError.Majority => BSONString("majority")
        case GetLastError.TagSet(tagSet) => BSONString(tagSet)
        case GetLastError.WaitForAknowledgments(n) => BSONInteger(n)
      }): BSONValue),
      "j" -> (if(wc.j) Some(true) else None),
      "wtimeout" -> wc.wtimeout)
  }
  implicit object WriteErrorReader extends BSONDocumentReader[subject.WriteError] {
    def read(doc: BSONDocument): subject.WriteError =
      subject.WriteError(
        index = doc.getAs[Int]("index").get,
        code = doc.getAs[Int]("code").get,
        errmsg = doc.getAs[String]("errmsg").get)
  }
  implicit object WriteConcernErrorReader extends BSONDocumentReader[subject.WriteConcernError] {
    def read(doc: BSONDocument): subject.WriteConcernError =
      subject.WriteConcernError(
        code = doc.getAs[Int]("code").get,
        errmsg = doc.getAs[String]("errmsg").get)
  }
}

object BSONInsertCommand extends InsertCommand[BSONSerializationPack.type] {
  val pack = BSONSerializationPack
}

object BSONInsertCommandImplicits {
  private val imp = Implicits(BSONInsertCommand)
  import BSONInsertCommand._
  import imp._

  implicit object InsertWriter extends BSONDocumentWriter[ResolvedCollectionCommand[Insert]] {
    def write(command: ResolvedCollectionCommand[Insert]) = {
      BSONDocument(
        "insert" -> command.collection,
        "documents" -> BSONArray(command.command.documents),
        "ordered" -> command.command.ordered,
        "writeConcern" -> command.command.writeConcern
      )
    }
  }
  implicit object InsertResultReader extends BSONDocumentReader[InsertResult] {
    def read(doc: BSONDocument): InsertResult = InsertResult(
      ok = doc.getAs[Int]("ok").exists(_ != 0),
      n = doc.getAs[Int]("n").get,
      writeErrors = doc.getAs[Seq[WriteError]]("writeErrors").getOrElse(Seq.empty),
      writeConcernError = doc.getAs[WriteConcernError]("writeConcernError")
    )
  }
}

object BSONUpdateCommand extends UpdateCommand[BSONSerializationPack.type] {
  val pack = BSONSerializationPack
}

object BSONUpdateCommandImplicits {
  private val imp = Implicits(BSONUpdateCommand)
  import BSONUpdateCommand._
  import imp._

  implicit object UpdateElementWriter extends BSONDocumentWriter[UpdateElement] {
    def write(element: UpdateElement) =
      BSONDocument(
        "q" -> element.q,
        "u" -> element.u,
        "upsert" -> element.upsert,
        "multi" -> element.multi
      )
  }
  implicit object UpdateWriter extends BSONDocumentWriter[ResolvedCollectionCommand[Update]] {
    def write(update: ResolvedCollectionCommand[Update]) = {
      BSONDocument(
        "update" -> update.collection,
        "updates" -> BSONArray(update.command.documents),
        "ordered" -> update.command.ordered,
        "writeConcern" -> update.command.writeConcern
      )
    }
  }
  implicit object UpsertedReader extends BSONDocumentReader[Upserted] {
    def read(doc: BSONDocument): Upserted = {
      Upserted(
        index = doc.getAs[Int]("index").get,
        _id = doc.get("_id"))
    }
  }
  implicit object UpdateResultReader extends BSONDocumentReader[UpdateResult] {
    def read(doc: BSONDocument): UpdateResult = {
      UpdateResult(
        ok = doc.getAs[Int]("ok").exists(_ != 0),
        n = doc.getAs[Int]("n").getOrElse(0),
        nModified = doc.getAs[Int]("nModified").getOrElse(0),
        upserted = doc.getAs[Seq[Upserted]]("writeErrors").getOrElse(Seq.empty),
        writeErrors = doc.getAs[Seq[WriteError]]("writeErrors").getOrElse(Seq.empty),
        writeConcernError = doc.getAs[WriteConcernError]("writeConcernError")
      )
    }
  }
}

object BSONDeleteCommand extends DeleteCommand[BSONSerializationPack.type] {
  val pack = BSONSerializationPack
}

object BSONDeleteCommandImplicits {
  private val imp = Implicits(BSONDeleteCommand)
  import BSONDeleteCommand._
  import imp._

  implicit object DeleteElementWriter extends BSONDocumentWriter[DeleteElement] {
    def write(element: DeleteElement): BSONDocument = {
      BSONDocument(
        "q" -> element.q,
        "limit" -> element.limit)
    }
  }

  implicit object DeleteWriter extends BSONDocumentWriter[ResolvedCollectionCommand[Delete]] {
    def write(delete: ResolvedCollectionCommand[Delete]): BSONDocument = {
      BSONDocument(
        "delete" -> delete.collection,
        "deletes" -> delete.command.deletes,
        "ordered" -> delete.command.ordered,
        "writeConcern" -> delete.command.writeConcern)
    }
  }

  implicit object DeleteResultReader extends BSONDocumentReader[DeleteResult] {
    def read(doc: BSONDocument): DeleteResult = {
      DeleteResult(
        ok = doc.getAs[Int]("ok").exists(_ != 0),
        n = doc.getAs[Int]("n").get,
        writeErrors = doc.getAs[Seq[WriteError]]("writeErrors").getOrElse(Seq.empty),
        writeConcernError = doc.getAs[WriteConcernError]("writeConcernError"))
    }
  }
}