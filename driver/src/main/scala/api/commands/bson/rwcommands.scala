package reactivemongo.api.commands.bson

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands._
import reactivemongo.bson._

object BSONGetLastErrorImplicits {
  implicit object GetLastErrorWriter extends BSONDocumentWriter[GetLastError] {
    def write(wc: GetLastError): BSONDocument = {
      val w = BSONCommonWriteCommandsImplicits.GetLastErrorWWriter.write(wc.w)
      BSONDocument(
        "getlasterror" -> 1,
        "w" -> w,
        "j" -> (if (wc.j) Some(true) else None),
        "wtimeout" -> wc.wtimeout)
    }
  }

  implicit object LastErrorReader extends DealingWithGenericCommandErrorsReader[LastError] {
    def readResult(doc: BSONDocument): LastError = LastError(
      ok = doc.getAs[BSONBooleanLike]("ok").map(_.toBoolean).getOrElse(false),
      errmsg = doc.getAs[String]("err"),
      code = doc.getAs[Int]("code"),
      lastOp = doc.getAs[BSONNumberLike]("lastOp").map(_.toLong),
      n = doc.getAs[Int]("n").getOrElse(0),
      singleShard = doc.getAs[String]("singleShard"),
      updatedExisting = doc.getAs[BSONBooleanLike]("updatedExisting").map(_.toBoolean).getOrElse(false),
      upserted = doc.getAs[BSONValue]("upserted"),
      wnote = doc.get("wnote").map {
        case BSONString("majority") => GetLastError.Majority
        case BSONString(tagSet)     => GetLastError.TagSet(tagSet)
        case BSONInteger(acks)      => GetLastError.WaitForAknowledgments(acks)
      },
      wtimeout = doc.getAs[Boolean]("wtimeout").getOrElse(false),
      waited = doc.getAs[Int]("waited"),
      wtime = doc.getAs[Int]("wtime"))
  }
}

object BSONCommonWriteCommandsImplicits {
  implicit object GetLastErrorWWriter
      extends BSONWriter[GetLastError.W, BSONValue] {
    def write(w: GetLastError.W): BSONValue = w match {
      case GetLastError.Majority                 => BSONString("majority")
      case GetLastError.TagSet(tagSet)           => BSONString(tagSet)
      case GetLastError.WaitForAknowledgments(n) => BSONInteger(n)
    }
  }

  implicit object WriteConcernWriter extends BSONDocumentWriter[WriteConcern] {
    def write(wc: WriteConcern): BSONDocument = BSONDocument(
      "w" -> wc.w,
      "j" -> (if (wc.j) Some(true) else None),
      "wtimeout" -> wc.wtimeout)
  }

  implicit object WriteErrorReader extends BSONDocumentReader[WriteError] {
    def read(doc: BSONDocument): WriteError =
      WriteError(
        index = doc.getAs[Int]("index").get,
        code = doc.getAs[Int]("code").get,
        errmsg = doc.getAs[String]("errmsg").get)
  }

  implicit object WriteConcernErrorReader
      extends BSONDocumentReader[WriteConcernError] {
    def read(doc: BSONDocument): WriteConcernError =
      WriteConcernError(
        code = doc.getAs[Int]("code").get,
        errmsg = doc.getAs[String]("errmsg").get)
  }

  implicit object DefaultWriteResultReader
      extends DealingWithGenericCommandErrorsReader[DefaultWriteResult] {
    def readResult(doc: BSONDocument): DefaultWriteResult = {
      DefaultWriteResult(
        ok = doc.getAs[Int]("ok").exists(_ != 0),
        n = doc.getAs[Int]("n").getOrElse(0),
        writeErrors = doc.getAs[Seq[WriteError]]("writeErrors").getOrElse(Seq.empty),
        writeConcernError = doc.getAs[WriteConcernError]("writeConcernError"),
        code = doc.getAs[Int]("code"),
        errmsg = doc.getAs[String]("errmsg"))
    }
  }
}

object BSONInsertCommand extends InsertCommand[BSONSerializationPack.type] {
  val pack = BSONSerializationPack
}

object BSONInsertCommandImplicits {
  import BSONInsertCommand._
  import BSONCommonWriteCommandsImplicits._

  implicit object InsertWriter extends BSONDocumentWriter[ResolvedCollectionCommand[Insert]] {
    def write(command: ResolvedCollectionCommand[Insert]) = {
      BSONDocument(
        "insert" -> command.collection,
        "documents" -> BSONArray(command.command.documents),
        "ordered" -> command.command.ordered,
        "writeConcern" -> command.command.writeConcern)
    }
  }
}

object BSONUpdateCommand extends UpdateCommand[BSONSerializationPack.type] {
  val pack = BSONSerializationPack
}

object BSONUpdateCommandImplicits {
  import BSONUpdateCommand._
  import BSONCommonWriteCommandsImplicits._

  implicit object UpdateElementWriter extends BSONDocumentWriter[UpdateElement] {
    def write(element: UpdateElement) =
      BSONDocument(
        "q" -> element.q,
        "u" -> element.u,
        "upsert" -> element.upsert,
        "multi" -> element.multi)
  }

  implicit object UpdateWriter extends BSONDocumentWriter[ResolvedCollectionCommand[Update]] {
    def write(update: ResolvedCollectionCommand[Update]) = {
      BSONDocument(
        "update" -> update.collection,
        "updates" -> update.command.documents,
        "ordered" -> update.command.ordered,
        "writeConcern" -> update.command.writeConcern)
    }
  }

  implicit object UpsertedReader extends BSONDocumentReader[Upserted] {
    def read(doc: BSONDocument): Upserted = {
      Upserted(
        index = doc.getAs[Int]("index").get,
        _id = doc.get("_id").get)
    }
  }

  implicit object UpdateResultReader extends DealingWithGenericCommandErrorsReader[UpdateResult] {
    def readResult(doc: BSONDocument): UpdateResult = {
      UpdateWriteResult(
        ok = doc.getAs[Int]("ok").exists(_ != 0),
        n = doc.getAs[Int]("n").getOrElse(0),
        nModified = doc.getAs[Int]("nModified").getOrElse(0),
        upserted = doc.getAs[Seq[Upserted]]("upserted").getOrElse(Seq.empty),
        writeErrors = doc.getAs[Seq[WriteError]]("writeErrors").getOrElse(Seq.empty),
        writeConcernError = doc.getAs[WriteConcernError]("writeConcernError"),
        code = doc.getAs[Int]("code"), //FIXME There is no corresponding official docs.
        errmsg = doc.getAs[String]("errmsg") //FIXME There is no corresponding official docs.
        )
    }
  }
}

object BSONDeleteCommand extends DeleteCommand[BSONSerializationPack.type] {
  val pack = BSONSerializationPack
}

object BSONDeleteCommandImplicits {
  import BSONDeleteCommand._
  import BSONCommonWriteCommandsImplicits._

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
}
