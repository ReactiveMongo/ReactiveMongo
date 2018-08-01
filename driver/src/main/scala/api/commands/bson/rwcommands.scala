package reactivemongo.api.commands.bson

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands._
import reactivemongo.bson._

@deprecated("Internally use CommandCodecs", "0.16.0")
object BSONGetLastErrorImplicits {
  implicit object GetLastErrorWriter extends BSONDocumentWriter[GetLastError] {
    private val underlying =
      CommandCodecs.writeGetLastErrorWWriter(BSONSerializationPack.newBuilder)

    def write(wc: GetLastError): BSONDocument = {
      BSONDocument(
        "getlasterror" -> 1,
        "w" -> underlying(wc.w),
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
      wnote = doc.get("wnote").flatMap {
        case BSONString("majority") =>
          Some(GetLastError.Majority)

        case BSONString(tagSet) =>
          Some(GetLastError.TagSet(tagSet))

        case BSONInteger(acks) =>
          Some(GetLastError.WaitForAcknowledgments(acks))

        case _ => Option.empty
      },
      wtimeout = doc.getAs[Boolean]("wtimeout").getOrElse(false),
      waited = doc.getAs[Int]("waited"),
      wtime = doc.getAs[Int]("wtime"))
  }
}

@deprecated("Internally use CommandCodecs", "0.16.0")
object BSONCommonWriteCommandsImplicits {
  implicit object GetLastErrorWWriter
    extends BSONWriter[GetLastError.W, BSONValue] {

    private val underlying =
      CommandCodecs.writeGetLastErrorWWriter(BSONSerializationPack.newBuilder)

    def write(w: GetLastError.W): BSONValue = underlying(w)
  }

  @deprecated("Internally use CommandCodecs", "0.16.0")
  implicit object WriteConcernWriter extends BSONDocumentWriter[WriteConcern] {
    private val writer = CommandCodecs.writeWriteConcern(BSONSerializationPack)

    def write(wc: WriteConcern): BSONDocument = writer(wc)
  }

  @deprecated("Internally use CommandCodecs", "0.16.0")
  implicit object WriteErrorReader extends BSONDocumentReader[WriteError] {
    private val underlying =
      CommandCodecs.readWriteError(BSONSerializationPack.newDecoder)

    def read(doc: BSONDocument): WriteError = underlying(doc)
  }

  @deprecated("Internally use CommandCodecs", "0.16.0")
  implicit object WriteConcernErrorReader
    extends BSONDocumentReader[WriteConcernError] {
    private val underlying =
      CommandCodecs.readWriteConcernError(BSONSerializationPack.newDecoder)

    def read(doc: BSONDocument): WriteConcernError = underlying(doc)
  }

  @deprecated("Use internal CommandCodecs", "0.13.1")
  implicit object DefaultWriteResultReader
    extends DealingWithGenericCommandErrorsReader[DefaultWriteResult] {
    private val underlying =
      CommandCodecs.defaultWriteResultReader(BSONSerializationPack)

    def readResult(doc: BSONDocument): DefaultWriteResult = underlying.read(doc)
  }
}

@deprecated("Will be private/internal", "0.16.0")
object BSONInsertCommand extends InsertCommand[BSONSerializationPack.type] {
  val pack = BSONSerializationPack
}

@deprecated("Will be private/internal", "0.16.0")
object BSONInsertCommandImplicits {
  import BSONInsertCommand._

  object InsertWriter
    extends BSONDocumentWriter[ResolvedCollectionCommand[Insert]] {

    private val underlying =
      InsertCommand.writer(BSONSerializationPack)(BSONInsertCommand)(None)

    def write(insert: ResolvedCollectionCommand[Insert]): BSONDocument =
      underlying(insert)
  }
}

@deprecated("Will be private/internal", "0.16.0")
object BSONUpdateCommand extends UpdateCommand[BSONSerializationPack.type] {
  val pack = BSONSerializationPack
}

@deprecated("Will be private/internal", "0.16.0")
object BSONUpdateCommandImplicits {
  import BSONUpdateCommand._
  import BSONCommonWriteCommandsImplicits._

  @deprecated("Will be private/internal", "0.16.0")
  implicit object UpdateElementWriter extends BSONDocumentWriter[UpdateElement] {
    def write(element: UpdateElement) = BSONDocument(
      "q" -> element.q,
      "u" -> element.u,
      "upsert" -> element.upsert,
      "multi" -> element.multi)
  }

  @deprecated("Will be private/internal", "0.16.0")
  implicit object UpdateWriter extends BSONDocumentWriter[ResolvedCollectionCommand[Update]] {
    def write(update: ResolvedCollectionCommand[Update]) = BSONDocument(
      "update" -> update.collection,
      "updates" -> update.command.updates,
      "ordered" -> update.command.ordered,
      "writeConcern" -> update.command.writeConcern)
  }

  @deprecated("Will be removed", "0.16.0")
  implicit object UpsertedReader extends BSONDocumentReader[Upserted] {
    private val underlying =
      CommandCodecs.readUpserted(BSONSerializationPack.newDecoder)

    def read(doc: BSONDocument) = underlying(doc)
  }

  @deprecated("Will be removed", "0.16.0")
  implicit object UpdateResultReader extends DealingWithGenericCommandErrorsReader[UpdateResult] {
    private val underlying =
      UpdateCommand.reader(BSONSerializationPack)(BSONUpdateCommand)

    def readResult(doc: BSONDocument): UpdateResult = underlying.read(doc)
  }
}

@deprecated("Will be private/internal", "0.16.0")
object BSONDeleteCommand extends DeleteCommand[BSONSerializationPack.type] {
  val pack = BSONSerializationPack
}

@deprecated("Will be private/internal", "0.16.0")
object BSONDeleteCommandImplicits {
  import BSONDeleteCommand._
  import BSONCommonWriteCommandsImplicits._

  @deprecated("Use internal writer", "0.13.1")
  implicit object DeleteElementWriter extends BSONDocumentWriter[DeleteElement] {
    def write(element: DeleteElement): BSONDocument = {
      BSONDocument(
        "q" -> element.q,
        "limit" -> element.limit)
    }
  }

  @deprecated("Use internal writer", "0.13.1")
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
