package reactivemongo.api.commands.bson

import reactivemongo.api.commands._
import reactivemongo.bson._

@deprecated("Internally use CommandCodecs", "0.16.0")
object BSONGetLastErrorImplicits {

  implicit object LastErrorReader extends DealingWithGenericCommandErrorsReader[LastError] {
    def readResult(doc: BSONDocument): LastError = new LastError(
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
      wtime = doc.getAs[Int]("wtime"),
      writeErrors = Seq.empty,
      writeConcernError = Option.empty)

  }
}
