package reactivemongo.api.commands.bson

import reactivemongo.bson.{
  BSONBooleanLike,
  BSONDocument,
  BSONDocumentReader,
  BSONDocumentWriter
}

import reactivemongo.api.ReadConcern
import reactivemongo.api.commands.{ Command, CommandError, UnitBox }

@deprecated("Will be private/internal", "0.16.0")
object CommonImplicits { // See CommandCodecs
  implicit object UnitBoxReader
    extends DealingWithGenericCommandErrorsReader[UnitBox.type] {
    def readResult(doc: BSONDocument): UnitBox.type = UnitBox
  }

  implicit object ReadConcernWriter extends BSONDocumentWriter[ReadConcern] {
    def write(concern: ReadConcern) = BSONDocument("level" -> concern.level)
  }
}

@deprecated("Will be private/internal", "0.16.0")
trait BSONCommandError extends CommandError {
  def originalDocument: BSONDocument
}

// See CommandError.apply
@deprecated("Will be private/internal", "0.16.0")
case class DefaultBSONCommandError(
  code: Option[Int],
  errmsg: Option[String],
  originalDocument: BSONDocument) extends BSONCommandError {
  override def getMessage = s"CommandError[code=${code.getOrElse("<unknown>")}, errmsg=${errmsg.getOrElse("<unknown>")}, doc: ${BSONDocument.pretty(originalDocument)}]"
}

/** Helper to read a command result, with error handling. */
@deprecated("Will be private/internal", "0.16.0")
trait DealingWithGenericCommandErrorsReader[A] extends BSONDocumentReader[A] {
  /** Results the successful result (only if `ok` is true). */
  def readResult(doc: BSONDocument): A

  final def read(doc: BSONDocument): A = {
    if (!doc.getAs[BSONBooleanLike]("ok").forall(_.toBoolean)) {
      throw new DefaultBSONCommandError(
        code = doc.getAs[Int]("code"),
        errmsg = doc.getAs[String]("errmsg"),
        originalDocument = doc)
    } else {
      doc.getAs[String]("note").foreach { note =>
        Command.logger.info(s"${note}: ${BSONDocument pretty doc}")
      }

      readResult(doc)
    }
  }
}
