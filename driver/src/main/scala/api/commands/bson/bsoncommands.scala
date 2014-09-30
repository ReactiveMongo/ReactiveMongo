package reactivemongo.api.commands.bson

import reactivemongo.api.commands._
import reactivemongo.bson._

object CommonImplicits {
  implicit object UnitBoxReader extends BSONDocumentReader[UnitBox.type] {
    def read(doc: BSONDocument): UnitBox.type = UnitBox
  }
}