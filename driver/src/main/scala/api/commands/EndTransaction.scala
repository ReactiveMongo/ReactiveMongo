package reactivemongo.api.commands

import reactivemongo.api.{ Serialization, Session, WriteConcern => WC }

/**
 * Support for [[https://docs.mongodb.com/manual/reference/command/abortTransaction/ abortTransaction]] and [[https://docs.mongodb.com/manual/reference/command/commitTransaction/ commitTransaction]] commands.
 */
private[reactivemongo] sealed abstract class EndTransaction(
  val session: Session,
  val writeConcern: WC) extends Command with CommandWithResult[UnitBox.type] {
  protected def kind: String
}

private[reactivemongo] object EndTransaction {
  /** Returns an [[https://docs.mongodb.com/manual/reference/command/abortTransaction/ abortTransaction]] command */
  def abort(session: Session, writeConcern: WC): EndTransaction =
    new EndTransaction(session, writeConcern) {
      val kind = "abortTransaction"
    }

  /** Returns an [[https://docs.mongodb.com/manual/reference/command/commitTransaction/ commitTransaction]] command */
  def commit(session: Session, writeConcern: WC): EndTransaction =
    new EndTransaction(session, writeConcern) {
      val kind = "commitTransaction"
    }

  def commandWriter: Serialization.Pack#Writer[EndTransaction] = {
    val pack = Serialization.internalSerializationPack
    val builder = pack.newBuilder
    val writeWriteConcern = CommandCodecs.writeWriteConcern(builder)
    val writeSession = CommandCodecs.writeSession(builder)

    import builder.elementProducer

    pack.writer[EndTransaction] { end =>
      val elements = Seq.newBuilder[pack.ElementProducer]

      elements ++= Seq(
        elementProducer(end.kind, builder.int(1)),
        elementProducer("writeConcern", writeWriteConcern(end.writeConcern)))

      elements ++= writeSession(end.session)

      end.session.transaction.toOption.flatMap(_.recoveryToken).foreach { t =>
        elements += elementProducer("recoveryToken", t)
      }

      builder.document(elements.result())
    }
  }
}
