package reactivemongo.api.commands

import reactivemongo.api.{ SerializationPack, Session, WriteConcern => WC }

/**
 * Support for [[https://docs.mongodb.com/manual/reference/command/abortTransaction/ abortTransaction]] and [[https://docs.mongodb.com/manual/reference/command/commitTransaction/ commitTransaction]] commands.
 */
private[reactivemongo] sealed abstract class EndTransaction(
  val session: Session,
  val writeConcern: WC) extends Command with CommandWithResult[UnitBox.type] {
  protected def kind: String
}

private[reactivemongo] object EndTransaction {
  def abort(session: Session, writeConcern: WC): EndTransaction =
    new EndTransaction(session, writeConcern) {
      val kind = "abortTransaction"
    }

  def commit(session: Session, writeConcern: WC): EndTransaction =
    new EndTransaction(session, writeConcern) {
      val kind = "commitTransaction"
    }

  def commandWriter[P <: SerializationPack](pack: P): pack.Writer[EndTransaction] = {
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

      builder.document(elements.result())
    }
  }
}
