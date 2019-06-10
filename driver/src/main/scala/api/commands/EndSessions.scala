package reactivemongo.api.commands

import java.util.UUID

import reactivemongo.api.SerializationPack

/**
 * Support for [[https://docs.mongodb.com/manual/reference/command/killSessions/ killSession]] and [[https://docs.mongodb.com/manual/reference/command/endSessions/ endSessions]] commands.
 */
private[reactivemongo] sealed abstract class EndSessions(
  val id: UUID,
  val ids: Seq[UUID]) extends Command with CommandWithResult[UnitBox.type] {
  protected def kind: String
}

private[reactivemongo] object EndSessions {
  /** Returns a [[https://docs.mongodb.com/manual/reference/command/endSessions/ endSessions]] command */
  def end(id: UUID, ids: UUID*): EndSessions = new EndSessions(id, ids) {
    val kind = "endSessions"
  }

  /** Returns a [[https://docs.mongodb.com/manual/reference/command/killSessions/ killSessions]] command */
  def kill(id: UUID, ids: UUID*): EndSessions = new EndSessions(id, ids) {
    val kind = "killSessions"
  }

  def commandWriter[P <: SerializationPack](pack: P): pack.Writer[EndSessions] = {
    val builder = pack.newBuilder
    import builder.{ document, elementProducer => element }

    def uuid(id: UUID) = document(Seq(element("id", builder.uuid(id))))

    pack.writer[EndSessions] { cmd =>
      document(Seq(element(
        cmd.kind,
        builder.array(uuid(cmd.id), cmd.ids.map(uuid)))))
    }
  }
}
