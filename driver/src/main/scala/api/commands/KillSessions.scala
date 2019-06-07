package reactivemongo.api.commands

import java.util.UUID

import reactivemongo.api.SerializationPack

// TODO: EndSession like EndTransaction
// See https://docs.mongodb.com/manual/reference/command/killSessions/
// See https://docs.mongodb.com/manual/reference/command/endSessions/
private[reactivemongo] final class KillSessions(val id: UUID, val ids: UUID*)
  extends Command with CommandWithResult[UnitBox.type]

private[reactivemongo] object KillSessions {
  def commandWriter[P <: SerializationPack](pack: P): pack.Writer[KillSessions] = {
    val builder = pack.newBuilder
    import builder.{ document, elementProducer => element }

    def uuid(id: UUID) = document(Seq(element("id", builder.uuid(id))))

    pack.writer[KillSessions] { kill =>
      document(Seq(element(
        "killSessions",
        builder.array(uuid(kill.id), kill.ids.map(uuid)))))
    }
  }
}
