package reactivemongo.api.commands

import java.util.UUID

import reactivemongo.api.SerializationPack

// See https://docs.mongodb.com/manual/reference/command/endSessions/
private[reactivemongo] final class EndSessions(val id: UUID, val ids: UUID*)
  extends Command with CommandWithResult[UnitBox.type]

private[reactivemongo] object EndSessions {
  def commandWriter[P <: SerializationPack](pack: P): pack.Writer[EndSessions] = {
    val builder = pack.newBuilder
    import builder.{ document, elementProducer => element }

    def uuid(id: UUID) = document(Seq(element("id", builder.uuid(id))))

    pack.writer[EndSessions] { end =>
      document(Seq(element(
        "endSessions",
        builder.array(uuid(end.id), end.ids.map(uuid)))))
    }
  }
}
