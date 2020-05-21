package reactivemongo.api.commands

import java.util.UUID

import reactivemongo.api.SerializationPack

// See https://docs.mongodb.com/manual/reference/command/startSession/
private[reactivemongo] object StartSession
  extends Command with CommandWithResult[StartSessionResult] {

  def commandWriter[P <: SerializationPack](pack: P): pack.Writer[StartSession.type] = {
    val builder = pack.newBuilder

    pack.writer[StartSession.type] { _ =>
      builder.document(Seq(
        builder.elementProducer("startSession", builder.int(1))))
    }
  }
}

private[reactivemongo] case class StartSessionResult(
  id: UUID,
  timeoutMinutes: Int)

private[reactivemongo] object StartSessionResult {
  import CommandCodecs.{ dealingWithGenericCommandExceptionsReaderOpt => cmdReader }

  def reader[P <: SerializationPack](pack: P): pack.Reader[StartSessionResult] = {
    val decoder = pack.newDecoder

    cmdReader[pack.type, StartSessionResult](pack) { doc =>
      (for {
        idCont <- decoder.child(doc, "id")
        uuid <- decoder.uuid(idCont, "id")
        timeout <- decoder.int(doc, "timeoutMinutes")
      } yield StartSessionResult(uuid, timeout))
    }
  }
}
