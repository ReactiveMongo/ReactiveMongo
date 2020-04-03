package reactivemongo.api.commands

import reactivemongo.api.{ PackSupport, SerializationPack }

private[reactivemongo] trait CommandCodecsWithPack[P <: SerializationPack with Singleton] { _: PackSupport[P] =>
  final protected implicit lazy val resultReader: pack.Reader[DefaultWriteResult] = CommandCodecs.defaultWriteResultReader(pack)

}
