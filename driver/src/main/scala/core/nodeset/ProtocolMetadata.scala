package reactivemongo.core.nodeset

import reactivemongo.core.protocol.MongoWireVersion

// TODO#1.1: Move to `protocol` package
case class ProtocolMetadata(
  minWireVersion: MongoWireVersion,
  maxWireVersion: MongoWireVersion,
  maxMessageSizeBytes: Int,
  maxBsonSize: Int,
  maxBulkSize: Int) {
  override lazy val toString =
    s"ProtocolMetadata($minWireVersion, $maxWireVersion)"
}

object ProtocolMetadata {
  val Default = ProtocolMetadata(
    MongoWireVersion.V30, MongoWireVersion.V30,
    48000000, 16 * 1024 * 1024, 1000)
}
