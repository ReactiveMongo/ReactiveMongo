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
  /**
   * maxBulkSize = 1000
   */
  val Default = ProtocolMetadata(
    minWireVersion = MongoWireVersion.V30,
    maxWireVersion = MongoWireVersion.V30,
    maxMessageSizeBytes = 48000000,
    maxBsonSize = 16 * 1024 * 1024,
    maxBulkSize = 1000)
}
