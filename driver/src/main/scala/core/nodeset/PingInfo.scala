package reactivemongo.core.nodeset

/**
 * @param ping the response delay for the last IsMaster request (duration between request and its response, or `Long.MaxValue`)
 * @param lastIsMasterTime the timestamp when the last IsMaster request has been sent (or 0)
 * @param lastIsMasterId the ID of the last IsMaster request (or -1 if none)
 */
case class PingInfo(
  ping: Long = Long.MaxValue,
  lastIsMasterTime: Long = 0,
  lastIsMasterId: Int = -1)

object PingInfo {
  // TODO: Use MongoConnectionOption (e.g. monitorRefreshMS)
  val pingTimeout = 60 * 1000
}
