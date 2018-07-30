package reactivemongo.api

import java.util.UUID

/** See [[https://docs.mongodb.com/manual/reference/server-sessions/#command-options commands options]] related to logical session. */
private[api] sealed abstract class Session(val lsid: UUID) {
  /** Only if obtained from a replicaset. */
  def nextTxnNumber(): Option[Long]
}

private[api] final class PlainSession(lsid: UUID) extends Session(lsid) {
  @inline def nextTxnNumber() = Option.empty[Long]
}

private[api] final class ReplicaSetSession(lsid: UUID) extends Session(lsid) {
  private val transactionNumber = new java.util.concurrent.atomic.AtomicLong()

  def nextTxnNumber(): Option[Long] =
    Option(transactionNumber.incrementAndGet())
}
