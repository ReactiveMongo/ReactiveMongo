package reactivemongo.api

import java.util.UUID

import java.util.concurrent.atomic.AtomicLong

/**
 * The [[https://docs.mongodb.com/manual/reference/server-sessions/#command-options options]] to execute commands using a started session.
 *
 * @param lsid the ID of the logical (server) session
 * @param causalConsistency the causal consistency
 */
private[api] sealed abstract class Session(
  val lsid: UUID,
  val causalConsistency: Boolean) {

  /** Only if obtained from a replicaset. */
  def nextTxnNumber(): Option[Long]

  // ---

  override def equals(that: Any): Boolean = that match {
    case other: Session =>
      (lsid -> causalConsistency) == (other.lsid -> other.causalConsistency)

    case _ => false
  }

  override lazy val hashCode: Int = (lsid -> causalConsistency).hashCode
}

private[api] final class PlainSession(
  lsid: UUID,
  causalConsistency: Boolean = true) extends Session(lsid, causalConsistency) {

  @inline def nextTxnNumber() = Option.empty[Long]
}

private[api] final class ReplicaSetSession(
  lsid: UUID,
  causalConsistency: Boolean = true) extends Session(lsid, causalConsistency) {

  private val transactionNumber = new AtomicLong()

  def nextTxnNumber(): Option[Long] =
    Option(transactionNumber.incrementAndGet())
}
