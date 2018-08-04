package reactivemongo.api

import java.util.UUID

import java.util.concurrent.atomic.{ AtomicLong, AtomicReference }

import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.bson.{ BSONDocument, BSONTimestamp }
import reactivemongo.core.protocol.Response

/**
 * The [[https://docs.mongodb.com/manual/reference/server-sessions/#command-options options]] to execute commands using a started session.
 *
 * @param lsid the ID of the logical (server) session
 * @param causalConsistency the causal consistency
 */
private[reactivemongo] sealed abstract class Session(
  val lsid: UUID,
  val causalConsistency: Boolean) {

  /** Only if obtained from a replicaset. */
  def nextTxnNumber(): Option[Long]

  @inline def operationTime: Option[Long] = Option.empty[Long]

  /** No-op as not tracking times, save for [[ReplicaSetSession]]. */
  private[api] def update(
    operationTime: Long,
    clusterTime: Option[Long]): Session = this // No-op

  // ---

  override def equals(that: Any): Boolean = that match {
    case other: Session =>
      (lsid -> causalConsistency) == (other.lsid -> other.causalConsistency)

    case _ => false
  }

  override lazy val hashCode: Int = (lsid -> causalConsistency).hashCode
}

private[reactivemongo] final class PlainSession(
  lsid: UUID,
  causalConsistency: Boolean = true) extends Session(lsid, causalConsistency) {

  @inline def nextTxnNumber() = Option.empty[Long]
}

private[reactivemongo] final class ReplicaSetSession(
  lsid: UUID,
  causalConsistency: Boolean = true) extends Session(lsid, causalConsistency) {

  private val transactionNumber = new AtomicLong()

  private val gossip = new AtomicReference(0L -> 0L)

  override private[api] def update(
    operationTime: Long,
    clusterTime: Option[Long]): Session = {

    gossip.getAndAccumulate(
      operationTime -> clusterTime.getOrElse(0),
      Session.UpdateGossip)

    this
  }

  override def operationTime = Option(gossip.get()).collect {
    case (opTime, _) if opTime > 0 => opTime
  }

  def nextTxnNumber(): Option[Long] =
    Option(transactionNumber.incrementAndGet())
}

private[api] object Session {
  object UpdateGossip extends java.util.function.BinaryOperator[(Long, Long)] {
    def apply(current: (Long, Long), upd: (Long, Long)): (Long, Long) =
      (current._1 max upd._1) -> (current._2 max upd._2)
  }

  private[api] def updateOnResponse(
    session: Session,
    response: Response)(implicit ec: ExecutionContext): Future[(Session, Response)] = Response.preload(response).map {
    case (resp, preloaded) =>
      val opTime = preloaded.get("operationTime").collect {
        case BSONTimestamp(time) => time
      }

      opTime.fold(session -> resp) { operationTime =>
        //TODO: logging/trace: println(s"session.operationTime = $operationTime")

        val clusterTime = for {
          nested <- preloaded.getAs[BSONDocument](f"$$clusterTime")
          time <- nested.get("clusterTime").collect {
            case BSONTimestamp(value) => value
          }
        } yield time

        session.update(operationTime, clusterTime) -> resp
      }
  }
}
