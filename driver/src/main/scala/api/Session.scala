package reactivemongo.api

import java.util.UUID

import java.util.concurrent.atomic.AtomicReference

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
  def transaction: Option[SessionTransaction]

  @inline def operationTime: Option[Long] = Option.empty[Long]

  /** No-op as not tracking times, save for [[ReplicaSetSession]]. */
  private[api] val update: Function2[Long, Option[Long], Session] =
    (_, _) => this // No-op

  /** Returns `Some` newly started transaction if any. */
  private[api] val startTransaction: WriteConcern => Option[SessionTransaction] = _ => transaction

  private[api] def transactionToFlag(): Boolean = false

  /** Returns `Some` ended transaction if any. */
  private[api] def endTransaction(): Option[SessionTransaction] = transaction

  // ---

  override def equals(that: Any): Boolean = that match {
    case other: Session =>
      (lsid -> causalConsistency) == (other.lsid -> other.causalConsistency)

    case _ => false
  }

  override lazy val hashCode: Int = (lsid -> causalConsistency).hashCode

  override def toString = s"${getClass.getName}($lsid, $causalConsistency)"
}

private[reactivemongo] final class PlainSession(
  lsid: UUID,
  causalConsistency: Boolean = true) extends Session(lsid, causalConsistency) {

  val transaction = Option.empty[SessionTransaction]
}

private[reactivemongo] final class ReplicaSetSession(
  lsid: UUID,
  causalConsistency: Boolean = true) extends Session(lsid, causalConsistency) {

  private val txState = new AtomicReference[SessionTransaction](
    SessionTransaction(
      txnNumber = 0L,
      writeConcern = Option.empty[WriteConcern], // not started
      flagSent = false))

  private val gossip = new AtomicReference(0L -> 0L)

  override private[api] val update: Function2[Long, Option[Long], Session] =
    (operationTime, clusterTime) => {
      gossip.getAndAccumulate(
        operationTime -> clusterTime.getOrElse(0),
        Session.UpdateGossip)

      this
    }

  override def operationTime = Option(gossip.get()).collect {
    case (opTime, _) if opTime > 0 => opTime
  }

  def transaction: Option[SessionTransaction] = Option(txState.get()).collect {
    case tx if tx.isStarted => tx
  }

  override private[api] val startTransaction: WriteConcern => Option[SessionTransaction] = { wc =>
    val startOp = new Session.IncTxnNumberIfNotStarted(wc)

    Option(txState updateAndGet startOp).collect {
      case tx if startOp.updated => tx
    }
  }

  override private[api] def transactionToFlag(): Boolean = {
    val before = txState.getAndUpdate(Session.TransactionStartSent)

    before.flagSent // was not sent before, so need to send it now
  }

  override private[api] def endTransaction(): Option[SessionTransaction] =
    Option(txState getAndUpdate Session.EndTxIfStarted).collect {
      case tx if tx.isStarted => tx
    }
}

private[api] object Session {
  import java.util.function.{ BinaryOperator, UnaryOperator }

  private val logger =
    reactivemongo.util.LazyLogger("reactivemongo.api.Session")

  def updateOnResponse(
    session: Session,
    response: Response)(implicit ec: ExecutionContext): Future[(Session, Response)] = Response.preload(response).map {
    case (resp, preloaded) =>
      val opTime = preloaded.get("operationTime").collect {
        case BSONTimestamp(time) => time
      }

      opTime.fold(session -> resp) { operationTime =>
        logger.debug(s"Update session ${session.lsid} with response to #${response.header.responseTo} at $operationTime")

        val clusterTime = for {
          nested <- preloaded.getAs[BSONDocument](f"$$clusterTime")
          time <- nested.get("clusterTime").collect {
            case BSONTimestamp(value) => value
          }
        } yield time

        session.update(operationTime, clusterTime) -> resp
      }
  }

  object UpdateGossip extends BinaryOperator[(Long, Long)] {
    def apply(current: (Long, Long), upd: (Long, Long)): (Long, Long) =
      (current._1 max upd._1) -> (current._2 max upd._2)
  }

  final class IncTxnNumberIfNotStarted(
    wc: WriteConcern) extends UnaryOperator[SessionTransaction] {

    var updated: Boolean = false

    def apply(current: SessionTransaction): SessionTransaction =
      if (current.isStarted) {
        current // unchanged
      } else {
        updated = true

        current.copy(
          txnNumber = current.txnNumber + 1L,
          writeConcern = Some(wc), // started with given WriteConcern
          flagSent = false)
      }
  }

  object EndTxIfStarted extends UnaryOperator[SessionTransaction] {
    def apply(current: SessionTransaction): SessionTransaction =
      if (current.isStarted) {
        current.copy(writeConcern = None, flagSent = false)
      } else {
        current
      }
  }

  object TransactionStartSent extends UnaryOperator[SessionTransaction] {
    def apply(current: SessionTransaction): SessionTransaction =
      if (current.isStarted) {
        current.copy(flagSent = true)
      } else {
        current
      }
  }
}

/**
 * @param txnNumber the current transaction number
 * @param writeConcern the write concern if the session transaction is started
 */
private[reactivemongo] case class SessionTransaction(
  txnNumber: Long,
  writeConcern: Option[WriteConcern],
  flagSent: Boolean) {

  @inline def isStarted: Boolean = writeConcern.isDefined
}
