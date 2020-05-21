package reactivemongo.api

import java.util.UUID

import java.util.concurrent.atomic.AtomicReference

import scala.util.{ Failure, Try }
import scala.concurrent.{ ExecutionContext, Future }

import reactivemongo.core.errors.GenericDriverException
import reactivemongo.core.protocol.Response

import reactivemongo.api.Serialization.Pack

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
  def transaction: Try[SessionTransaction]

  @inline def operationTime: Option[Long] = Option.empty[Long]

  /** No-op as not tracking times, save for [[NodeSetSession]]. */
  private[reactivemongo] val update: Function3[Long, Option[Long], Option[Pack#Document], Session] = (_, _, _) => this // No-op

  /** Returns `Some` newly started transaction if any. */
  private[reactivemongo] val startTransaction: Function2[WriteConcern, Option[String], Try[(SessionTransaction, Boolean)]] = (_, _) => transaction.map(_ -> false)

  private[reactivemongo] def transactionToFlag(): Boolean = false

  /** Returns `Some` ended transaction if any. */
  private[reactivemongo] def endTransaction(): Option[SessionTransaction] = None

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

  lazy val transaction: Try[SessionTransaction] =
    Failure(new GenericDriverException(
      s"Cannot start transaction for session '$lsid': no replicaset"))

}

private[reactivemongo] sealed class NodeSetSession(
  lsid: UUID,
  causalConsistency: Boolean = true) extends Session(lsid, causalConsistency) {

  protected final val txState = new AtomicReference[SessionTransaction](
    SessionTransaction(
      txnNumber = 0L,
      writeConcern = Option.empty[WriteConcern], // not started
      pinnedNode = Option.empty[String], // not started
      recoveryToken = Option.empty[Pack#Document],
      flagSent = false))

  final protected val gossip = new AtomicReference(0L -> 0L)

  override private[reactivemongo] val update: Function3[Long, Option[Long], Option[Pack#Document], Session] = (operationTime, clusterTime, _) => {
    gossip.getAndAccumulate(
      operationTime -> clusterTime.getOrElse(0),
      Session.UpdateGossip)

    this
  }

  final override def operationTime = Option(gossip.get()).collect {
    case (opTime, _) if opTime > 0 => opTime
  }

  final def transaction: Try[SessionTransaction] =
    Try(txState.get()).filter(_.isStarted)

  override private[reactivemongo] val startTransaction: (WriteConcern, Option[String]) => Try[(SessionTransaction, Boolean)] = { (wc, _) =>
    val startOp = new Session.IncTxnNumberIfNotStarted(wc)

    Try(txState updateAndGet startOp).map(_ -> startOp.updated)
  }

  final override private[reactivemongo] def transactionToFlag(): Boolean = {
    val before = txState.getAndUpdate(Session.TransactionStartSent)

    before.flagSent // was not sent before, so need to send it now
  }

  final override private[reactivemongo] def endTransaction(): Option[SessionTransaction] =
    Option(txState getAndUpdate Session.EndTxIfStarted).filter(_.isStarted)
}

private[reactivemongo] final class DistributedSession(
  lsid: UUID,
  causalConsistency: Boolean = true) extends NodeSetSession(lsid, causalConsistency) {

  final override private[reactivemongo] val startTransaction: (WriteConcern, Option[String]) => Try[(SessionTransaction, Boolean)] = {
    case (wc, Some(txNode)) => {
      val startOp = new Session.IncTxnNumberAndPinNodeIfNotStarted(wc, txNode)

      Try(txState updateAndGet startOp).map(_ -> startOp.updated)
    }

    case _ =>
      Failure(new GenericDriverException(
        "Cannot start a distributed transaction without a pinned node"))
  }

  final override private[reactivemongo] val update: Function3[Long, Option[Long], Option[Pack#Document], Session] = (operationTime, clusterTime, recoveryToken) => {
    recoveryToken.foreach { token =>
      txState.updateAndGet(new Session.TransactionSetRecoveryToken(token))
    }

    gossip.getAndAccumulate(
      operationTime -> clusterTime.getOrElse(0),
      Session.UpdateGossip)

    this
  }
}

private[api] object Session {
  import java.util.function.{ BinaryOperator, UnaryOperator }

  private[api] val logger =
    reactivemongo.util.LazyLogger("reactivemongo.api.Session")

  private lazy val decoder = Serialization.internalSerializationPack.newDecoder

  def updateOnResponse(
    session: Session,
    response: Response)(implicit ec: ExecutionContext): Future[(Session, Response)] = Response.preload(response).map {
    case (resp, preloaded) =>
      val opTime = decoder.long(preloaded, "operationTime")

      opTime.fold(session -> resp) { operationTime =>
        logger.debug(s"Update session ${session.lsid} with response to #${response.header.responseTo} at $operationTime")

        val clusterTime = for {
          nested <- decoder.child(preloaded, f"$$clusterTime")
          time <- decoder.long(nested, "clusterTime")
        } yield time

        val recoveryToken = decoder.child(preloaded, "recoveryToken")

        session.update(operationTime, clusterTime, recoveryToken) -> resp
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

  /**
   * @param node the name of the node to be pinned on transaction
   */
  final class IncTxnNumberAndPinNodeIfNotStarted(
    wc: WriteConcern,
    node: String) extends UnaryOperator[SessionTransaction] {

    var updated: Boolean = false

    def apply(current: SessionTransaction): SessionTransaction =
      if (current.isStarted) {
        current // unchanged
      } else {
        updated = true

        current.copy(
          txnNumber = current.txnNumber + 1L,
          writeConcern = Some(wc), // started with given WriteConcern
          pinnedNode = Some(node),
          flagSent = false)
      }
  }

  object EndTxIfStarted extends UnaryOperator[SessionTransaction] {
    def apply(current: SessionTransaction): SessionTransaction =
      if (current.isStarted) {
        current.copy(
          writeConcern = None,
          pinnedNode = None,
          flagSent = false,
          recoveryToken = None)
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

  final class TransactionSetRecoveryToken(
    recoveryToken: Pack#Document) extends UnaryOperator[SessionTransaction] {

    def apply(current: SessionTransaction): SessionTransaction =
      if (current.isStarted) {
        current.copy(recoveryToken = Some(recoveryToken))
      } else {
        current
      }
  }
}

/**
 * @param txnNumber the current transaction number
 * @param writeConcern the write concern if the session transaction is started
 * @param pinnedNode the name of the [[https://github.com/mongodb/specifications/blob/master/source/transactions/transactions.rst#mongos-pinning node pinned to the transaction]]
 */
private[reactivemongo] case class SessionTransaction(
  txnNumber: Long,
  writeConcern: Option[WriteConcern],
  pinnedNode: Option[String],
  flagSent: Boolean,
  recoveryToken: Option[Pack#Document]) {

  @inline def isStarted: Boolean = writeConcern.isDefined
}
