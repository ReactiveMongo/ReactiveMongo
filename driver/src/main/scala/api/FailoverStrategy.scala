package reactivemongo.api

import scala.concurrent.duration.FiniteDuration

/**
 * A failover strategy for sending requests.
 * The default uses 10 retries:
 * 125ms, 250ms, 375ms, 500ms, 625ms, 750ms, 875ms, 1s, 1125ms, 1250ms
 *
 * {{{
 * import scala.concurrent.duration._
 *
 * reactivemongo.api.FailoverStrategy(
 *   initialDelay = 150.milliseconds,
 *   retries = 20,
 *   delayFactor = { `try` => `try` * 1.5D })
 * }}}
 */
final class FailoverStrategy private[api] (
    _initialDelay: FiniteDuration,
    _retries: Int,
    _delayFactor: Int => Double) {

  /** The initial delay between the first failed attempt and the next one */
  @inline def initialDelay: FiniteDuration = _initialDelay

  /** The number of retries to do before giving up */
  @inline def retries: Int = _retries

  /**
   * The function that takes the current iteration,
   * and returns a factor to be applied to the initialDelay
   * (default: [[FailoverStrategy.defaultFactor]];
   * See [[FailoverStrategy.FactorFun]])
   */
  @inline def delayFactor: Int => Double = _delayFactor

  // ---

  @SuppressWarnings(Array("VariableShadowing"))
  def copy(
      initialDelay: FiniteDuration = _initialDelay,
      retries: Int = _retries,
      delayFactor: Int => Double = _delayFactor
    ): FailoverStrategy =
    new FailoverStrategy(initialDelay, retries, delayFactor)

  override lazy val toString = delayFactor match {
    case fn @ FailoverStrategy.FactorFun(_) =>
      s"FailoverStrategy($initialDelay, $retries, $fn)"

    case _ => s"FailoverStrategy($initialDelay, $retries)"
  }
}

/** [[FailoverStrategy]] utilities */
object FailoverStrategy {

  /** The default strategy */
  val default = FailoverStrategy()

  /** The strategy when the MongoDB nodes are remote (with 16 retries) */
  val remote = FailoverStrategy(retries = 16)

  /** A more strict strategy: same as default but with less retries (=5). */
  val strict = FailoverStrategy(retries = 5)

  /** A factor function using simple multiplication. */
  final class FactorFun private[api] (val multiplier: Double)
      extends (Int => Double) {

    /**
     * Returns the factor by which the initial delay must be multiplied,
     * for the current try.
     *
     * @param `try` the current number of tries
     */
    def apply(`try`: Int): Double = `try` * multiplier

    override lazy val toString = s"× $multiplier"

    @SuppressWarnings(Array("ComparingFloatingPointTypes"))
    override def equals(that: Any): Boolean = that match {
      case other: FactorFun =>
        this.multiplier == other.multiplier

      case _ =>
        false
    }

    override def hashCode: Int = multiplier.toInt
  }

  object FactorFun {
    def apply(multiplier: Double): FactorFun = new FactorFun(multiplier)

    def unapply(fun: FactorFun): Option[Double] = Some(fun.multiplier)
  }

  /** The factor function for the default strategy: × 1.25 */
  @inline def defaultFactor = FactorFun(1.25)
  // 'def' instead of 'val' as used in defaults of the case class

  def apply(
      initialDelay: FiniteDuration = FiniteDuration(100, "ms"),
      retries: Int = 10,
      delayFactor: Int => Double = defaultFactor
    ): FailoverStrategy =
    new FailoverStrategy(initialDelay, retries, delayFactor)
}
