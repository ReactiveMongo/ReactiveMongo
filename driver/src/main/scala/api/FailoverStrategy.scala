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
 *
 * @param initialDelay the initial delay between the first failed attempt and the next one.
 * @param retries the number of retries to do before giving up.
 * @param delayFactor a function that takes the current iteration and returns a factor to be applied to the initialDelay (default: [[FailoverStrategy.defaultFactor]]; see [[FailoverStrategy.FactorFun]])
 */
class FailoverStrategy private[api] (
  val initialDelay: FiniteDuration,
  val retries: Int,
  val delayFactor: Int => Double) extends Product3[FiniteDuration, Int, Int => Double] with Serializable {

  @deprecated("No longer a case class", "1.0.0-rc.1")
  @inline def _1 = initialDelay

  @deprecated("No longer a case class", "1.0.0-rc.1")
  @inline def _2 = retries

  @deprecated("No longer a case class", "1.0.0-rc.1")
  @inline def _3 = delayFactor

  @deprecated("No longer a case class", "1.0.0-rc.1")
  def canEqual(that: Any): Boolean = false

  @deprecated("No longer a case class", "1.0.0-rc.1")
  def copy(
    initialDelay: FiniteDuration = this.initialDelay,
    retries: Int = this.retries,
    delayFactor: Int => Double = delayFactor): FailoverStrategy =
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
  case class FactorFun(multiplier: Double) extends (Int => Double) {
    /**
     * Returns the factor by which the initial delay must be multiplied,
     * for the current try.
     *
     * @param `try` the current number of tries
     */
    final def apply(`try`: Int): Double = `try` * multiplier

    override lazy val toString = s"× $multiplier"
  }

  /** The factor function for the default strategy: × 1.25 */
  @inline def defaultFactor = FactorFun(1.25)
  // 'def' instead of 'val' as used in defaults of the case class

  def apply(
    initialDelay: FiniteDuration = FiniteDuration(100, "ms"),
    retries: Int = 10,
    delayFactor: Int => Double = defaultFactor): FailoverStrategy =
    new FailoverStrategy(initialDelay, retries, delayFactor)

  @deprecated("No longer a case class", "1.0.0-rc.1")
  def unapply(strategy: FailoverStrategy): Option[Tuple3[FiniteDuration, Int, Int => Double]] = Option(strategy).map { s => Tuple3(s.initialDelay, s.retries, s.delayFactor) }

}
