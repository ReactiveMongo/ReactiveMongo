package reactivemongo.api

import scala.concurrent.duration.FiniteDuration

/**
 * A failover strategy for sending requests.
 * The default uses 10 retries:
 * 125ms, 250ms, 375ms, 500ms, 625ms, 750ms, 875ms, 1s, 1125ms, 1250ms
 *
 * @param initialDelay the initial delay between the first failed attempt and the next one.
 * @param retries the number of retries to do before giving up.
 * @param delayFactor a function that takes the current iteration and returns a factor to be applied to the initialDelay (default: [[FailoverStrategy.defaultFactor]])
 */
case class FailoverStrategy(
  initialDelay: FiniteDuration = FiniteDuration(100, "ms"),
  retries: Int = 10,
  delayFactor: Int => Double = FailoverStrategy.defaultFactor) {

  override lazy val toString = delayFactor match {
    case fn @ FailoverStrategy.FactorFun(_) =>
      s"FailoverStrategy($initialDelay, $retries, $fn)"

    case _ => s"FailoverStrategy($initialDelay, $retries)"
  }
}

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
}
