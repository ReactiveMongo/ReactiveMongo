package reactivemongo.core.actors

import reactivemongo.core.protocol.Response

/**
 * @param lower the lower bound
 * @param upper the upper bound
 */
private[actors] class RequestIdGenerator(val lower: Int, val upper: Int) {
  private val lock = new Object {}
  private var value: Int = lower

  def next: Int = lock.synchronized {
    val v = value

    if (v == upper) value = lower
    else value = v + 1

    v
  }

  def accepts(id: Int): Boolean = id >= lower && id <= upper
  def accepts(response: Response): Boolean = accepts(response.header.responseTo)

  override def equals(that: Any): Boolean = that match {
    case RequestIdGenerator(l, u) => (lower == l) && (upper == u)
    case _                        => false
  }

  override lazy val hashCode: Int = (lower, upper).hashCode
}

private[actors] object RequestIdGenerator {
  def unapply(g: RequestIdGenerator): Option[(Int, Int)] =
    Some(g.lower -> g.upper)
}
