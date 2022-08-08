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

private[reactivemongo] object RequestIdGenerator {

  def unapply(g: RequestIdGenerator): Option[(Int, Int)] =
    Some(g.lower -> g.upper)

  // all requestIds [0, 1000[ are for isMaster messages
  val isMaster = new RequestIdGenerator(0, 999)

  // all requestIds [1000, 2000[ are for getnonce messages
  val getNonce = new RequestIdGenerator(1000, 1999) // CR auth

  // all requestIds [2000, 3000[ are for authenticate messages
  val authenticate = new RequestIdGenerator(2000, 2999)

  // all requestIds [3000[ are for common messages
  val common = new RequestIdGenerator(3000, Int.MaxValue - 1)
}
