package reactivemongo.core.actors

import reactivemongo.core.protocol.Response

/**
 * @param lower the lower bound
 * @param upper the upper bound
 */
private[actors] class RequestIdGenerator(val lower: Int, val upper: Int)
  extends Product with Serializable {

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

  @deprecated("", "0.12-RC6")
  def canEqual(that: Any): Boolean = that.isInstanceOf[RequestIdGenerator]

  @deprecated("", "0.12-RC6")
  val productArity = 2

  @deprecated("", "0.12-RC6")
  def productElement(n: Int): Any = n match {
    case 0 => lower
    case _ => upper
  }
}

@deprecated("Use the class RequestIdGenerator", "0.12-RC6")
object RequestIdGenerator
  extends scala.runtime.AbstractFunction2[Int, Int, RequestIdGenerator] {

  def apply(lower: Int, upper: Int): RequestIdGenerator =
    new RequestIdGenerator(lower, upper)

  def unapply(g: RequestIdGenerator): Option[(Int, Int)] =
    Some(g.lower -> g.upper)
}
