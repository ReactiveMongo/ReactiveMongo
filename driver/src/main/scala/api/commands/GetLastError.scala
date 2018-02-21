package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

/**
 * @param wtimeout the [[http://docs.mongodb.org/manual/reference/write-concern/#wtimeout time limit]]
 */
case class GetLastError(
  w: GetLastError.W,
  j: Boolean,
  fsync: Boolean,
  wtimeout: Option[Int] = None) extends Command
  with CommandWithResult[LastError]

// TODO: Rename as WriteConcern
object GetLastError {
  sealed trait W
  case object Majority extends W
  case class TagSet(tag: String) extends W
  @deprecated(message = "Use `WaitForAcknowledgments`", since = "0.12.4")
  case class WaitForAknowledgments(i: Int) extends W
  case class WaitForAcknowledgments(i: Int) extends W

  object W {
    @deprecated(message = "Use `W(s)`", since = "0.12.7")
    def strToTagSet(s: String): W = apply(s)

    /** Factory */
    def apply(s: String): W = TagSet(s)

    @deprecated(message = "Use `intToWaitForAcknowledgments`", since = "0.12.4")
    def intToWaitForAknowledgments(i: Int): W =
      WaitForAknowledgments(i)

    @deprecated(message = "Use `W(i)`", since = "0.12.7")
    def intToWaitForAcknowledgments(i: Int): W = apply(i)

    /** Factory */
    def apply(i: Int): W = WaitForAcknowledgments(i)
  }

  val Unacknowledged: GetLastError =
    GetLastError(WaitForAcknowledgments(0), false, false, None)

  val Acknowledged: GetLastError =
    GetLastError(WaitForAcknowledgments(1), false, false, None)

  val Journaled: GetLastError =
    GetLastError(WaitForAcknowledgments(1), true, false, None)

  def ReplicaAcknowledged(n: Int, timeout: Int, journaled: Boolean): GetLastError = GetLastError(WaitForAcknowledgments(if (n < 2) 2 else n), journaled, false, (if (timeout <= 0) None else Some(timeout)))

  def TagReplicaAcknowledged(tag: String, timeout: Int, journaled: Boolean): GetLastError = GetLastError(TagSet(tag), journaled, false, (if (timeout <= 0) None else Some(timeout)))

  def Default: GetLastError = Acknowledged

  private[api] def serializeWith[P <: SerializationPack](
    pack: P, writeConcern: WriteConcern)(
    builder: SerializationPack.Builder[pack.type]): pack.Document = {
    import builder.{ elementProducer => element, int, string }

    val elements = Seq.newBuilder[pack.ElementProducer]

    writeConcern.w match {
      case GetLastError.Majority =>
        elements += element("w", string("majority"))

      case GetLastError.TagSet(tagSet) =>
        elements += element("w", string(tagSet))

      case GetLastError.WaitForAcknowledgments(n) =>
        elements += element("w", int(n))

      case GetLastError.WaitForAknowledgments(n) =>
        elements += element("w", int(n))

    }

    element("j", builder.boolean(true))

    writeConcern.wtimeout.foreach { t =>
      elements += element("wtimeout", int(t))
    }

    builder.document(elements.result())
  }
}
