package reactivemongo.api

trait WriteConcern {
  def w: WriteConcern.W
  def j: Boolean
  def fsync: Boolean
  def wtimeout: Option[Int]
}

object WriteConcern {
  import reactivemongo.api.commands.GetLastError

  sealed trait W

  sealed trait Majority extends W
  sealed class TagSet(val tag: String) extends W
  sealed class WaitForAcknowledgments(val i: Int) extends W

  val Unacknowledged: GetLastError with WriteConcern =
    GetLastError(GetLastError.WaitForAcknowledgments(0), false, false, None)

  val Acknowledged: GetLastError with WriteConcern =
    GetLastError(GetLastError.WaitForAcknowledgments(1), false, false, None)

  val Journaled: GetLastError with WriteConcern =
    GetLastError(GetLastError.WaitForAcknowledgments(1), true, false, None)

  def ReplicaAcknowledged(n: Int, timeout: Int, journaled: Boolean): GetLastError with WriteConcern = GetLastError(GetLastError.WaitForAcknowledgments(if (n < 2) 2 else n), journaled, false, (if (timeout <= 0) None else Some(timeout)))

  def TagReplicaAcknowledged(tag: String, timeout: Int, journaled: Boolean): GetLastError with WriteConcern = GetLastError(GetLastError.TagSet(tag), journaled, false, (if (timeout <= 0) None else Some(timeout)))

  def Default: GetLastError with WriteConcern = Acknowledged
}

package object commands {
  import reactivemongo.api.{ WriteConcern => WC }

  @deprecated("Will be replaced by `reactivemongo.api.commands.WriteConcern`", "0.16.0")
  type WriteConcern = GetLastError

  @deprecated("Will be replaced by `reactivemongo.api.commands.WriteConcern`", "0.16.0")
  val WriteConcern = GetLastError

  /**
   * @param wtimeout the [[http://docs.mongodb.org/manual/reference/write-concern/#wtimeout time limit]]
   */
  @deprecated("Will be replaced by `reactivemongo.api.commands.WriteConcern`", "0.16.0")
  case class GetLastError(
    w: GetLastError.W,
    j: Boolean,
    fsync: Boolean,
    wtimeout: Option[Int] = None) extends Command with WC
    with CommandWithResult[LastError]

  @deprecated("Will be replaced by `reactivemongo.api.commands.WriteConcern`", "0.16.0")
  object GetLastError {
    sealed trait W extends WC.W
    case object Majority extends WC.Majority with W
    case class TagSet(override val tag: String) extends WC.TagSet(tag) with W

    @deprecated(message = "Use `WaitForAcknowledgments`", since = "0.12.4")
    case class WaitForAknowledgments(override val i: Int)
      extends WC.WaitForAcknowledgments(i) with W

    case class WaitForAcknowledgments(override val i: Int)
      extends WC.WaitForAcknowledgments(i) with W

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
  }
}
