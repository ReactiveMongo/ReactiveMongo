package reactivemongo.api

/**
 * The [[https://docs.mongodb.com/manual/reference/write-concern/index.html write concern]].
 *
 * {{{
 * import scala.concurrent.ExecutionContext
 * import reactivemongo.api.{ DB, WriteConcern }
 * import reactivemongo.api.bson.BSONDocument
 *
 * def foo(db: DB)(implicit ec: ExecutionContext) =
 *   db.collection("myColl").
 *     insert(ordered = false, WriteConcern.Acknowledged).
 *     one(BSONDocument("foo" -> "bar"))
 * }}}
 */
sealed trait WriteConcern {
  def w: WriteConcern.W
  def j: Boolean
  def fsync: Boolean
  def wtimeout: Option[Int]
}

/** [[WriteConcern]] utilities. */
object WriteConcern {
  import reactivemongo.api.commands.GetLastError

  /** [[https://docs.mongodb.com/manual/reference/write-concern/index.html#w-option Acknowledgment]] specification (w) */
  sealed trait W

  /** [[https://docs.mongodb.com/manual/reference/write-concern/index.html#writeconcern._dq_majority_dq_ Majority]] acknowledgment */
  sealed trait Majority extends W

  /** [[https://docs.mongodb.com/manual/reference/write-concern/index.html#writeconcern.%3Ccustom-write-concern-name%3E Tagged]] acknowledgment */
  sealed class TagSet(val tag: String) extends W

  /** Requests acknowledgment [[https://docs.mongodb.com/manual/reference/write-concern/index.html#writeconcern.%3Cnumber%3E by at least]] `i` nodes. */
  sealed class WaitForAcknowledgments(val i: Int) extends W

  /** [[WriteConcern]] with no acknowledgment required. */
  val Unacknowledged: GetLastError with WriteConcern =
    GetLastError(GetLastError.WaitForAcknowledgments(0), false, false, None)

  /** [[WriteConcern]] with one acknowledgment required. */
  val Acknowledged: GetLastError with WriteConcern =
    GetLastError(GetLastError.WaitForAcknowledgments(1), false, false, None)

  /**
   * [[WriteConcern]] with one acknowledgment and operation
   * written to the [[https://docs.mongodb.com/manual/reference/write-concern/index.html#j-option on-disk journal]].
   */
  val Journaled: GetLastError with WriteConcern =
    GetLastError(GetLastError.WaitForAcknowledgments(1), true, false, None)

  def ReplicaAcknowledged(n: Int, timeout: Int, journaled: Boolean): GetLastError with WriteConcern = GetLastError(GetLastError.WaitForAcknowledgments(if (n < 2) 2 else n), journaled, false, (if (timeout <= 0) None else Some(timeout)))

  def TagReplicaAcknowledged(tag: String, timeout: Int, journaled: Boolean): GetLastError with WriteConcern = GetLastError(GetLastError.TagSet(tag), journaled, false, (if (timeout <= 0) None else Some(timeout)))

  /** The default [[WriteConcern]] */
  def Default: GetLastError with WriteConcern = Acknowledged
}

package commands {
  import reactivemongo.api.{ WriteConcern => WC }

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

    class TagSet private[api] (override val tag: String)
      extends WC.TagSet(tag) with W
      with Product1[String] with Serializable {

      @deprecated("No longer a case class", "0.20.3")
      @inline def _1 = tag

      @deprecated("No longer a case class", "0.20.3")
      def canEqual(that: Any): Boolean = that match {
        case _: TagSet => true
        case _         => false
      }

      override def equals(that: Any): Boolean = that match {
        case other: TagSet =>
          this.tag == other.tag

        case _ =>
          false
      }

      override def hashCode: Int = tag.hashCode

      override def toString = s"TagSet($tag)"
    }

    object TagSet extends scala.runtime.AbstractFunction1[String, TagSet] {
      def apply(tag: String): TagSet = new TagSet(tag)

      @deprecated("No longer a case class", "0.20.3")
      def unapply(set: TagSet): Option[String] = Option(set).map(_.tag)
    }

    @deprecated(message = "Use `WaitForAcknowledgments`", since = "0.12.4")
    case class WaitForAknowledgments(override val i: Int)
      extends WC.WaitForAcknowledgments(i) with W

    class WaitForAcknowledgments private[api] (override val i: Int)
      extends WC.WaitForAcknowledgments(i) with W
      with Product1[Int] with Serializable {

      @deprecated("No longer a case class", "0.20.3")
      @inline def _1 = i

      @deprecated("No longer a case class", "0.20.3")
      def canEqual(that: Any): Boolean = that match {
        case _: WaitForAcknowledgments => true
        case _                         => false
      }

      override def equals(that: Any): Boolean = that match {
        case other: WaitForAcknowledgments =>
          this.i == other.i

        case _ =>
          false
      }

      override def hashCode: Int = i

      override def toString = s"WaitForAcknowledgments($i)"
    }

    object WaitForAcknowledgments extends scala.runtime.AbstractFunction1[Int, WaitForAcknowledgments] {
      def apply(i: Int): WaitForAcknowledgments = new WaitForAcknowledgments(i)

      @deprecated("No longer a case class", "0.20.3")
      def unapply(set: WaitForAcknowledgments): Option[Int] = Option(set).map(_.i)
    }

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
