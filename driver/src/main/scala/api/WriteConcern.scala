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
final class WriteConcern private[api] (
  _w: WriteConcern.W,
  _j: Boolean,
  _fsync: Boolean,
  _wtimeout: Option[Int]) {

  @inline def w: WriteConcern.W = _w

  /** The journal flag */
  @inline def j: Boolean = _j

  @inline def fsync: Boolean = _fsync

  /**
   * The time limit, in milliseconds
   * (only applicable for `w` values greater than 1)
   */
  @inline def wtimeout: Option[Int] = _wtimeout

  @SuppressWarnings(Array("VariableShadowing"))
  def copy(
    w: WriteConcern.W = _w,
    j: Boolean = _j,
    fsync: Boolean = _fsync,
    wtimeout: Option[Int] = _wtimeout): WriteConcern =
    new WriteConcern(w, j, fsync, wtimeout)

  override def equals(that: Any): Boolean = that match {
    case other: WriteConcern => tupled == other.tupled
    case _                   => false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString = s"WriteConcern${tupled.toString}"

  private lazy val tupled = Tuple4(w, j, fsync, wtimeout)
}

/** [[WriteConcern]] utilities. */
object WriteConcern {
  def apply(
    w: WriteConcern.W,
    j: Boolean,
    fsync: Boolean,
    wtimeout: Option[Int]): WriteConcern =
    new WriteConcern(w, j, fsync, wtimeout)

  // ---

  /** [[https://docs.mongodb.com/manual/reference/write-concern/index.html#w-option Acknowledgment]] specification (w) */
  sealed trait W

  /** [[https://docs.mongodb.com/manual/reference/write-concern/index.html#writeconcern._dq_majority_dq_ Majority]] acknowledgment */
  object Majority extends W {
    override def toString = "Majority"
  }

  /** [[https://docs.mongodb.com/manual/reference/write-concern/index.html#writeconcern.%3Ccustom-write-concern-name%3E Tagged]] acknowledgment */
  final class TagSet private[api] (val tag: String) extends W {
    override def equals(that: Any): Boolean = that match {
      case other: TagSet =>
        (tag == null && other.tag == null) || (tag != null && tag == other.tag)

      case _ =>
        false
    }

    override def hashCode: Int = tag.hashCode

    override def toString = s"TagSet($tag)"
  }

  object TagSet {
    @inline def apply(tag: String): TagSet = new TagSet(tag)

    private[api] def unapply(that: W): Option[String] = that match {
      case other: TagSet => Option(other.tag)
      case _             => None
    }
  }

  /** Requests acknowledgment [[https://docs.mongodb.com/manual/reference/write-concern/index.html#writeconcern.%3Cnumber%3E by at least]] `i` nodes. */
  final class WaitForAcknowledgments private[api] (val i: Int) extends W {
    override def equals(that: Any): Boolean = that match {
      case other: WaitForAcknowledgments => i == other.i
      case _                             => false
    }

    override def hashCode: Int = i

    override def toString = s"WaitForAcknowledgments($i)"
  }

  object WaitForAcknowledgments {
    @inline def apply(i: Int): WaitForAcknowledgments =
      new WaitForAcknowledgments(i)

    private[api] def unapply(that: W): Option[Int] = that match {
      case other: WaitForAcknowledgments => Some(other.i)
      case _                             => None
    }
  }

  /** [[WriteConcern]] with no acknowledgment required. */
  val Unacknowledged: WriteConcern =
    WriteConcern(new WaitForAcknowledgments(0), false, false, None)

  /** [[WriteConcern]] with one acknowledgment required. */
  val Acknowledged: WriteConcern =
    WriteConcern(new WaitForAcknowledgments(1), false, false, None)

  /**
   * [[WriteConcern]] with one acknowledgment and operation
   * written to the [[https://docs.mongodb.com/manual/reference/write-concern/index.html#j-option on-disk journal]].
   */
  val Journaled: WriteConcern =
    WriteConcern(new WaitForAcknowledgments(1), true, false, None)

  @SuppressWarnings(Array("MethodNames"))
  def ReplicaAcknowledged(n: Int, timeout: Int, journaled: Boolean): WriteConcern = WriteConcern(new WaitForAcknowledgments(if (n < 2) 2 else n), journaled, false, (if (timeout <= 0) None else Some(timeout)))

  @SuppressWarnings(Array("MethodNames"))
  def TagReplicaAcknowledged(tag: String, timeout: Int, journaled: Boolean): WriteConcern = WriteConcern(new TagSet(tag), journaled, false, (if (timeout <= 0) None else Some(timeout)))

  /** The default [[WriteConcern]] */
  @SuppressWarnings(Array("MethodNames"))
  def Default: WriteConcern = Acknowledged
}
