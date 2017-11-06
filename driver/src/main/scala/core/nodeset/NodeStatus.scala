package reactivemongo.core.nodeset

sealed trait NodeStatus { def queryable = false }

sealed trait QueryableNodeStatus { self: NodeStatus =>
  override def queryable = true
}

sealed trait CanonicalNodeStatus { self: NodeStatus => }

object NodeStatus {
  object Uninitialized extends NodeStatus {
    override def toString = "Uninitialized"
  }

  object NonQueryableUnknownStatus extends NodeStatus {
    override def toString = "NonQueryableUnknownStatus"
  }

  /** Cannot vote. All members start up in this state. The mongod parses the replica set configuration document while in STARTUP. */
  object Startup extends NodeStatus with CanonicalNodeStatus { override def toString = "Startup" }

  /** Can vote. The primary is the only member to accept write operations. */
  object Primary extends NodeStatus with QueryableNodeStatus with CanonicalNodeStatus { override def toString = "Primary" }

  /** Can vote. The secondary replicates the data store. */
  object Secondary extends NodeStatus with QueryableNodeStatus with CanonicalNodeStatus { override def toString = "Secondary" }

  /** Can vote. Members either perform startup self-checks, or transition from completing a rollback or resync. */
  object Recovering extends NodeStatus with CanonicalNodeStatus { override def toString = "Recovering" }

  /** Cannot vote. Has encountered an unrecoverable error. */
  object Fatal extends NodeStatus with CanonicalNodeStatus { override def toString = "Fatal" }

  /** Cannot vote. Forks replication and election threads before becoming a secondary. */
  object Startup2 extends NodeStatus with CanonicalNodeStatus { override def toString = "Startup2" }

  /** Cannot vote. Has never connected to the replica set. */
  object Unknown extends NodeStatus with CanonicalNodeStatus { override def toString = "Unknown" }

  /** Can vote. Arbiters do not replicate data and exist solely to participate in elections. */
  object Arbiter extends NodeStatus with CanonicalNodeStatus { override def toString = "Arbiter" }

  /** Cannot vote. Is not accessible to the set. */
  object Down extends NodeStatus with CanonicalNodeStatus { override def toString = "Down" }

  /** Can vote. Performs a rollback. */
  object Rollback extends NodeStatus with CanonicalNodeStatus { override def toString = "Rollback" }

  /** Shunned. */
  object Shunned extends NodeStatus with CanonicalNodeStatus { override def toString = "Shunned" }

  def apply(code: Int): NodeStatus = code match {
    case 0  => Startup
    case 1  => Primary
    case 2  => Secondary
    case 3  => Recovering
    case 4  => Fatal
    case 5  => Startup2
    case 6  => Unknown
    case 7  => Arbiter
    case 8  => Down
    case 9  => Rollback
    case 10 => Shunned
    case _  => NonQueryableUnknownStatus
  }
}
