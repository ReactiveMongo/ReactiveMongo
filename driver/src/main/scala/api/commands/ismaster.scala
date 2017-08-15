package reactivemongo.api.commands

import reactivemongo.api.SerializationPack
import reactivemongo.core.nodeset.NodeStatus

trait IsMasterCommand[P <: SerializationPack] {
  object IsMaster extends Command with CommandWithResult[IsMasterResult] with CommandWithPack[P]

  /**
   * @param setVersion the set version, or -1 if unknown
   * @param electionId the unique identifier for each election, or -1
   */
  sealed class ReplicaSet(
    val setName: String,
    val setVersion: Int,
    val me: String,
    val primary: Option[String],
    val hosts: Seq[String],
    val passives: Seq[String],
    val arbiters: Seq[String],
    val isSecondary: Boolean, // `secondary`
    val isArbiterOnly: Boolean, // `arbiterOnly`
    val isPassive: Boolean, // `passive`
    val isHidden: Boolean, // `hidden`
    val tags: Option[P#Document],
    val electionId: Int) extends Product with Equals with java.io.Serializable with Serializable {

    // setVersion
    override lazy val toString = s"""ReplicaSet($setName, primary = $primary, me = $me, hosts = ${hosts.mkString("[", ",", "]")})"""

    private def tupled = (setName, setVersion, me, primary, hosts, passives, arbiters, isSecondary, isArbiterOnly, isPassive, isHidden, tags, electionId)

    override lazy val hashCode = tupled.hashCode

    override def equals(that: Any): Boolean = that match {
      case rs: ReplicaSet => tupled == rs.tupled
      case _              => false
    }

    // Deprecated
    def canEqual(that: Any): Boolean = that match {
      case _: ReplicaSet => true
      case _             => false
    }

    val productArity = 11

    def productElement(n: Int): Any = (n: @annotation.switch) match {
      case 0  => setName
      case 1  => me
      case 2  => primary
      case 3  => hosts
      case 4  => passives
      case 5  => arbiters
      case 6  => isSecondary
      case 7  => isArbiterOnly
      case 8  => isPassive
      case 9  => isHidden
      case 10 => tags
      case _  => throw new NoSuchElementException()
    }

    override val productPrefix = "ReplicaSet"

    def this(
      setName: String,
      me: String,
      primary: Option[String],
      hosts: Seq[String],
      passives: Seq[String],
      arbiters: Seq[String],
      isSecondary: Boolean, // `secondary`
      isArbiterOnly: Boolean, // `arbiterOnly`
      isPassive: Boolean, // `passive`
      isHidden: Boolean, // `hidden`
      tags: Option[P#Document]) = this(setName, -1, me, primary, hosts, passives,
      arbiters, isSecondary, isArbiterOnly, isPassive, isHidden, tags, -1)

    def copy(
      setName: String = this.setName,
      me: String = this.me,
      primary: Option[String] = this.primary,
      hosts: Seq[String] = this.hosts,
      passives: Seq[String] = this.passives,
      arbiters: Seq[String] = this.arbiters,
      isSecondary: Boolean = this.isSecondary,
      isArbiterOnly: Boolean = this.isArbiterOnly,
      isPassive: Boolean = this.isPassive,
      isHidden: Boolean = this.isHidden,
      tags: Option[P#Document] = this.tags): ReplicaSet = new ReplicaSet(setName, -1, me, primary, hosts, passives,
      arbiters, isSecondary, isArbiterOnly, isPassive, isHidden, tags, -1)
  }

  object ReplicaSet extends scala.runtime.AbstractFunction11[String, String, Option[String], Seq[String], Seq[String], Seq[String], Boolean, Boolean, Boolean, Boolean, Option[P#Document], ReplicaSet] {

    @deprecated(
      message = "Use constructor with `setVersion` and `electionId`",
      since = "12-RC1")
    def apply(
      setName: String,
      me: String,
      primary: Option[String],
      hosts: Seq[String],
      passives: Seq[String],
      arbiters: Seq[String],
      isSecondary: Boolean, // `secondary`
      isArbiterOnly: Boolean, // `arbiterOnly`
      isPassive: Boolean, // `passive`
      isHidden: Boolean, // `hidden`
      tags: Option[P#Document]): ReplicaSet = new ReplicaSet(setName, -1, me, primary, hosts, passives,
      arbiters, isSecondary, isArbiterOnly, isPassive, isHidden, tags, -1)

    def unapply(rs: ReplicaSet): Option[(String, String, Option[String], Seq[String], Seq[String], Seq[String], Boolean, Boolean, Boolean, Boolean, Option[P#Document])] = Some((rs.setName, rs.me, rs.primary, rs.hosts, rs.passives, rs.arbiters, rs.isSecondary, rs.isArbiterOnly, rs.isPassive, rs.isHidden, rs.tags))
  }

  case class IsMasterResult(
    isMaster: Boolean, // `ismaster`
    maxBsonObjectSize: Int, // default = 16 * 1024 * 1024
    maxMessageSizeBytes: Int, // default = 48000000, mongod >= 2.4
    maxWriteBatchSize: Int, // default = 1000, mongod >= 2.6
    localTime: Option[Long], // date? mongod >= 2.2
    minWireVersion: Int, // int? mongod >= 2.6
    maxWireVersion: Int, // int? mongod >= 2.6
    replicaSet: Option[ReplicaSet], // flattened in the result
    msg: Option[String] // Contains the value isdbgrid when isMaster returns from a mongos instance.
  ) {
    def isMongos: Boolean = msg.isDefined
    def status: NodeStatus = if (isMaster) NodeStatus.Primary else if (replicaSet.exists(_.isSecondary)) NodeStatus.Secondary else NodeStatus.NonQueryableUnknownStatus
  }
}
