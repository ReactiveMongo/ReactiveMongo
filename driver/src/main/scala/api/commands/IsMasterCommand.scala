package reactivemongo.api.commands

import java.util.Date

import reactivemongo.api.SerializationPack

import reactivemongo.core.ClientMetadata
import reactivemongo.core.nodeset.NodeStatus

@deprecated("Internal: will be made private", "0.16.0")
trait IsMasterCommand[P <: SerializationPack] {
  /**
   * @param client the client metadata (only for first isMaster request)
   */
  class IsMaster(
    val client: Option[ClientMetadata],
    val comment: Option[String]) extends Command
    with CommandWithResult[IsMasterResult] with CommandWithPack[P] {

    @deprecated("Use new constructor", "0.18.2")
    def this(comment: Option[String]) = this(
      client = None,
      comment = comment)
  }

  object IsMaster extends IsMaster(None) {
    @deprecated("Use factory with client metadata", "0.18.2")
    def apply(comment: String): IsMaster =
      new IsMaster(None, Some(comment))

    def apply(client: Option[ClientMetadata], comment: String): IsMaster =
      new IsMaster(client, Some(comment))

  }

  final class LastWrite(
    val opTime: Long,
    val lastWriteDate: Date,
    val majorityOpTime: Long,
    val majorityWriteDate: Date) {

    override def equals(that: Any): Boolean = that match {
      case other: this.type => other.tupled == tupled
      case _                => false
    }

    override def hashCode: Int = tupled.hashCode

    override def toString = s"LastWrite${tupled.toString}"

    private lazy val tupled = Tuple4(
      opTime, lastWriteDate, majorityOpTime, majorityWriteDate)
  }

  /**
   * @param setVersion the set version, or -1 if unknown
   * @param electionId the unique identifier for each election, or -1
   */
  sealed class ReplicaSet private[commands] (
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
    val tags: Map[String, String],
    val electionId: Int,
    val lastWrite: Option[LastWrite]) extends Product with Serializable {

    @deprecated("Use the constructor with tag map", "0.19.1")
    def this(
      setName: String,
      setVersion: Int,
      me: String,
      primary: Option[String],
      hosts: Seq[String],
      passives: Seq[String],
      arbiters: Seq[String],
      isSecondary: Boolean,
      isArbiterOnly: Boolean,
      isPassive: Boolean,
      isHidden: Boolean,
      tags: Option[P#Document],
      electionId: Int,
      lastWrite: Option[LastWrite]) =
      this(setName, setVersion, me, primary, hosts, passives, arbiters,
        isSecondary, isArbiterOnly, isPassive, isHidden,
        Map.empty[String, String],
        electionId, lastWrite)

    @deprecated("Use the constructor with lastWrite", "0.18.5")
    def this(
      setName: String,
      setVersion: Int,
      me: String,
      primary: Option[String],
      hosts: Seq[String],
      passives: Seq[String],
      arbiters: Seq[String],
      isSecondary: Boolean, // `secondary`
      isArbiterOnly: Boolean, // `arbiterOnly`
      isPassive: Boolean, // `passive`
      isHidden: Boolean, // `hidden`
      tags: Option[P#Document],
      electionId: Int) = this(setName, setVersion, me,
      primary, hosts, passives, arbiters, isSecondary, isArbiterOnly,
      isPassive, isHidden, tags, electionId, None)

    @deprecated("No longer a case class", "0.19.1")
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
      tags: Option[P#Document] = None): ReplicaSet = new ReplicaSet(
      setName, -1, me, primary, hosts, passives, arbiters, isSecondary,
      isArbiterOnly, isPassive, isHidden, Map.empty[String, String], -1, None)

    @deprecated("No longer a case class", "0.19.1")
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
      tags: Option[P#Document]) = this(setName, -1, me, primary, hosts,
      passives, arbiters, isSecondary, isArbiterOnly, isPassive,
      isHidden, tags, -1, None)

    // setVersion
    override lazy val toString = s"""ReplicaSet($setName, primary = $primary, me = $me, hosts = ${hosts.mkString("[", ",", "]")}, lastWrite = $lastWrite)"""

    private def tupled = (setName, setVersion, me, primary, hosts, passives, arbiters, isSecondary, isArbiterOnly, isPassive, isHidden, tags, electionId, lastWrite)

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
      case 11 => lastWrite
      case _  => throw new NoSuchElementException()
    }

    override val productPrefix = "ReplicaSet"
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
      tags: Option[P#Document]): ReplicaSet = new ReplicaSet(
      setName,
      -1, me, primary, hosts, passives, arbiters, isSecondary,
      isArbiterOnly, isPassive, isHidden, Map.empty[String, String], -1, None)

    @deprecated("No longer a case class", "0.19.1")
    def unapply(rs: ReplicaSet): Option[(String, String, Option[String], Seq[String], Seq[String], Seq[String], Boolean, Boolean, Boolean, Boolean, Option[P#Document])] = Some((rs.setName, rs.me, rs.primary, rs.hosts, rs.passives, rs.arbiters, rs.isSecondary, rs.isArbiterOnly, rs.isPassive, rs.isHidden, None))
  }

  class IsMasterResult private[commands] (
    val isMaster: Boolean, // `ismaster`
    val maxBsonObjectSize: Int, // default = 16 * 1024 * 1024
    val maxMessageSizeBytes: Int, // default = 48000000, mongod >= 2.4
    val maxWriteBatchSize: Int, // default = 1000, mongod >= 2.6
    val localTime: Option[Long], // date? mongod >= 2.2
    val logicalSessionTimeoutMinutes: Option[Long],
    val minWireVersion: Int, // int? mongod >= 2.6
    val maxWireVersion: Int, // int? mongod >= 2.6
    val readOnly: Option[Boolean],
    val compression: List[String],
    val saslSupportedMech: List[String], // GSSAPI, SCRAM-SHA-256, SCRAM-SHA-1
    val replicaSet: Option[ReplicaSet], // flattened in the result
    val msg: Option[String] // Contains the value isdbgrid when isMaster returns from a mongos instance.
  ) extends Product with Serializable {

    @deprecated("Use the complete constructor", "0.18.5")
    def this(
      isMaster: Boolean,
      maxBsonObjectSize: Int,
      maxMessageSizeBytes: Int,
      maxWriteBatchSize: Int,
      localTime: Option[Long],
      minWireVersion: Int,
      maxWireVersion: Int,
      replicaSet: Option[ReplicaSet],
      msg: Option[String]) = this(
      isMaster, maxBsonObjectSize, maxMessageSizeBytes, maxWriteBatchSize, localTime, None, minWireVersion, maxWireVersion, None, List.empty, List.empty, replicaSet, msg)

    def isMongos: Boolean = msg.exists(_ == "isdbgrid")

    def status: NodeStatus = {
      if (isMaster) NodeStatus.Primary
      else if (replicaSet.exists(_.isSecondary)) NodeStatus.Secondary
      else NodeStatus.NonQueryableUnknownStatus
    }

    val productArity: Int = 9

    def productElement(n: Int): Any = (n: @annotation.switch) match {
      case 0 => isMaster
      case 1 => maxBsonObjectSize
      case 2 => maxMessageSizeBytes
      case 3 => maxWriteBatchSize
      case 4 => localTime
      case 5 => minWireVersion
      case 6 => maxWireVersion
      case 7 => replicaSet
      case _ => msg
    }

    def canEqual(that: Any): Boolean = that match {
      case _: IsMasterResult => true
      case _                 => false
    }

    override def equals(that: Any): Boolean = that match {
      case other: IsMasterResult => other.tupled == tupled
      case _                     => false
    }

    override def hashCode: Int = tupled.hashCode

    private[commands] lazy val tupled = Tuple13(
      isMaster,
      maxBsonObjectSize,
      maxMessageSizeBytes,
      maxWriteBatchSize,
      localTime,
      logicalSessionTimeoutMinutes,
      minWireVersion,
      maxWireVersion,
      readOnly,
      compression,
      saslSupportedMech,
      replicaSet,
      msg)

  }

  object IsMasterResult extends scala.runtime.AbstractFunction9[Boolean, Int, Int, Int, Option[Long], Int, Int, Option[ReplicaSet], Option[String], IsMasterResult] {
    @deprecated("Use the complete constructor", "0.18.5")
    def apply(
      isMaster: Boolean,
      maxBsonObjectSize: Int,
      maxMessageSizeBytes: Int,
      maxWriteBatchSize: Int,
      localTime: Option[Long],
      minWireVersion: Int,
      maxWireVersion: Int,
      replicaSet: Option[ReplicaSet],
      msg: Option[String]): IsMasterResult = new IsMasterResult(isMaster, maxBsonObjectSize, maxMessageSizeBytes, maxWriteBatchSize, localTime, minWireVersion, maxWireVersion, replicaSet, msg)

    @deprecated("Use complete extractor", "0.18.5")
    def unapply(res: IsMasterResult): Option[(Boolean, Int, Int, Int, Option[Long], Int, Int, Option[ReplicaSet], Option[String])] = Some(Tuple9(res.isMaster, res.maxBsonObjectSize, res.maxMessageSizeBytes, res.maxWriteBatchSize, res.localTime, res.minWireVersion, res.maxWireVersion, res.replicaSet, res.msg))
  }

  // ---

  private[api] def writer[T <: IsMaster](pack: P): pack.Writer[T] = {
    val builder = pack.newBuilder
    import builder.{ elementProducer => element }

    val serializeClientMeta: ClientMetadata => Option[pack.Document] =
      ClientMetadata.serialize[pack.type](pack)

    pack.writer[T] { im =>
      val elms = Seq.newBuilder[pack.ElementProducer]

      elms += element("ismaster", builder.int(1))

      im.comment.foreach { comment =>
        elms += element(f"$$comment", builder.string(comment))
      }

      im.client.flatMap(serializeClientMeta).foreach { meta =>
        elms += element("client", meta)
      }

      builder.document(elms.result())
    }
  }

  private[reactivemongo] def reader(pack: P)(implicit sr: pack.NarrowValueReader[String]): pack.Reader[IsMasterResult] = {
    val decoder = pack.newDecoder

    import decoder.{ booleanLike, int, long, string, values }

    pack.reader[IsMasterResult] { doc =>
      def rs = for {
        me <- string(doc, "me")
        setName <- string(doc, "setName")
      } yield new ReplicaSet(
        setName = setName,
        setVersion = int(doc, "setVersion").getOrElse(-1),
        me = me,
        primary = string(doc, "primary"),
        hosts = values[String](doc, "hosts").getOrElse(Seq.empty),
        passives = values[String](doc, "passives").getOrElse(Seq.empty),
        arbiters = values[String](doc, "arbiters").getOrElse(Seq.empty),
        isSecondary = booleanLike(doc, "secondary").getOrElse(false),
        isArbiterOnly = booleanLike(doc, "arbiterOnly").getOrElse(false),
        isPassive = booleanLike(doc, "passive").getOrElse(false),
        isHidden = booleanLike(doc, "hidden").getOrElse(false),
        tags = decoder.child(doc, "tags").map { doc =>
          decoder.names(doc).flatMap { tag =>
            string(doc, tag).map(tag -> _)
          }.toMap
        }.getOrElse(Map.empty),
        electionId = int(doc, "electionId").getOrElse(-1),
        lastWrite = decoder.child(doc, "lastWrite").flatMap { ld =>
          for {
            opTime <- long(ld, "opTime")
            lastWriteDate <- long(ld, "lastWriteDate").map(new Date(_))
            majorityOpTime <- long(ld, "majorityOpTime")
            majorityWriteDate <- long(ld, "majorityWriteDate").map(new Date(_))
          } yield new LastWrite(
            opTime.toLong, lastWriteDate,
            majorityOpTime.toLong, majorityWriteDate)
        })

      new IsMasterResult(
        isMaster = booleanLike(doc, "ismaster").getOrElse(false), // `ismaster`
        maxBsonObjectSize = int(doc, "maxBsonObjectSize").
          getOrElse(16777216), // default = 16 * 1024 * 1024
        maxMessageSizeBytes = int(doc, "maxMessageSizeBytes").
          getOrElse(48000000), // default = 48000000, mongod >= 2.4
        maxWriteBatchSize = int(doc, "maxWriteBatchSize").getOrElse(1000),
        localTime = long(doc, "localTime"), // date? mongod >= 2.2
        logicalSessionTimeoutMinutes = long(
          doc, "logicalSessionTimeoutMinutes"),
        minWireVersion = int(doc, "minWireVersion").
          getOrElse(0), // int? mongod >= 2.6
        maxWireVersion = int(doc, "maxWireVersion").
          getOrElse(0), // int? mongod >= 2.6
        readOnly = booleanLike(doc, "readOnly"),
        compression = values[String](doc, "compression").
          fold(List.empty[String])(_.toList),
        saslSupportedMech = values[String](doc, "saslSupportedMech").
          fold(List.empty[String])(_.toList),
        replicaSet = rs, // flattened in the result
        msg = string(doc, "msg") // Contains the value isdbgrid when isMaster returns from a mongos instance.
      )
    }
  }

}
