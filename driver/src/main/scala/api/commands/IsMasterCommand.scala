package reactivemongo.api.commands

import java.util.Date

import scala.collection.immutable.ListSet

import reactivemongo.api.{ Compressor, SerializationPack }

import reactivemongo.core.ClientMetadata
import reactivemongo.core.nodeset.NodeStatus

// See https://github.com/mongodb/specifications/blob/master/source/mongodb-handshake/handshake.rst
// TODO: hello?
private[reactivemongo] trait IsMasterCommand[P <: SerializationPack] {
  /**
   * @param client the client metadata (only for first isMaster request)
   */
  private[reactivemongo] final class IsMaster(
    val client: Option[ClientMetadata],
    val compression: ListSet[Compressor],
    val comment: Option[String]) extends Command
    with CommandWithResult[IsMasterResult] with CommandWithPack[P] {
    val commandKind = CommandKind.Hello

    override def toString = s"IsMaster($client, $compression)"
  }

  private[reactivemongo] final class LastWrite(
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
  final class ReplicaSet private[commands] (
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
    val lastWrite: Option[LastWrite]) {

    // setVersion
    override lazy val toString = s"""ReplicaSet($setName, primary = $primary, me = $me, hosts = ${hosts.mkString("[", ",", "]")}, lastWrite = $lastWrite)"""

    private def tupled = (setName, setVersion, me, primary, hosts, passives, arbiters, isSecondary, isArbiterOnly, isPassive, isHidden, tags, electionId, lastWrite)

    override lazy val hashCode = tupled.hashCode

    override def equals(that: Any): Boolean = that match {
      case rs: this.type => tupled == rs.tupled
      case _             => false
    }
  }

  final class IsMasterResult private[commands] (
    val isMaster: Boolean, // `ismaster`
    val maxBsonObjectSize: Int, // default = 16 * 1024 * 1024
    val maxMessageSizeBytes: Int, // default = 48000000, mongod >= 2.4
    val maxWriteBatchSize: Int, // default = 1000, mongod >= 2.6
    val localTime: Option[Long], // date? mongod >= 2.2
    val logicalSessionTimeoutMinutes: Option[Long],
    val minWireVersion: Int, // int? mongod >= 2.6
    val maxWireVersion: Int, // int? mongod >= 2.6
    val readOnly: Option[Boolean],
    val compression: ListSet[Compressor],
    val saslSupportedMech: Seq[String], // GSSAPI, SCRAM-SHA-256, SCRAM-SHA-1
    val replicaSet: Option[ReplicaSet], // flattened in the result
    val msg: Option[String] // Contains the value isdbgrid when isMaster returns from a mongos instance.
  ) {
    def isMongos: Boolean = msg contains "isdbgrid"

    def status: NodeStatus = {
      if (isMaster) NodeStatus.Primary
      else if (replicaSet.exists(_.isSecondary)) NodeStatus.Secondary
      else NodeStatus.NonQueryableUnknownStatus
    }

    override def equals(that: Any): Boolean = that match {
      case other: this.type => other.tupled == tupled
      case _                => false
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

  private[reactivemongo] object IsMasterResult {
    def unapply(res: IsMasterResult) = Option(res).map(_.tupled)
  }

  private[reactivemongo] def writer(pack: P): pack.Writer[IsMaster] = {
    val builder = pack.newBuilder
    import builder.{ elementProducer => element }

    val serializeClientMeta: ClientMetadata => Option[pack.Document] =
      ClientMetadata.serialize[pack.type](pack)

    pack.writer[IsMaster] { im =>
      val elms = Seq.newBuilder[pack.ElementProducer]

      elms += element("ismaster", builder.int(1))

      im.client.flatMap(serializeClientMeta).foreach { meta =>
        elms += element("client", meta)
      }

      elms += element("compression", builder.array(
        im.compression.toSeq.map(c => builder.string(c.name))))

      im.comment.foreach { comment =>
        elms += element(f"$$comment", builder.string(comment))
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
        tags = decoder.child(doc, "tags").map { tagDoc =>
          reactivemongo.util.toFlatMap(decoder names tagDoc) { tag =>
            string(tagDoc, tag).map(tag -> _)
          }
        }.getOrElse(Map.empty),
        electionId = int(doc, "electionId").getOrElse(-1),
        lastWrite = decoder.child(doc, "lastWrite").flatMap { ld =>
          for {
            opTime <- long(ld, "opTime")
            lastWriteDate <- long(ld, "lastWriteDate").map(new Date(_))
            majorityOpTime <- long(ld, "majorityOpTime")
            majorityWriteDate <- long(ld, "majorityWriteDate").map(new Date(_))
          } yield new LastWrite(
            opTime, lastWriteDate,
            majorityOpTime, majorityWriteDate)
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
        minWireVersion = int(doc, "minWireVersion").getOrElse(0),
        maxWireVersion = int(doc, "maxWireVersion").getOrElse(0),
        readOnly = booleanLike(doc, "readOnly"),
        compression = values[String](doc, "compression").
          fold(ListSet.empty[Compressor]) { cs =>
            ListSet.empty[Compressor] ++ cs.collect {
              case Compressor.Snappy.name =>
                Compressor.Snappy

              case Compressor.Zlib.name =>
                Compressor.Zlib.DefaultCompressor

              case Compressor.Zstd.name =>
                Compressor.Zstd
            }
          },
        saslSupportedMech = values[String](doc, "saslSupportedMech").
          getOrElse(List.empty[String]),
        replicaSet = rs, // flattened in the result
        msg = string(doc, "msg") // Contains the value isdbgrid when isMaster returns from a mongos instance.
      )
    }
  }
}
