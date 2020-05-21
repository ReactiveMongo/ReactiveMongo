package reactivemongo.api.commands

import reactivemongo.api.{ SerializationPack, WriteConcern }

import reactivemongo.core.errors.GenericDriverException

private[reactivemongo] object DropDatabase extends Command with CommandWithResult[Unit] {
  private[api] def writer[P <: SerializationPack](pack: P): pack.Writer[DropDatabase.type] = {
    val builder = pack.newBuilder
    val cmd = builder.document(Seq(
      builder.elementProducer("dropDatabase", builder.int(1))))

    pack.writer[DropDatabase.type](_ => cmd)
  }
}

private[reactivemongo] object DropCollection extends CollectionCommand
  with CommandWithResult[Unit] {

  private[api] def writer[P <: SerializationPack](pack: P): pack.Writer[ResolvedCollectionCommand[DropCollection.type]] = {
    val builder = pack.newBuilder

    pack.writer[ResolvedCollectionCommand[DropCollection.type]] { drop =>
      builder.document(Seq(
        builder.elementProducer("drop", builder.string(drop.collection))))
    }
  }
}

private[reactivemongo] case class RenameCollection(
  fullyQualifiedCollectionName: String,
  fullyQualifiedTargetName: String,
  dropTarget: Boolean = false) extends Command with CommandWithResult[Unit]

private[api] object RenameCollection {
  def writer[P <: SerializationPack](pack: P): pack.Writer[RenameCollection] = {
    val builder = pack.newBuilder

    import builder.{ elementProducer => element, string }

    pack.writer[RenameCollection] { rename =>
      val elements = Seq[pack.ElementProducer](
        element(
          "renameCollection",
          string(rename.fullyQualifiedCollectionName)),
        element("to", string(rename.fullyQualifiedTargetName)),
        element("dropTarget", builder.boolean(rename.dropTarget)))

      builder.document(elements)
    }
  }
}

// TODO: storageEngine
// TODO: validator, validationLevel, validationAction
// TODO: indexOptionDefaults
private[api] case class Create(
  capped: Option[Capped] = None, // if set, "capped" -> true, size -> <int>, max -> <int>,
  writeConcern: WriteConcern = WriteConcern.Default,
  flags: Int = 1 // defaults to 1
) extends CollectionCommand with CommandWithResult[Unit]

private[api] object CreateCollection {
  def writer[P <: SerializationPack](pack: P): pack.Writer[ResolvedCollectionCommand[Create]] = {
    val builder = pack.newBuilder
    import builder.{ boolean, elementProducer => element }

    val writeWriteConcern = CommandCodecs.writeWriteConcern(builder)

    pack.writer[ResolvedCollectionCommand[Create]] { create =>
      val elms = Seq.newBuilder[pack.ElementProducer]

      elms ++= Seq(
        element("create", builder.string(create.collection)),
        element("writeConcern", writeWriteConcern(create.command.writeConcern)))

      create.command.capped.foreach { capped =>
        elms += element("capped", boolean(true))

        Capped.writeTo[pack.type](pack)(builder, elms += _)(capped)
      }

      builder.document(elms.result())
    }
  }
}

private[reactivemongo] final class ConvertToCapped(
  val capped: Capped)
  extends CollectionCommand with CommandWithResult[Unit] {

  override def equals(that: Any): Boolean = that match {
    case other: ConvertToCapped =>
      this.capped == other.capped

    case _ =>
      false
  }

  override def hashCode: Int = capped.hashCode

  override def toString: String = s"ConvertToCapped($capped)"
}

private[api] object ConvertToCapped {
  def writer[P <: SerializationPack](pack: P): pack.Writer[ResolvedCollectionCommand[ConvertToCapped]] = {
    val builder = pack.newBuilder

    pack.writer[ResolvedCollectionCommand[ConvertToCapped]] { convert =>
      val elms = Seq.newBuilder[pack.ElementProducer]

      elms += builder.elementProducer(
        "convertToCapped", builder.string(convert.collection))

      Capped.writeTo[pack.type](pack)(
        builder, elms += _)(convert.command.capped)

      builder.document(elms.result())
    }
  }
}

private[api] case class CollectionNames(names: List[String])

/** List the names of DB collections. */
private[api] object ListCollectionNames
  extends Command with CommandWithResult[CollectionNames] {

  def writer[P <: SerializationPack](pack: P): pack.Writer[ListCollectionNames.type] = {
    val builder = pack.newBuilder

    val cmd = builder.document(Seq(
      builder.elementProducer("listCollections", builder.int(1))))

    pack.writer[ListCollectionNames.type](_ => cmd)
  }

  def reader[P <: SerializationPack](pack: P): pack.Reader[CollectionNames] = {
    val decoder = pack.newDecoder

    CommandCodecs.dealingWithGenericCommandExceptionsReaderOpt[pack.type, CollectionNames](pack) { doc =>
      (for {
        cr <- decoder.child(doc, "cursor")
        fb = decoder.children(cr, "firstBatch")
        ns <- wtColNames[pack.type](pack)(decoder, fb, List.empty)
      } yield CollectionNames(ns)).orElse[CollectionNames](
        throw new GenericDriverException("fails to read collection names"))
    }
  }

  @annotation.tailrec
  private def wtColNames[P <: SerializationPack](pack: P)(decoder: SerializationPack.Decoder[pack.type], meta: List[pack.Document], ns: List[String]): Option[List[String]] = meta match {
    case d :: ds => decoder.string(d, "name") match {
      case Some(n) => wtColNames[pack.type](pack)(decoder, ds, n :: ns)
      case _       => Option.empty[List[String]] // error
    }

    case _ => Some(ns.reverse)
  }
}

/**
 * Replica set member.
 *
 * @param name the member name (e.g. "host:port")
 * @param health the health indicator for this member
 * @param state the [[http://docs.mongodb.org/manual/reference/replica-states/ state code]] of the member
 * @param stateStr the string representation of the state
 * @param uptime the number of seconds that this member has been online
 * @param optime information regarding the last operation from the operation log that this member has applied
 * @param lastHeartbeat the time of the transmission time of last heartbeat received from this member
 * @param lastHeartbeatRecv the time that the last heartbeat was received from this member
 * @param lastHeartbeatMessage an optional message from the last heartbeat
 * @param electionTime if the member is the primary, the time it was elected as
 * @param self indicates which replica set member processed the replSetGetStatus command
 * @param pingMs the number of milliseconds (ms) that a round-trip packet takes to travel between the remote member and the local instance (does not appear for the member that returns the `rs.status()` data)
 * @param syncingTo the hostname of the member from which this instance is syncing (only present on the output of `rs.status()` on secondary and recovering members)
 * @param configVersion the configuration version (since MongoDB 3.0)
 */
final class ReplSetMember private[api] (
  val _id: Long,
  val name: String,
  val health: Int,
  val state: Int,
  val stateStr: String,
  val uptime: Long,
  val optime: Long,
  val lastHeartbeat: Option[Long],
  val lastHeartbeatRecv: Option[Long],
  val lastHeartbeatMessage: Option[String],
  val electionTime: Option[Long],
  val self: Boolean,
  val pingMs: Option[Long],
  val syncingTo: Option[String],
  val configVersion: Option[Int]) {

  private[api] def tupled = Tuple15(_id, name, health, state, stateStr, uptime, optime, lastHeartbeat, lastHeartbeatRecv, lastHeartbeatMessage, electionTime, self, pingMs, syncingTo, configVersion)

  override def equals(that: Any): Boolean = that match {
    case other: ReplSetMember =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString = s"ReplSetMember${tupled.toString}"
}

/**
 * Result from the [[http://docs.mongodb.org/manual/reference/command/replSetGetStatus/ replSetGetStatus]].
 *
 * @param name the name of the replica set
 * @param time the current server time
 * @param state the [[http://docs.mongodb.org/manual/reference/replica-states/ state code]] of the current member
 * @param members the list of the members of this replicate set
 */
final class ReplSetStatus private[api] (
  val name: String,
  val time: Long,
  val myState: Int,
  val members: List[ReplSetMember]) {

  private[api] lazy val tupled = Tuple4(name, time, myState, members)

  override def equals(that: Any): Boolean = that match {
    case other: ReplSetStatus =>
      this.tupled == other.tupled

    case _ =>
      false
  }

  override def hashCode: Int = tupled.hashCode

  override def toString = s"ReplSetStatus${tupled.toString}"
}

/**
 * The command [[http://docs.mongodb.org/manual/reference/command/replSetGetStatus/ replSetGetStatus]]
 */
private[reactivemongo] object ReplSetGetStatus
  extends Command with CommandWithResult[ReplSetStatus] {

  def writer[P <: SerializationPack](pack: P): pack.Writer[ReplSetGetStatus.type] = {
    val builder = pack.newBuilder
    val cmd = builder.document(Seq(builder.elementProducer(
      "replSetGetStatus", builder.int(1))))

    pack.writer[ReplSetGetStatus.type](_ => cmd)
  }

  @SuppressWarnings(Array("UnusedMethodParameter"))
  private def readMember[P <: SerializationPack](pack: P)(decoder: SerializationPack.Decoder[pack.type], doc: pack.Document): Option[ReplSetMember] = {
    import decoder.{ int, long, string }

    for {
      id <- long(doc, "_id")
      name <- string(doc, "name")
      health <- int(doc, "health")
      state <- int(doc, "state")
      stateStr <- string(doc, "stateStr")
      uptime <- long(doc, "uptime")
      optime <- long(doc, "optimeDate")
    } yield new ReplSetMember(id, name, health, state, stateStr, uptime, optime,
      long(doc, "lastHeartbeat"),
      long(doc, "lastHeartbeatRecv"),
      string(doc, "lastHeartbeatMessage"),
      long(doc, "electionTime"),
      decoder.booleanLike(doc, "self").getOrElse(false),
      long(doc, "pingMs"),
      string(doc, "syncingTo"),
      int(doc, "configVersion"))
  }

  def reader[P <: SerializationPack](pack: P): pack.Reader[ReplSetStatus] = {
    val decoder = pack.newDecoder

    CommandCodecs.dealingWithGenericCommandExceptionsReaderOpt[pack.type, ReplSetStatus](pack) { doc =>
      (for {
        name <- decoder.string(doc, "set")
        time <- decoder.long(doc, "date")
        myState <- decoder.int(doc, "myState")
        members = decoder.children(doc, "members").flatMap { m =>
          readMember[pack.type](pack)(decoder, m)
        }
      } yield new ReplSetStatus(name, time, myState, members))
    }
  }
}

/**
 * The command [[https://docs.mongodb.org/manual/reference/command/resync/ resync]]
 */
private[api] object Resync extends Command with CommandWithResult[Unit]

/**
 * The [[https://docs.mongodb.org/manual/reference/command/replSetMaintenance/ replSetMaintenance]] command.
 * It must be executed against the `admin` database.
 *
 * @param enable if true the the member enters the `RECOVERING` state
 */
private[reactivemongo] final class ReplSetMaintenance(
  val enable: Boolean = true) extends Command with CommandWithResult[Unit] {
  override def equals(that: Any): Boolean = that match {
    case other: ReplSetMaintenance =>
      this.enable == other.enable

    case _ =>
      false
  }

  override def hashCode: Int = enable.hashCode

  override def toString = s"ReplSetMaintenance($enable)"
}

private[reactivemongo] object ReplSetMaintenance {

  @inline def apply(enable: Boolean): ReplSetMaintenance =
    new ReplSetMaintenance(enable)

  private[api] def writer[P <: SerializationPack](pack: P): pack.Writer[ReplSetMaintenance] = {
    val builder = pack.newBuilder

    pack.writer[ReplSetMaintenance] { set =>
      builder.document(Seq(builder.elementProducer(
        "replSetMaintenance", builder.boolean(set.enable))))
    }
  }
}

/**
 * The [[https://docs.mongodb.com/manual/reference/command/ping/ ping]] command.
 */
private[reactivemongo] object PingCommand
  extends Command with CommandWithResult[Boolean]
