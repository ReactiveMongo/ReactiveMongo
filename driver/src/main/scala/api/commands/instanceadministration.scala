package reactivemongo.api.commands

import scala.util.control.NonFatal

import reactivemongo.api.SerializationPack

import reactivemongo.core.errors.GenericDriverException

@deprecated("Internal: will be made private", "0.16.0")
object DropDatabase extends Command with CommandWithResult[UnitBox.type] {
  private[api] def writer[P <: SerializationPack](pack: P): pack.Writer[DropDatabase.type] = {
    val builder = pack.newBuilder
    val cmd = builder.document(Seq(
      builder.elementProducer("dropDatabase", builder.int(1))))

    pack.writer[DropDatabase.type](_ => cmd)
  }
}

/**
 * @param dropped true if the collection existed and was dropped
 */
@deprecated("Internal: will be made private", "0.16.0")
case class DropCollectionResult(dropped: Boolean)

private[api] object DropCollectionResult { //extends scala.runtime.AbstractFunction1[Boolean, DropCollectionResult] {
  //@inline def apply(dropped: Boolean): DropCollectionResult = new DropCollectionResult(dropped)

  def reader[P <: SerializationPack](pack: P): pack.Reader[DropCollectionResult] = {
    val decoder = pack.newDecoder
    val unitBoxReader = CommandCodecs.unitBoxReader[pack.type](pack)

    pack.reader[DropCollectionResult] { doc =>
      try {
        pack.deserialize(doc, unitBoxReader)

        DropCollectionResult(true)
      } catch {
        case NonFatal(cause) =>
          def code = decoder.int(doc, "code")
          def msg = decoder.string(doc, "errmsg")

          if (code.exists(_ == 26) || msg.exists(_ startsWith "ns not found")) {
            DropCollectionResult(false)
          } else {
            throw cause
          }
      }
    }
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object DropCollection extends CollectionCommand
  with CommandWithResult[DropCollectionResult] {

  private[api] def writer[P <: SerializationPack](pack: P): pack.Writer[ResolvedCollectionCommand[DropCollection.type]] = {
    val builder = pack.newBuilder

    pack.writer[ResolvedCollectionCommand[DropCollection.type]] { drop =>
      builder.document(Seq(
        builder.elementProducer("drop", builder.string(drop.collection))))
    }
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object EmptyCapped extends CollectionCommand
  with CommandWithResult[UnitBox.type]

@deprecated("Internal: will be made private", "0.16.0")
case class RenameCollection(
  fullyQualifiedCollectionName: String,
  fullyQualifiedTargetName: String,
  dropTarget: Boolean = false) extends Command with CommandWithResult[UnitBox.type]

private[api] object RenameCollection extends scala.runtime.AbstractFunction3[String, String, Boolean, RenameCollection] {
  def writer[P <: SerializationPack with Singleton](pack: P): pack.Writer[RenameCollection] = {
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

@deprecated("Internal: will be made private", "0.16.0")
case class Create(
  capped: Option[Capped] = None, // if set, "capped" -> true, size -> <int>, max -> <int>
  autoIndexId: Boolean = true, // optional
  flags: Int = 1 // defaults to 1
) extends CollectionCommand with CommandWithResult[UnitBox.type]

private[api] object CreateCollection {
  def writer[P <: SerializationPack](pack: P): pack.Writer[ResolvedCollectionCommand[Create]] = {
    val builder = pack.newBuilder
    import builder.{ boolean, elementProducer => element }

    pack.writer[ResolvedCollectionCommand[Create]] { create =>
      val elms = Seq.newBuilder[pack.ElementProducer]

      elms += element("create", builder.string(create.collection))

      if (create.command.autoIndexId) {
        elms += element("autoIndexId", boolean(create.command.autoIndexId))
      }

      create.command.capped.foreach { capped =>
        elms += element("capped", boolean(true))

        Capped.writeTo[pack.type](pack)(builder, elms += _)(capped)
      }

      builder.document(elms.result())
    }
  }
}

@deprecated("Internal: will be made private", "0.16.0")
class ConvertToCapped(
  val capped: Capped) extends Product with Serializable with CollectionCommand with CommandWithResult[UnitBox.type] {

  val productArity = 1

  def productElement(n: Int): Any = capped

  def canEqual(that: Any): Boolean = that match {
    case _: ConvertToCapped => true
    case _                  => false
  }

  override def equals(that: Any): Boolean = that match {
    case other: ConvertToCapped =>
      this.capped == other.capped

    case _ =>
      false
  }

  override def hashCode: Int = capped.hashCode

  override def toString: String = s"ConvertToCapped($capped)"
}

object ConvertToCapped
  extends scala.runtime.AbstractFunction1[Capped, ConvertToCapped] {

  @inline def apply(capped: Capped): ConvertToCapped =
    new ConvertToCapped(capped)

  private[api] def writer[P <: SerializationPack](pack: P): pack.Writer[ResolvedCollectionCommand[ConvertToCapped]] = {
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

@deprecated("Internal: will be made private", "0.16.0")
case class CollectionNames(names: List[String])

/** List the names of DB collections. */
@deprecated("Internal: will be made private", "0.16.0")
object ListCollectionNames
  extends Command with CommandWithResult[CollectionNames] {

  private[api] def writer[P <: SerializationPack](pack: P): pack.Writer[ListCollectionNames.type] = {
    val builder = pack.newBuilder

    val cmd = builder.document(Seq(
      builder.elementProducer("listCollections", builder.int(1))))

    pack.writer[ListCollectionNames.type](_ => cmd)
  }

  private[api] def reader[P <: SerializationPack](pack: P): pack.Reader[CollectionNames] = {
    val decoder = pack.newDecoder

    CommandCodecs.dealingWithGenericCommandErrorsReader[pack.type, CollectionNames](pack) { doc =>
      (for {
        cr <- decoder.child(doc, "cursor")
        fb = decoder.children(cr, "firstBatch")
        ns <- wtColNames[pack.type](pack)(decoder, fb, List.empty)
      } yield CollectionNames(ns)).getOrElse[CollectionNames](
        throw GenericDriverException("fails to read collection names"))
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
case class ReplSetMember(
  _id: Long,
  name: String,
  health: Int,
  state: Int,
  stateStr: String,
  uptime: Long,
  optime: Long,
  lastHeartbeat: Option[Long],
  lastHeartbeatRecv: Option[Long],
  lastHeartbeatMessage: Option[String],
  electionTime: Option[Long],
  self: Boolean,
  pingMs: Option[Long],
  syncingTo: Option[String],
  configVersion: Option[Int])

/**
 * Result from the [[http://docs.mongodb.org/manual/reference/command/replSetGetStatus/ replSetGetStatus]].
 *
 * @param name the name of the replica set
 * @param time the current server time
 * @param state the [[http://docs.mongodb.org/manual/reference/replica-states/ state code]] of the current member
 * @param members the list of the members of this replicate set
 */
case class ReplSetStatus(
  name: String,
  time: Long,
  myState: Int,
  members: List[ReplSetMember])

/**
 * The command [[http://docs.mongodb.org/manual/reference/command/replSetGetStatus/ replSetGetStatus]]
 */
@deprecated("Internal: will be made private", "0.16.0")
case object ReplSetGetStatus
  extends Command with CommandWithResult[ReplSetStatus] {

  private[api] def writer[P <: SerializationPack](pack: P): pack.Writer[ReplSetGetStatus.type] = {
    val builder = pack.newBuilder
    val cmd = builder.document(Seq(builder.elementProducer(
      "replSetGetStatus", builder.int(1))))

    pack.writer[ReplSetGetStatus.type](_ => cmd)
  }

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
    } yield ReplSetMember(id, name, health, state, stateStr, uptime, optime,
      long(doc, "lastHeartbeat"),
      long(doc, "lastHeartbeatRecv"),
      string(doc, "lastHeartbeatMessage"),
      long(doc, "electionTime"),
      decoder.booleanLike(doc, "self").getOrElse(false),
      long(doc, "pingMs"),
      string(doc, "syncingTo"),
      int(doc, "configVersion"))
  }

  private[api] def reader[P <: SerializationPack](pack: P): pack.Reader[ReplSetStatus] = {
    val decoder = pack.newDecoder

    CommandCodecs.dealingWithGenericCommandErrorsReader[pack.type, ReplSetStatus](pack) { doc =>
      (for {
        name <- decoder.string(doc, "set")
        time <- decoder.long(doc, "date")
        myState <- decoder.int(doc, "myState")
        members = decoder.children(doc, "members").flatMap { m =>
          readMember[pack.type](pack)(decoder, m)
        }
      } yield ReplSetStatus(name, time, myState, members)).get
    }
  }
}

sealed trait ServerProcess

object ServerProcess {
  def unapply(repr: String): Option[ServerProcess] = repr match {
    case "mongos" => Some(MongosProcess)
    case "mongod" => Some(MongodProcess)
    case _        => None
  }
}

case object MongodProcess extends ServerProcess {
  override val toString = "mongod"
}
case object MongosProcess extends ServerProcess {
  override val toString = "mongos"
}

object ResyncResult extends BoxedAnyVal[Unit] {
  val value = {}
}

/**
 * The command [[https://docs.mongodb.org/manual/reference/command/resync/ resync]]
 */
@deprecated("Internal: will be made private", "0.16.0")
object Resync extends Command with CommandWithResult[ResyncResult.type]

/**
 * The [[https://docs.mongodb.org/manual/reference/command/replSetMaintenance/ replSetMaintenance]] command.
 * It must be executed against the `admin` database.
 *
 * @param enable if true the the member enters the `RECOVERING` state
 */
@deprecated("Internal: will be made private", "0.16.0")
case class ReplSetMaintenance(enable: Boolean = true) extends Command
  with CommandWithResult[UnitBox.type]

@deprecated("Internal: will be made private", "0.19.0")
object ReplSetMaintenance {
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
@deprecated("Internal: will be made private", "0.16.0")
case object PingCommand extends Command with CommandWithResult[Boolean]
