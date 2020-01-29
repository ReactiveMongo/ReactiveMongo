package reactivemongo.api.commands.bson

import scala.util.{ Failure, Success }

import reactivemongo.bson.{
  BSONBooleanLike,
  BSONDocument,
  BSONDocumentReader,
  BSONDocumentWriter,
  BSONInteger,
  BSONNumberLike,
  BSONString,
  BSONValue,
  BSONWriter
}

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands._

import reactivemongo.core.errors.GenericDriverException

@deprecated("Internal: will be made private", "0.16.0")
object BSONDropDatabaseImplicits {
  object DropDatabaseWriter
    extends BSONDocumentWriter[DropDatabase.type] {
    val command = BSONDocument("dropDatabase" -> 1)
    def write(dd: DropDatabase.type): BSONDocument = command
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONListCollectionNamesImplicits {
  object ListCollectionNamesWriter
    extends BSONDocumentWriter[ListCollectionNames.type] {
    val command = BSONDocument("listCollections" -> 1)
    def write(ls: ListCollectionNames.type): BSONDocument = command
  }

  object BSONCollectionNameReaders
    extends DealingWithGenericCommandErrorsReader[CollectionNames] {
    def readResult(doc: BSONDocument): CollectionNames = (for {
      cr <- doc.getAs[BSONDocument]("cursor")
      fb <- cr.getAs[List[BSONDocument]]("firstBatch")
      ns <- wtColNames(fb, Nil)
    } yield CollectionNames(ns)).getOrElse[CollectionNames](
      throw GenericDriverException("fails to read collection names"))
  }

  @annotation.tailrec
  private def wtColNames(meta: List[BSONDocument], ns: List[String]): Option[List[String]] = meta match {
    case d :: ds => d.getAs[String]("name") match {
      case Some(n) => wtColNames(ds, n :: ns)
      case _       => Option.empty[List[String]] // error
    }
    case _ => Some(ns.reverse)
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONDropCollectionImplicits {
  object DropCollectionWriter extends BSONDocumentWriter[ResolvedCollectionCommand[DropCollection.type]] {
    def write(command: ResolvedCollectionCommand[DropCollection.type]): BSONDocument = BSONDocument("drop" -> command.collection)
  }

  object DropCollectionResultReader
    extends BSONDocumentReader[DropCollectionResult] {
    def read(doc: BSONDocument): DropCollectionResult =
      CommonImplicits.UnitBoxReader.readTry(doc).transform(
        { _ => Success(true) }, { error =>
          if (doc.getAs[BSONNumberLike]("code"). // code == 26
            map(_.toInt).exists(_ == 26) ||
            doc.getAs[String]("errmsg").
            exists(_ startsWith "ns not found")) {
            Success(false) // code not avail. before 3.x
          } else Failure(error)
        }).map(DropCollectionResult(_)).get
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONEmptyCappedImplicits {
  implicit object EmptyCappedWriter extends BSONDocumentWriter[ResolvedCollectionCommand[EmptyCapped.type]] {
    def write(command: ResolvedCollectionCommand[EmptyCapped.type]): BSONDocument = BSONDocument("emptyCapped" -> command.collection)
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONRenameCollectionImplicits {
  object RenameCollectionWriter extends BSONDocumentWriter[RenameCollection] {
    def write(command: RenameCollection): BSONDocument =
      BSONDocument(
        "renameCollection" -> command.fullyQualifiedCollectionName,
        "to" -> command.fullyQualifiedTargetName,
        "dropTarget" -> command.dropTarget)
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONCreateImplicits {
  object CappedWriter extends BSONDocumentWriter[Capped] {
    def write(capped: Capped): BSONDocument =
      BSONDocument(
        "size" -> capped.size,
        "max" -> capped.max)
  }

  object CreateWriter extends BSONDocumentWriter[ResolvedCollectionCommand[Create]] {
    def write(command: ResolvedCollectionCommand[Create]): BSONDocument = {
      val base = BSONDocument("create" -> command.collection)

      val cmd = if (command.command.autoIndexId) {
        base.merge("autoIndexId" -> command.command.autoIndexId)
      } else base

      command.command.capped.fold(cmd) { capped =>
        cmd.merge("capped" -> true) ++ CappedWriter.write(capped)
      }
    }
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONCollStatsImplicits {
  object CollStatsWriter extends BSONDocumentWriter[ResolvedCollectionCommand[CollStats]] {
    def write(command: ResolvedCollectionCommand[CollStats]): BSONDocument =
      BSONDocument(
        "collStats" -> command.collection,
        "scale" -> command.command.scale)
  }

  implicit object CollStatsResultReader extends DealingWithGenericCommandErrorsReader[CollStatsResult] {
    def readResult(doc: BSONDocument): CollStatsResult = CollStatsResult(
      doc.getAs[String]("ns").get,
      doc.getAs[BSONNumberLike]("count").map(_.toInt).get,
      doc.getAs[BSONNumberLike]("size").map(_.toDouble).get,
      doc.getAs[BSONNumberLike]("avgObjSize").map(_.toDouble),
      doc.getAs[BSONNumberLike]("storageSize").map(_.toDouble).get,
      doc.getAs[BSONNumberLike]("numExtents").map(_.toInt),
      doc.getAs[BSONNumberLike]("nindexes").map(_.toInt).get,
      doc.getAs[BSONNumberLike]("lastExtentSize").map(_.toInt),
      doc.getAs[BSONNumberLike]("paddingFactor").map(_.toDouble),
      doc.getAs[BSONNumberLike]("systemFlags").map(_.toInt),
      doc.getAs[BSONNumberLike]("userFlags").map(_.toInt),
      doc.getAs[BSONNumberLike]("totalIndexSize").map(_.toInt).get,
      {
        val indexSizes = doc.getAs[BSONDocument]("indexSizes").get
        (for (kv <- indexSizes.elements)
          yield kv.name -> kv.value.asInstanceOf[BSONInteger].value).toList
      },
      doc.getAs[BSONBooleanLike]("capped").fold(false)(_.toBoolean),
      doc.getAs[BSONNumberLike]("max").map(_.toLong),
      doc.getAs[BSONNumberLike]("maxSize").map(_.toDouble))
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONConvertToCappedImplicits {
  object ConvertToCappedWriter extends BSONDocumentWriter[ResolvedCollectionCommand[ConvertToCapped]] {
    def write(command: ResolvedCollectionCommand[ConvertToCapped]): BSONDocument =
      BSONDocument("convertToCapped" -> command.collection) ++ BSONCreateImplicits.CappedWriter.write(command.command.capped)
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONDropIndexesImplicits {
  object BSONDropIndexesWriter extends BSONDocumentWriter[ResolvedCollectionCommand[DropIndexes]] {
    def write(command: ResolvedCollectionCommand[DropIndexes]): BSONDocument =
      BSONDocument(
        "dropIndexes" -> command.collection,
        "index" -> command.command.index)
  }

  object BSONDropIndexesReader extends DealingWithGenericCommandErrorsReader[DropIndexesResult] {
    def readResult(doc: BSONDocument): DropIndexesResult =
      DropIndexesResult(doc.getAs[BSONNumberLike]("nIndexesWas").map(_.toInt).getOrElse(0))
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONListIndexesImplicits {
  import scala.util.{ Failure, Success, Try }
  import reactivemongo.api.indexes.{ Index, IndexesManager }

  object BSONListIndexesWriter extends BSONDocumentWriter[ResolvedCollectionCommand[ListIndexes]] {
    def write(command: ResolvedCollectionCommand[ListIndexes]): BSONDocument =
      BSONDocument("listIndexes" -> command.collection)
  }

  object BSONIndexListReader
    extends DealingWithGenericCommandErrorsReader[List[Index]] {

    @deprecated("Only for internal use", "0.12.7")
    @annotation.tailrec
    def readBatch(batch: List[BSONDocument], indexes: List[Index]): Try[List[Index]] = batch match {
      case d :: ds => d.asTry[Index](IndexesManager.IndexReader) match {
        case Success(i) => readBatch(ds, i :: indexes)
        case Failure(e) => Failure(e)
      }
      case _ => Success(indexes)
    }

    import BSONCommonWriteCommandsImplicits.DefaultWriteResultReader

    @deprecated("Use [[BSONCommonWriteCommandsImplicits.DefaultWriteResultReader]]", "0.12.7")
    val LastErrorReader: BSONDocumentReader[WriteResult] =
      BSONDocumentReader[WriteResult] { DefaultWriteResultReader.read(_) }

    def readResult(doc: BSONDocument): List[Index] = (for {
      a <- doc.getAs[BSONDocument]("cursor")
      b <- a.getAs[List[BSONDocument]]("firstBatch")
    } yield b).fold[List[Index]](throw GenericDriverException(
      "the cursor and firstBatch must be defined"))(readBatch(_, Nil).get)

  }
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONCreateIndexesImplicits {
  import reactivemongo.api.{ BSONSerializationPack => LegacyPack }
  import reactivemongo.api.commands.WriteResult

  object BSONCreateIndexesWriter extends BSONDocumentWriter[ResolvedCollectionCommand[CreateIndexes]] {
    import reactivemongo.api.indexes.{ IndexesManager, NSIndex, Index }
    implicit val nsIndexWriter = IndexesManager.nsIndexWriter(LegacyPack)

    def write(cmd: ResolvedCollectionCommand[CreateIndexes]): BSONDocument = {
      type Index = Index.Aux[LegacyPack.type]
      val indexes = cmd.command.indexes.collect {
        case i: Index =>
          val nsi = NSIndex.at(cmd.command.db + "." + cmd.collection, i)

          nsIndexWriter.write(nsi)
      }

      BSONDocument(
        "createIndexes" -> cmd.collection,
        "indexes" -> indexes)
    }
  }

  @deprecated("Use [[BSONCommonWriteCommandsImplicits.DefaultWriteResultReader]]", "0.12.7")
  val BSONCreateIndexesResultReader = BSONDocumentReader[WriteResult] {
    BSONCommonWriteCommandsImplicits.DefaultWriteResultReader.read(_)
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONReplSetGetStatusImplicits {
  object ReplSetGetStatusWriter
    extends BSONDocumentWriter[ReplSetGetStatus.type] {

    val bsonCmd = BSONDocument("replSetGetStatus" -> 1)
    def write(command: ReplSetGetStatus.type): BSONDocument = bsonCmd
  }

  object ReplSetMemberReader
    extends BSONDocumentReader[ReplSetMember] {

    def read(doc: BSONDocument): ReplSetMember = (for {
      id <- doc.getAsTry[BSONNumberLike]("_id").map(_.toLong)
      name <- doc.getAsTry[String]("name")
      health <- doc.getAsTry[BSONNumberLike]("health").map(_.toInt)
      state <- doc.getAsTry[BSONNumberLike]("state").map(_.toInt)
      stateStr <- doc.getAsTry[String]("stateStr")
      uptime <- doc.getAsTry[BSONNumberLike]("uptime").map(_.toLong)
      optime <- doc.getAsTry[BSONNumberLike]("optimeDate").map(_.toLong)
    } yield ReplSetMember(id, name, health, state, stateStr, uptime, optime,
      doc.getAs[BSONNumberLike]("lastHeartbeat").map(_.toLong),
      doc.getAs[BSONNumberLike]("lastHeartbeatRecv").map(_.toLong),
      doc.getAs[String]("lastHeartbeatMessage"),
      doc.getAs[BSONNumberLike]("electionTime").map(_.toLong),
      doc.getAs[BSONBooleanLike]("self").fold(false)(_.toBoolean),
      doc.getAs[BSONNumberLike]("pingMs").map(_.toLong),
      doc.getAs[String]("syncingTo"),
      doc.getAs[BSONNumberLike]("configVersion").map(_.toInt))).get

  }

  object ReplSetStatusReader
    extends DealingWithGenericCommandErrorsReader[ReplSetStatus] {

    private implicit def memberReader = ReplSetMemberReader

    def readResult(doc: BSONDocument): ReplSetStatus = (for {
      name <- doc.getAsTry[String]("set")
      time <- doc.getAsTry[BSONNumberLike]("date").map(_.toLong)
      myState <- doc.getAsTry[BSONNumberLike]("myState").map(_.toInt)
      members <- doc.getAsTry[List[ReplSetMember]]("members")
    } yield ReplSetStatus(name, time, myState, members)).get
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONResyncImplicits {
  private val logger =
    reactivemongo.util.LazyLogger("reactivemongo.api.commands.bson.Resync")

  implicit object ResyncReader extends BSONDocumentReader[ResyncResult.type] {
    @inline def notDeadWarn(err: BSONCommandError) =
      err.code.exists(_ == 125) || err.errmsg.exists(_ startsWith "not dead")

    def read(doc: BSONDocument): ResyncResult.type = try {
      CommonImplicits.UnitBoxReader.read(doc)
      ResyncResult
    } catch {
      case err: BSONCommandError if (notDeadWarn(err)) => {
        logger.warn(s"no resync done: ${err.errmsg mkString ""}")
        ResyncResult
      }
      case error: Throwable => throw error
    }
  }

  implicit object ResyncWriter extends BSONDocumentWriter[Resync.type] {
    val command = BSONDocument("resync" -> 1)
    def write(dd: Resync.type): BSONDocument = command
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONReplSetMaintenanceImplicits {
  implicit val ReplSetMaintenanceReader = CommonImplicits.UnitBoxReader

  object ReplSetMaintenanceWriter
    extends BSONDocumentWriter[ReplSetMaintenance] {

    def write(command: ReplSetMaintenance) =
      BSONDocument("replSetMaintenance" -> command.enable)
  }
}

import reactivemongo.api.BSONSerializationPack

@deprecated("Internal: will be made private", "0.16.0")
object BSONCreateUserCommand
  extends CreateUserCommand[BSONSerializationPack.type] {

  import BSONCommonWriteCommandsImplicits.WriteConcernWriter

  val pack = BSONSerializationPack

  implicit object UserRoleWriter extends BSONWriter[UserRole, BSONValue] {
    def write(role: UserRole): BSONValue = role match {
      case DBUserRole(name, dbn) => BSONDocument("role" -> name, "db" -> dbn)
      case _                     => BSONString(role.name)
    }
  }

  object CreateUserWriter extends BSONDocumentWriter[CreateUser] {
    def write(create: CreateUser) = BSONDocument(
      "createUser" -> create.name,
      "pwd" -> create.pwd,
      "customData" -> create.customData,
      "roles" -> create.roles,
      "digestPassword" -> create.digestPassword,
      "writeConcern" -> create.writeConcern)
  }
}

@deprecated("Internal: will be made private", "0.16.0")
object BSONPingCommandImplicits {
  implicit object PingWriter extends BSONDocumentWriter[PingCommand.type] {
    val command = BSONDocument("ping" -> 1.0)
    def write(ping: PingCommand.type): BSONDocument = command
  }

  implicit object PingReader extends DealingWithGenericCommandErrorsReader[Boolean] {
    def readResult(bson: BSONDocument): Boolean =
      bson.getAs[BSONBooleanLike]("ok").exists(_.toBoolean)
  }
}
