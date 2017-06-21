package reactivemongo.api.commands.bson

import scala.util.{ Failure, Success }

import reactivemongo.api.commands._
import reactivemongo.bson._

import reactivemongo.core.errors.GenericDriverException

object BSONDropDatabaseImplicits {
  implicit object DropDatabaseWriter
      extends BSONDocumentWriter[DropDatabase.type] {
    val command = BSONDocument("dropDatabase" -> 1)
    def write(dd: DropDatabase.type): BSONDocument = command
  }
}

object BSONListCollectionNamesImplicits {
  implicit object ListCollectionNamesWriter
      extends BSONDocumentWriter[ListCollectionNames.type] {
    val command = BSONDocument("listCollections" -> 1)
    def write(ls: ListCollectionNames.type): BSONDocument = command
  }

  implicit object BSONCollectionNameReaders
      extends BSONDocumentReader[CollectionNames] {
    def read(doc: BSONDocument): CollectionNames = (for {
      _ <- doc.getAs[BSONNumberLike]("ok").map(_.toInt).filter(_ == 1)
      cr <- doc.getAs[BSONDocument]("cursor")
      fb <- cr.getAs[List[BSONDocument]]("firstBatch")
      ns <- wtColNames(fb, Nil)
    } yield CollectionNames(ns)).getOrElse[CollectionNames](
      throw GenericDriverException("fails to read collection names")
    )
  }

  @annotation.tailrec
  private def wtColNames(meta: List[BSONDocument], ns: List[String]): Option[List[String]] = meta match {
    case d :: ds => d.getAs[String]("name") match {
      case Some(n) => wtColNames(ds, n :: ns)
      case _       => None // error
    }
    case _ => Some(ns.reverse)
  }
}

@deprecated("Use [[BSONDropCollectionImplicits]]", "0.12.0")
object BSONDropImplicits {
  implicit object DropWriter extends BSONDocumentWriter[ResolvedCollectionCommand[Drop.type]] {
    def write(command: ResolvedCollectionCommand[Drop.type]): BSONDocument =
      BSONDocument("drop" -> command.collection)
  }
}

object BSONDropCollectionImplicits {
  implicit object DropCollectionWriter extends BSONDocumentWriter[ResolvedCollectionCommand[DropCollection.type]] {
    def write(command: ResolvedCollectionCommand[DropCollection.type]): BSONDocument = BSONDocument("drop" -> command.collection)
  }

  implicit object DropCollectionResultReader
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
        }
      ).map(DropCollectionResult(_)).get
  }
}

object BSONEmptyCappedImplicits {
  implicit object EmptyCappedWriter extends BSONDocumentWriter[ResolvedCollectionCommand[EmptyCapped.type]] {
    def write(command: ResolvedCollectionCommand[EmptyCapped.type]): BSONDocument = BSONDocument("emptyCapped" -> command.collection)
  }
}

object BSONRenameCollectionImplicits {
  implicit object RenameCollectionWriter extends BSONDocumentWriter[RenameCollection] {
    def write(command: RenameCollection): BSONDocument =
      BSONDocument(
        "renameCollection" -> command.fullyQualifiedCollectionName,
        "to" -> command.fullyQualifiedTargetName,
        "dropTarget" -> command.dropTarget
      )
  }
}

object BSONCreateImplicits {
  implicit object CappedWriter extends BSONDocumentWriter[Capped] {
    def write(capped: Capped): BSONDocument =
      BSONDocument(
        "size" -> capped.size,
        "max" -> capped.max
      )
  }
  implicit object CreateWriter extends BSONDocumentWriter[ResolvedCollectionCommand[Create]] {
    def write(command: ResolvedCollectionCommand[Create]): BSONDocument =
      BSONDocument(
        "create" -> command.collection,
        "autoIndexId" -> command.command.autoIndexId
      ) ++ command.command.capped.fold(BSONDocument.empty)(capped => {
          CappedWriter.write(capped) ++ ("capped" -> true)
        })
  }
}

object BSONCollStatsImplicits {
  implicit object CollStatsWriter extends BSONDocumentWriter[ResolvedCollectionCommand[CollStats]] {
    def write(command: ResolvedCollectionCommand[CollStats]): BSONDocument =
      BSONDocument(
        "collStats" -> command.collection,
        "scale" -> command.command.scale
      )
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
        (for (kv <- indexSizes.elements) yield kv._1 -> kv._2.asInstanceOf[BSONInteger].value).toList
      },
      doc.getAs[BSONBooleanLike]("capped").fold(false)(_.toBoolean),
      doc.getAs[BSONNumberLike]("max").map(_.toLong),
      doc.getAs[BSONNumberLike]("maxSize").map(_.toDouble)
    )
  }
}

object BSONConvertToCappedImplicits {
  implicit object ConvertToCappedWriter extends BSONDocumentWriter[ResolvedCollectionCommand[ConvertToCapped]] {
    def write(command: ResolvedCollectionCommand[ConvertToCapped]): BSONDocument =
      BSONDocument("convertToCapped" -> command.collection) ++ BSONCreateImplicits.CappedWriter.write(command.command.capped)
  }
}

object BSONDropIndexesImplicits {
  implicit object BSONDropIndexesWriter extends BSONDocumentWriter[ResolvedCollectionCommand[DropIndexes]] {
    def write(command: ResolvedCollectionCommand[DropIndexes]): BSONDocument =
      BSONDocument(
        "dropIndexes" -> command.collection,
        "index" -> command.command.index
      )
  }

  implicit object BSONDropIndexesReader extends DealingWithGenericCommandErrorsReader[DropIndexesResult] {
    def readResult(doc: BSONDocument): DropIndexesResult =
      DropIndexesResult(doc.getAs[BSONNumberLike]("nIndexesWas").map(_.toInt).getOrElse(0))
  }
}

object BSONListIndexesImplicits {
  import scala.util.{ Failure, Success, Try }
  import reactivemongo.api.indexes.{ Index, IndexesManager }

  implicit object BSONListIndexesWriter extends BSONDocumentWriter[ResolvedCollectionCommand[ListIndexes]] {
    def write(command: ResolvedCollectionCommand[ListIndexes]): BSONDocument =
      BSONDocument("listIndexes" -> command.collection)
  }

  implicit object BSONIndexListReader extends BSONDocumentReader[List[Index]] {
    @annotation.tailrec
    def readBatch(batch: List[BSONDocument], indexes: List[Index]): Try[List[Index]] = batch match {
      case d :: ds => d.asTry[Index](IndexesManager.IndexReader) match {
        case Success(i) => readBatch(ds, i :: indexes)
        case Failure(e) => Failure(e)
      }
      case _ => Success(indexes)
    }

    implicit object LastErrorReader extends BSONDocumentReader[WriteResult] {
      def read(doc: BSONDocument): WriteResult = (for {
        ok <- doc.getAs[BSONBooleanLike]("ok").map(_.toBoolean)
        n = doc.getAs[BSONNumberLike]("n").fold(0)(_.toInt)
        msg <- doc.getAs[String]("errmsg")
        code <- doc.getAs[BSONNumberLike]("code").map(_.toInt)
      } yield DefaultWriteResult(
        ok, n, Nil, None, Some(code), Some(msg)
      )).get
    }

    def read(doc: BSONDocument): List[Index] = (for {
      _ <- doc.getAs[BSONNumberLike]("ok").fold[Option[Unit]](
        throw GenericDriverException(
          "the result of listIndexes must be ok"
        )
      ) { ok =>
          if (ok.toInt == 1) Some(()) else {
            throw doc.asOpt[WriteResult].
              flatMap[Exception](WriteResult.lastError).
              getOrElse(new GenericDriverException(
                s"fails to create index: ${BSONDocument pretty doc}"
              ))
          }
        }
      a <- doc.getAs[BSONDocument]("cursor")
      b <- a.getAs[List[BSONDocument]]("firstBatch")
    } yield b).fold[List[Index]](throw GenericDriverException(
      "the cursor and firstBatch must be defined"
    ))(readBatch(_, Nil).get)

  }
}

object BSONCreateIndexesImplicits {
  import reactivemongo.api.commands.WriteResult

  implicit object BSONCreateIndexesWriter extends BSONDocumentWriter[ResolvedCollectionCommand[CreateIndexes]] {
    import reactivemongo.api.indexes.{ IndexesManager, NSIndex }
    implicit val nsIndexWriter = IndexesManager.NSIndexWriter

    def write(cmd: ResolvedCollectionCommand[CreateIndexes]): BSONDocument = {
      BSONDocument(
        "createIndexes" -> cmd.collection,
        "indexes" -> cmd.command.indexes.map(NSIndex(
          cmd.command.db + "." + cmd.collection, _
        ))
      )
    }
  }

  implicit object BSONCreateIndexesResultReader
      extends BSONDocumentReader[WriteResult] {

    import reactivemongo.api.commands.DefaultWriteResult

    def read(doc: BSONDocument): WriteResult =
      doc.getAs[BSONNumberLike]("ok").map(_.toInt).fold[WriteResult](
        throw GenericDriverException("the count must be defined")
      ) { n =>
          doc.getAs[String]("errmsg").fold[WriteResult](
            DefaultWriteResult(true, n, Nil, None, None, None)
          )(
              err => DefaultWriteResult(false, n, Nil, None, None, Some(err))
            )
        }
  }
}

/**
 * {{{
 * import reactivemongo.api.commands.ReplSetGetStatus
 * import reactivemongo.api.commands.bson.BSONReplSetGetStatusImplicits._
 *
 * adminDb.runCommand(ReplSetGetStatus)
 * }}}
 */
object BSONReplSetGetStatusImplicits {
  implicit object ReplSetGetStatusWriter
      extends BSONDocumentWriter[ReplSetGetStatus.type] {

    val bsonCmd = BSONDocument("replSetGetStatus" -> 1)
    def write(command: ReplSetGetStatus.type): BSONDocument = bsonCmd
  }

  implicit object ReplSetMemberReader
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

  implicit object ReplSetStatusReader
      extends DealingWithGenericCommandErrorsReader[ReplSetStatus] {

    def readResult(doc: BSONDocument): ReplSetStatus = (for {
      name <- doc.getAsTry[String]("set")
      time <- doc.getAsTry[BSONNumberLike]("date").map(_.toLong)
      myState <- doc.getAsTry[BSONNumberLike]("myState").map(_.toInt)
      members <- doc.getAsTry[List[ReplSetMember]]("members")
    } yield ReplSetStatus(name, time, myState, members)).get
  }
}

/**
 * {{{
 * import reactivemongo.api.commands.Resync
 * import reactivemongo.api.commands.bson.BSONResyncImplicits._
 *
 * db.runCommand(Resync)
 * }}}
 */
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

object BSONReplSetMaintenanceImplicits {
  implicit val ReplSetMaintenanceReader = CommonImplicits.UnitBoxReader

  implicit object ReplSetMaintenanceWriter
      extends BSONDocumentWriter[ReplSetMaintenance] {

    def write(command: ReplSetMaintenance) =
      BSONDocument("replSetMaintenance" -> command.enable)
  }
}

import reactivemongo.api.BSONSerializationPack

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

  implicit object CreateUserWriter extends BSONDocumentWriter[CreateUser] {
    def write(create: CreateUser) = BSONDocument(
      "createUser" -> create.name,
      "pwd" -> create.pwd,
      "customData" -> create.customData,
      "roles" -> create.roles,
      "digestPassword" -> create.digestPassword,
      "writeConcern" -> create.writeConcern
    )
  }
}

object BSONPingCommandImplicits {
  implicit object PingWriter extends BSONDocumentWriter[PingCommand.type] {
    val command = BSONDocument("ping" -> 1.0)
    def write(ping: PingCommand.type): BSONDocument = command
  }

  implicit object PingReader extends BSONDocumentReader[Boolean] {
    def read(bson: BSONDocument): Boolean = bson.getAs[BSONBooleanLike]("ok").exists(_.toBoolean)
  }
}
