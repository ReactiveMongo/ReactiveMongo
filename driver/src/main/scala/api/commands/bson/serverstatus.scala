package reactivemongo.api.commands.bson

import reactivemongo.api.commands.{
  MongodProcess,
  ServerProcess,
  ServerStatus,
  ServerStatusAsserts,
  ServerStatusBackgroundFlushing,
  ServerStatusConnections,
  ServerStatusExtraInfo,
  ServerStatusGlobalLock,
  ServerStatusJournaling,
  ServerStatusJournalingTime,
  ServerStatusResult,
  ServerStatusLock,
  ServerStatusNetwork
}

import reactivemongo.bson.{
  BSONDocument,
  BSONDocumentReader,
  BSONDocumentWriter,
  BSONNumberLike
}

/**
 * {{{
 * import reactivemongo.api.commands.ServerStatus
 * import reactivemongo.api.commands.bson.BSONServerStatusImplicits._
 *
 * db.runCommand(ServerStatus)
 * }}}
 */
object BSONServerStatusImplicits {
  implicit object BSONServerStatusWriter
    extends BSONDocumentWriter[ServerStatus.type] {

    val bsonCmd = BSONDocument("serverStatus" -> 1)
    def write(command: ServerStatus.type) = bsonCmd
  }

  implicit object BSONServerStatusAssertsReader
    extends BSONDocumentReader[ServerStatusAsserts] {
    def read(doc: BSONDocument): ServerStatusAsserts = (for {
      regular <- doc.getAsTry[BSONNumberLike]("regular").map(_.toInt)
      warning <- doc.getAsTry[BSONNumberLike]("warning").map(_.toInt)
      msg <- doc.getAsTry[BSONNumberLike]("msg").map(_.toInt)
      user <- doc.getAsTry[BSONNumberLike]("user").map(_.toInt)
      rollovers <- doc.getAsTry[BSONNumberLike]("rollovers").map(_.toInt)
    } yield ServerStatusAsserts(regular, warning, msg, user, rollovers)).get
  }

  implicit object BSONServerStatusBackgroundFlushingReader
    extends BSONDocumentReader[ServerStatusBackgroundFlushing] {
    def read(doc: BSONDocument): ServerStatusBackgroundFlushing = (for {
      flushes <- doc.getAsTry[BSONNumberLike]("flushes").map(_.toInt)
      totalMs <- doc.getAsTry[BSONNumberLike]("total_ms").map(_.toLong)
      averageMs <- doc.getAsTry[BSONNumberLike]("average_ms").map(_.toLong)
      lastMs <- doc.getAsTry[BSONNumberLike]("last_ms").map(_.toLong)
      lastFinished <- doc.getAsTry[BSONNumberLike](
        "last_finished").map(_.toLong)
    } yield ServerStatusBackgroundFlushing(
      flushes, totalMs, averageMs, lastMs, lastFinished)).get
  }

  implicit object BSONServerStatusConnections
    extends BSONDocumentReader[ServerStatusConnections] {
    def read(doc: BSONDocument): ServerStatusConnections = (for {
      current <- doc.getAsTry[BSONNumberLike]("current").map(_.toInt)
      available <- doc.getAsTry[BSONNumberLike]("available").map(_.toInt)
      totalCreated <- doc.getAsTry[BSONNumberLike]("totalCreated").map(_.toLong)
    } yield ServerStatusConnections(current, available, totalCreated)).get
  }

  implicit object BSONServerStatusJournalingTime
    extends BSONDocumentReader[ServerStatusJournalingTime] {
    def read(doc: BSONDocument): ServerStatusJournalingTime = (for {
      dt <- doc.getAsTry[BSONNumberLike]("dt").map(_.toLong)
      prepLogBuffer <- doc.getAsTry[BSONNumberLike](
        "prepLogBuffer").map(_.toLong)
      writeToJournal <- doc.getAsTry[BSONNumberLike](
        "writeToJournal").map(_.toLong)
      writeToDataFiles <- doc.getAsTry[BSONNumberLike](
        "writeToDataFiles").map(_.toLong)
      remapPrivateView <- doc.getAsTry[BSONNumberLike](
        "remapPrivateView").map(_.toLong)
      commits <- doc.getAsTry[BSONNumberLike]("commits").map(_.toLong)
      commitsInWriteLock <- doc.getAsTry[BSONNumberLike](
        "commitsInWriteLock").map(_.toLong)
    } yield ServerStatusJournalingTime(dt, prepLogBuffer, writeToJournal,
      writeToDataFiles, remapPrivateView, commits, commitsInWriteLock)).get
  }

  implicit object BSONServerStatusJournaling
    extends BSONDocumentReader[ServerStatusJournaling] {
    def read(doc: BSONDocument): ServerStatusJournaling = (for {
      commits <- doc.getAsTry[BSONNumberLike]("commits").map(_.toInt)
      journaledMB <- doc.getAsTry[BSONNumberLike]("journaledMB").map(_.toDouble)
      writeToDataFilesMB <- doc.getAsTry[BSONNumberLike](
        "writeToDataFilesMB").map(_.toDouble)
      compression <- doc.getAsTry[BSONNumberLike]("compression").map(_.toDouble)
      commitsInWriteLock <- doc.getAsTry[BSONNumberLike](
        "commitsInWriteLock").map(_.toInt)
      earlyCommits <- doc.getAsTry[BSONNumberLike]("earlyCommits").map(_.toInt)
      timeMs <- doc.getAsTry[ServerStatusJournalingTime]("timeMS")
    } yield ServerStatusJournaling(commits, journaledMB, writeToDataFilesMB,
      compression, commitsInWriteLock, earlyCommits, timeMs)).get
  }

  implicit object BSONServerStatusNetwork
    extends BSONDocumentReader[ServerStatusNetwork] {
    def read(doc: BSONDocument): ServerStatusNetwork = (for {
      bytesIn <- doc.getAsTry[BSONNumberLike]("bytesIn").map(_.toInt)
      bytesOut <- doc.getAsTry[BSONNumberLike]("bytesOut").map(_.toInt)
      numRequests <- doc.getAsTry[BSONNumberLike]("numRequests").map(_.toInt)
    } yield ServerStatusNetwork(bytesIn, bytesOut, numRequests)).get
  }

  implicit object BSONServerStatusLock
    extends BSONDocumentReader[ServerStatusLock] {
    def read(doc: BSONDocument): ServerStatusLock = (for {
      total <- doc.getAsTry[BSONNumberLike]("total").map(_.toInt)
      readers <- doc.getAsTry[BSONNumberLike]("readers").map(_.toInt)
      writers <- doc.getAsTry[BSONNumberLike]("writers").map(_.toInt)
    } yield ServerStatusLock(total, readers, writers)).get
  }

  implicit object BSONServerStatusGlobalLock
    extends BSONDocumentReader[ServerStatusGlobalLock] {
    def read(doc: BSONDocument): ServerStatusGlobalLock = (for {
      totalTime <- doc.getAsTry[BSONNumberLike]("totalTime").map(_.toInt)
      currentQueue <- doc.getAsTry[ServerStatusLock]("currentQueue")
      activeClients <- doc.getAsTry[ServerStatusLock]("activeClients")
    } yield ServerStatusGlobalLock(totalTime, currentQueue, activeClients)).get
  }

  implicit object BSONServerStatusExtraInfo
    extends BSONDocumentReader[ServerStatusExtraInfo] {
    def read(doc: BSONDocument): ServerStatusExtraInfo = (for {
      heapUsageBytes <- doc.getAsTry[BSONNumberLike](
        "heap_usage_bytes").map(_.toInt)
      pageFaults <- doc.getAsTry[BSONNumberLike]("page_faults").map(_.toInt)
    } yield ServerStatusExtraInfo(heapUsageBytes, pageFaults)).get
  }

  implicit object BSONServerStatusResultReader
    extends DealingWithGenericCommandErrorsReader[ServerStatusResult] {

    def readResult(doc: BSONDocument): ServerStatusResult = (for {
      host <- doc.getAsTry[String]("host")
      version <- doc.getAsTry[String]("version")
      process <- doc.getAsTry[String]("process").map[ServerProcess] {
        ServerProcess.unapply(_).getOrElse(MongodProcess)
      }
      pid <- doc.getAsTry[BSONNumberLike]("pid").map(_.toLong)
      uptime <- doc.getAsTry[BSONNumberLike]("uptime").map(_.toLong)
      uptimeMillis <- doc.getAsTry[BSONNumberLike]("uptimeMillis").map(_.toLong)
      uptimeEstimate <- doc.getAsTry[BSONNumberLike](
        "uptimeEstimate").map(_.toLong)
      localTime <- doc.getAsTry[BSONNumberLike]("localTime").map(_.toLong)
      advisoryHostFQDNs = doc.getAs[List[String]](
        "advisoryHostFQDNs").toList.flatten
      asserts <- doc.getAsTry[ServerStatusAsserts]("asserts")
      backgroundFlushing = doc.getAs[ServerStatusBackgroundFlushing](
        "backgroundFlushing")
      connections <- doc.getAsTry[ServerStatusConnections]("connections")
      dur = doc.getAs[ServerStatusJournaling]("dur")
      extraInfo = doc.getAs[ServerStatusExtraInfo]("extra_info")
      globalLock <- doc.getAsTry[ServerStatusGlobalLock]("globalLock")
      network <- doc.getAsTry[ServerStatusNetwork]("network")
    } yield ServerStatusResult(host, version, process, pid,
      uptime, uptimeMillis, uptimeEstimate, localTime, advisoryHostFQDNs,
      asserts, backgroundFlushing, connections, dur, extraInfo,
      globalLock, network)).get
  }
}
