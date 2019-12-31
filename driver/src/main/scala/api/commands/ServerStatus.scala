package reactivemongo.api.commands

import reactivemongo.api.SerializationPack

/**
 * @see [[ServerStatusResult]]
 * @see https://docs.mongodb.com/manual/reference/command/serverStatus/
 *
 * @param regular the number of regular assertions
 * @param warning the number of warnings
 * @param msg the number of message assertions
 * @param user the number of user assertions
 * @param rollovers the number of times that the rollovers counters have rolled over since the last time the MongoDB process started
 */
case class ServerStatusAsserts(
  regular: Int,
  warning: Int,
  msg: Int,
  user: Int,
  rollovers: Int)

/**
 * Only for the MMAPv1 storage engine.
 * @see [[ServerStatusResult]]
 * @see https://docs.mongodb.com/manual/reference/command/serverStatus/
 *
 * @param flushes the number of times the database has flushed all writes
 * @param totalMs the total number of milliseconds (ms) that the mongod processes have spent writing
 * @param averageMs the average time in milliseconds for each flush to disk
 * @param lastMs the amount of time, in milliseconds, that the last flush operation took to complete
 * @param lastFinished the timestamp of the last completed flush operation
 */
case class ServerStatusBackgroundFlushing(
  flushes: Int,
  totalMs: Long,
  averageMs: Long,
  lastMs: Long,
  lastFinished: Long)

/**
 * @see [[ServerStatusResult]]
 * @see https://docs.mongodb.com/manual/reference/command/serverStatus/
 *
 * @param current the number of incoming connections from clients to the database server
 * @param available the number of unused incoming connections available
 * @param totalCreated the count of all incoming connections created to the server.
 */
case class ServerStatusConnections(
  current: Int,
  available: Int,
  totalCreated: Long)

/**
 * Only for the MMAPv1 storage engine with the journaling enabled.
 * @see [[ServerStatusJournaling]]
 * @see https://docs.mongodb.com/manual/reference/command/serverStatus/
 * ServerStatusJournaling
 *
 * @param dt the amount of time in milliseconds, over which MongoDB collected this information
 * @param prepLogBuffer the amount of time, in milliseconds, spent preparing to write to the journal
 * @param writeToJournal the amount of time, in milliseconds, spent actually writing to the journal
 * @param writeToDataFiles the amount of time, in milliseconds, spent writing to data files after journaling
 * @param remapPrivateView the amount of time, in milliseconds, spent remapping copy-on-write memory mapped views
 * @param commits the amount of time in milliseconds spent for commits
 * @param commitsInWriteLock the amount of time, in milliseconds, spent for commits that occurred while a write lock was held
 */
case class ServerStatusJournalingTime(
  dt: Long,
  prepLogBuffer: Long,
  writeToJournal: Long,
  writeToDataFiles: Long,
  remapPrivateView: Long,
  commits: Long,
  commitsInWriteLock: Long)

/**
 * Only for the MMAPv1 storage engine with the journaling enabled.
 * @see [[ServerStatusResult]]
 * @see https://docs.mongodb.com/manual/reference/command/serverStatus/
 *
 * @param commits the number of transactions written to the journal during the last journal group commit interval
 * @param journaledMB the amount of data in megabytes (MB) written to journal
 * @param writeToDataFilesMB the amount of data in megabytes (MB) written from journal
 * @param compression the compression ratio of the data written to the journal
 * @param commitsInWriteLock the count of the commits that occurred while a write lock was held
 * @param earlyCommits the number of times MongoDB requested a commit
 */
case class ServerStatusJournaling(
  commits: Int,
  journaledMB: Double,
  writeToDataFilesMB: Double,
  compression: Double,
  commitsInWriteLock: Int,
  earlyCommits: Int,
  timeMs: ServerStatusJournalingTime)

/**
 * @see [[ServerStatusResult]]
 * @see https://docs.mongodb.com/manual/reference/command/serverStatus/
 *
 * @param heapUsageBytes the total size in bytes of heap space used by the database process
 * @param pageFaults the total number of page faults
 */
case class ServerStatusExtraInfo(
  heapUsageBytes: Int,
  pageFaults: Int)

/**
 * @see [[ServerStatusGlobalLock]]
 * @see https://docs.mongodb.com/manual/reference/command/serverStatus/
 *
 * @param total the total number of operations queued waiting for the lock
 * @param readers the number of operations that are currently queued and waiting for the read lock
 * @param writers the number of operations that are currently queued and waiting for the write lock
 */
case class ServerStatusLock(
  total: Int,
  readers: Int,
  writers: Int)

/**
 * @see [[ServerStatusResult]]
 * @see https://docs.mongodb.com/manual/reference/command/serverStatus/
 *
 * @param totalTime the time, in microseconds, since the database last started and created the globalLock
 * @param currentQueue the information concerning the number of operations queued because of a lock
 * @param activeClients the information about the number of connected clients
 */
case class ServerStatusGlobalLock(
  totalTime: Int,
  currentQueue: ServerStatusLock,
  activeClients: ServerStatusLock)

/**
 * @see [[ServerStatusResult]]
 * @see https://docs.mongodb.com/manual/reference/command/serverStatus/
 *
 * @param bytesIn the number of bytes that reflects the amount of network traffic received by this database
 * @param bytesOut the number of bytes that reflects the amount of network traffic sent from this database
 * @param numRequests the total number of distinct requests that the server has received
 */
case class ServerStatusNetwork(
  bytesIn: Int,
  bytesOut: Int,
  numRequests: Int)

/**
 * @see @see https://docs.mongodb.com/manual/reference/command/serverStatus/
 *
 * @param host the system hostname
 * @param version the MongoDB version
 * @param process the MongoDB process
 * @param pid the process ID
 * @param uptime the number of seconds this process has been active
 * @param uptimeMillis same as `uptime` but with millisecond precision
 * @param uptimeEstimate the uptime in seconds as calculated from MongoDB’s internal course-grained time keeping system
 * @param localTime the UTC representation of the current server time
 * @param advisoryHostFQDNs since MongoDB 3.2 (otherwise empty), the array of system fully qualified names
 * @param asserts the statistics about the assertions raised by the MongoDB process since it starts
 * @param backgroundFlushing the report on the periodic writes to disk (only for the MMAPv1 storage engine)
 * @param connections the report about the status of the connection
 * @param dur the report about the mongod instance’s journaling-related operations (only for the MMAPv1 storage engine with the journaling enabled)
 * @param extraInfo the additional information regarding the underlying system
 * @param globalLock the report about the database lock state
 * @param network the report about the MongoDB network use
 */
case class ServerStatusResult(
  host: String,
  version: String,
  process: ServerProcess,
  pid: Long,
  uptime: Long,
  uptimeMillis: Long,
  uptimeEstimate: Long,
  localTime: Long,
  advisoryHostFQDNs: List[String],
  asserts: ServerStatusAsserts,
  backgroundFlushing: Option[ServerStatusBackgroundFlushing],
  connections: ServerStatusConnections,
  dur: Option[ServerStatusJournaling],
  extraInfo: Option[ServerStatusExtraInfo],
  globalLock: ServerStatusGlobalLock,
  network: ServerStatusNetwork)

/** Server [[http://docs.mongodb.org/manual/reference/server-status/ status]] */
@deprecated("Internal: will be made private", "0.16.0")
case object ServerStatus
  extends Command with CommandWithResult[ServerStatusResult] {

  private[api] def writer[P <: SerializationPack](pack: P): pack.Writer[ServerStatus.type] = {
    val builder = pack.newBuilder
    val cmd = builder.document(Seq(builder.elementProducer(
      "serverStatus", builder.int(1))))

    pack.writer[ServerStatus.type](_ => cmd)
  }

  private def readAsserts[P <: SerializationPack](pack: P)(
    decoder: SerializationPack.Decoder[pack.type],
    doc: pack.Document): Option[ServerStatusAsserts] = for {
    regular <- decoder.int(doc, "regular")
    warning <- decoder.int(doc, "warning")
    msg <- decoder.int(doc, "msg")
    user <- decoder.int(doc, "user")
    rollovers <- decoder.int(doc, "rollovers")
  } yield ServerStatusAsserts(regular, warning, msg, user, rollovers)

  private def readBgFlushing[P <: SerializationPack](pack: P)(
    decoder: SerializationPack.Decoder[pack.type],
    doc: pack.Document): Option[ServerStatusBackgroundFlushing] = for {
    flushes <- decoder.int(doc, "flushes")
    totalMs <- decoder.long(doc, "total_ms")
    averageMs <- decoder.long(doc, "average_ms")
    lastMs <- decoder.long(doc, "last_ms")
    lastFinished <- decoder.long(doc, "last_finished")
  } yield ServerStatusBackgroundFlushing(
    flushes, totalMs, averageMs, lastMs, lastFinished)

  private def readConnections[P <: SerializationPack](pack: P)(
    decoder: SerializationPack.Decoder[pack.type],
    doc: pack.Document): Option[ServerStatusConnections] = for {
    current <- decoder.int(doc, "current")
    available <- decoder.int(doc, "available")
    totalCreated <- decoder.long(doc, "totalCreated")
  } yield ServerStatusConnections(current, available, totalCreated)

  private def readJournalingTime[P <: SerializationPack](pack: P)(
    decoder: SerializationPack.Decoder[pack.type],
    doc: pack.Document): Option[ServerStatusJournalingTime] = for {
    dt <- decoder.long(doc, "dt")
    prepLogBuffer <- decoder.long(doc, "prepLogBuffer")
    writeToJournal <- decoder.long(doc, "writeToJournal")
    writeToDataFiles <- decoder.long(doc, "writeToDataFiles")
    remapPrivateView <- decoder.long(doc, "remapPrivateView")
    commits <- decoder.long(doc, "commits")
    commitsInWriteLock <- decoder.long(doc, "commitsInWriteLock")
  } yield ServerStatusJournalingTime(dt, prepLogBuffer, writeToJournal,
    writeToDataFiles, remapPrivateView, commits, commitsInWriteLock)

  private def readJournaling[P <: SerializationPack](pack: P)(
    decoder: SerializationPack.Decoder[pack.type],
    doc: pack.Document): Option[ServerStatusJournaling] = for {
    commits <- decoder.int(doc, "commits")
    journaledMB <- decoder.double(doc, "journaledMB")
    writeToDataFilesMB <- decoder.double(doc, "writeToDataFilesMB")
    compression <- decoder.double(doc, "compression")
    commitsInWriteLock <- decoder.int(doc, "commitsInWriteLock")
    earlyCommits <- decoder.int(doc, "earlyCommits")
    timeDoc <- decoder.child(doc, "timeMs")
    timeMs <- readJournalingTime[pack.type](pack)(decoder, timeDoc)
  } yield ServerStatusJournaling(commits, journaledMB, writeToDataFilesMB,
    compression, commitsInWriteLock, earlyCommits, timeMs)

  private def readNetwork[P <: SerializationPack](pack: P)(
    decoder: SerializationPack.Decoder[pack.type],
    doc: pack.Document): Option[ServerStatusNetwork] = for {
    bytesIn <- decoder.int(doc, "bytesIn")
    bytesOut <- decoder.int(doc, "bytesOut")
    numRequests <- decoder.int(doc, "numRequests")
  } yield ServerStatusNetwork(bytesIn, bytesOut, numRequests)

  private def readStatusLock[P <: SerializationPack](pack: P)(
    decoder: SerializationPack.Decoder[pack.type],
    doc: pack.Document): Option[ServerStatusLock] = for {
    total <- decoder.int(doc, "total")
    readers <- decoder.int(doc, "readers")
    writers <- decoder.int(doc, "writers")
  } yield ServerStatusLock(total, readers, writers)

  private def readGlobalLock[P <: SerializationPack](pack: P)(
    decoder: SerializationPack.Decoder[pack.type],
    doc: pack.Document): Option[ServerStatusGlobalLock] = for {
    totalTime <- decoder.long(doc, "totalTime")
    currentQueue <- decoder.child(doc, "currentQueue").flatMap {
      readStatusLock[pack.type](pack)(decoder, _)
    }
    activeClients <- decoder.child(doc, "activeClients").flatMap {
      readStatusLock[pack.type](pack)(decoder, _)
    }
  } yield ServerStatusGlobalLock(totalTime.toInt, currentQueue, activeClients)

  private def readExtraInfo[P <: SerializationPack](pack: P)(
    decoder: SerializationPack.Decoder[pack.type],
    doc: pack.Document): Option[ServerStatusExtraInfo] = for {
    heapUsageBytes <- decoder.int(doc, "heap_usage_bytes")
    pageFaults <- decoder.int(doc, "page_faults")
  } yield ServerStatusExtraInfo(heapUsageBytes, pageFaults)

  private[api] def reader[P <: SerializationPack](pack: P)(implicit rs: pack.NarrowValueReader[String]): pack.Reader[ServerStatusResult] = {
    val decoder = pack.newDecoder
    import decoder.{ child, long, string }

    CommandCodecs.dealingWithGenericCommandErrorsReader[pack.type, ServerStatusResult](pack) { doc =>
      (for {
        host <- string(doc, "host")
        version <- string(doc, "version")
        process <- string(doc, "process").map[ServerProcess] {
          ServerProcess.unapply(_).getOrElse(MongodProcess)
        }
        pid <- long(doc, "pid")
        uptime <- long(doc, "uptime")
        uptimeMillis <- long(doc, "uptimeMillis")
        uptimeEstimate <- long(doc, "uptimeEstimate")
        localTime <- long(doc, "localTime")
        advisoryHostFQDNs = decoder.values[String](doc, "advisoryHostFQDNs")
        asserts <- child(doc, "asserts").flatMap {
          readAsserts[pack.type](pack)(decoder, _)
        }
        backgroundFlushing = child(doc, "backgroundFlushing").flatMap {
          readBgFlushing[pack.type](pack)(decoder, _)
        }
        connections <- child(doc, "connections").flatMap {
          readConnections[pack.type](pack)(decoder, _)
        }
        dur = child(doc, "dur").flatMap {
          readJournaling[pack.type](pack)(decoder, _)
        }
        extraInfo = child(doc, "extra_info").flatMap {
          readExtraInfo[pack.type](pack)(decoder, _)
        }
        globalLock <- child(doc, "globalLock").flatMap {
          readGlobalLock[pack.type](pack)(decoder, _)
        }
        network <- child(doc, "network").flatMap {
          readNetwork[pack.type](pack)(decoder, _)
        }
      } yield ServerStatusResult(host, version, process, pid,
        uptime, uptimeMillis, uptimeEstimate, localTime,
        advisoryHostFQDNs.fold(List.empty[String])(_.toList),
        asserts, backgroundFlushing, connections,
        dur, extraInfo, globalLock, network)).get
    }
  }
}
