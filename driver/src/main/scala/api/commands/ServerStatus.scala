package reactivemongo.api.commands

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
  rollovers: Int
)

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
  lastFinished: Long
)

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
  totalCreated: Long
)

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
  commitsInWriteLock: Long
)

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
  timeMs: ServerStatusJournalingTime
)

/**
 * @see [[ServerStatusResult]]
 * @see https://docs.mongodb.com/manual/reference/command/serverStatus/
 *
 * @param heapUsageBytes the total size in bytes of heap space used by the database process
 * @param pageFaults the total number of page faults
 */
case class ServerStatusExtraInfo(
  heapUsageBytes: Int,
  pageFaults: Int
)

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
  writers: Int
)

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
  activeClients: ServerStatusLock
)

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
  numRequests: Int
)

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
  // TODO: locks
  network: ServerStatusNetwork
// TODO: opcounters, opcountersRepl, rangeDeleter, repl, security, storageEngine, wiredTiger
)

/** Server [[http://docs.mongodb.org/manual/reference/server-status/ status]] */
case object ServerStatus
  extends Command with CommandWithResult[ServerStatusResult]
