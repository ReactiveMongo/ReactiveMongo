package reactivemongo.core.actors

import reactivemongo.core.errors.DriverException

// exceptions
object Exceptions {
  import scala.util.control.NoStackTrace

  private val primaryUnavailableMsg = "No primary node is available!"

  private val nodeSetReachableMsg =
    "The node set can not be reached! Please check your network connectivity"

  /** An exception thrown when a request needs a non available primary. */
  sealed class PrimaryUnavailableException private[reactivemongo] (
    val message: String,
    override val cause: Throwable = null) extends DriverException with NoStackTrace {

    def this(supervisor: String, connection: String, cause: Throwable) =
      this(s"$primaryUnavailableMsg ($supervisor/$connection)", cause)

    def this() = this(primaryUnavailableMsg, null)
  }

  @deprecated(message = "Use constructor with details", since = "0.12-RC0")
  case object PrimaryUnavailableException extends PrimaryUnavailableException()

  /**
   * An exception thrown when the entire node set is unavailable.
   * The application may not have access to the network anymore.
   */
  sealed class NodeSetNotReachable private[reactivemongo] (
    val message: String,
    override val cause: Throwable) extends DriverException with NoStackTrace {

    private[reactivemongo] def this(supervisor: String, connection: String, cause: Throwable) = this(s"$nodeSetReachableMsg ($supervisor/$connection)", cause)

    private[reactivemongo] def this() = this(nodeSetReachableMsg, null)
  }

  @deprecated(message = "Use constructor with details", since = "0.12-RC0")
  case object NodeSetNotReachable extends NodeSetNotReachable()

  sealed class ChannelNotFound private[reactivemongo] (
    val message: String,
    val retriable: Boolean,
    override val cause: Throwable) extends DriverException with NoStackTrace

  @deprecated(message = "Use constructor with details", since = "0.12-RC0")
  case object ChannelNotFound
    extends ChannelNotFound("ChannelNotFound", false, null)

  sealed class ClosedException private (
    val message: String,
    override val cause: Throwable) extends DriverException with NoStackTrace {

    private[reactivemongo] def this(supervisor: String, connection: String, cause: Throwable) = this(s"This MongoConnection is closed ($supervisor/$connection)", cause)

    private[reactivemongo] def this(msg: String) = this(msg, null)
    def this() = this("This MongoConnection is closed", null)
  }

  @deprecated(message = "Use constructor with details", since = "0.12-RC0")
  case object ClosedException extends ClosedException()

  final class InternalState private[actors] (
    trace: Array[StackTraceElement]) extends DriverException with NoStackTrace {
    override def getMessage: String = null
    val message = "InternalState"
    super.setStackTrace(trace)
  }

  object InternalState {
    val empty = new InternalState(Array.empty)
  }
}
