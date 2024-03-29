package reactivemongo.core.actors

import reactivemongo.core.errors.DriverException

// exceptions
object Exceptions {
  import scala.util.control.NoStackTrace

  private val primaryUnavailableMsg = "No primary node is available!"

  private val nodeSetReachableMsg =
    "The node set can not be reached! Please check your network connectivity"

  /** An exception thrown when a request needs a non available primary. */
  @SuppressWarnings(Array("NullAssignment"))
  final class PrimaryUnavailableException private[reactivemongo] (
      val message: String,
      override val cause: Throwable = null)
      extends DriverException
      with NoStackTrace {

    def this(supervisor: String, connection: String, cause: Throwable) =
      this(s"$primaryUnavailableMsg ($supervisor/$connection)", cause)

    @SuppressWarnings(Array("NullParameter"))
    def this() = this(primaryUnavailableMsg, null)
  }

  /**
   * An exception thrown when the entire node set is unavailable.
   * The application may not have access to the network anymore.
   */
  final class NodeSetNotReachableException private[reactivemongo] (
      val message: String,
      override val cause: Throwable)
      extends DriverException
      with NoStackTrace {

    private[reactivemongo] def this(
        supervisor: String,
        connection: String,
        cause: Throwable
      ) = this(s"$nodeSetReachableMsg ($supervisor/$connection)", cause)

    @SuppressWarnings(Array("NullParameter"))
    private[reactivemongo] def this() = this(nodeSetReachableMsg, null)
  }

  final class ChannelNotFoundException private[reactivemongo] (
      val message: String,
      val retriable: Boolean,
      override val cause: Throwable)
      extends DriverException
      with NoStackTrace

  final class ClosedException private (
      val message: String,
      override val cause: Throwable)
      extends DriverException
      with NoStackTrace {

    private[reactivemongo] def this(
        supervisor: String,
        connection: String,
        cause: Throwable
      ) =
      this(s"This MongoConnection is closed ($supervisor/$connection)", cause)

    @SuppressWarnings(Array("NullParameter"))
    private[reactivemongo] def this(msg: String) = this(msg, null)

    @SuppressWarnings(Array("NullParameter"))
    def this() = this("This MongoConnection is closed", null)
  }

  final class NotAuthenticatedException private[core] (
      val message: String)
      extends DriverException
      with NoStackTrace

  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final class InternalState private[actors] (
      trace: Array[StackTraceElement])
      extends DriverException
      with NoStackTrace {
    val message = "InternalState"
    @inline override def getMessage: String = ""
    super.setStackTrace(trace)
  }

  private[reactivemongo] object InternalState {
    val empty = new InternalState(Array.empty)
  }
}
