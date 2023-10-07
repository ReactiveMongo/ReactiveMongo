package reactivemongo.actors.pattern

import org.apache.pekko.pattern.{
  AskSupport,
  FutureTimeoutSupport,
  GracefulStopSupport,
  PipeToSupport
}

package object ask
    extends PipeToSupport
    with AskSupport
    with GracefulStopSupport
    with FutureTimeoutSupport
