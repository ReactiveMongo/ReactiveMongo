package reactivemongo.actors.pattern

import akka.pattern.{
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
