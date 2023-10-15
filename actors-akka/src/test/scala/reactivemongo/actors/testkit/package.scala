package reactivemongo.actors

import akka.actor.Actor

package object testkit {
  type TestActorRef[T <: Actor] = akka.testkit.TestActorRef[T]
  val TestActorRef = akka.testkit.TestActorRef
}
