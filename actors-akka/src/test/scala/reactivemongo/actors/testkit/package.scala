package reactivemongo.actors

import akka.actor.Actor
import akka.testkit.{ TestActorRef => PekkoTestActorRef }

package object testkit {
  type TestActorRef[T <: Actor] = PekkoTestActorRef[T]
  val TestActorRef = PekkoTestActorRef
}
