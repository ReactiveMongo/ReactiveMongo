package reactivemongo.actors

import org.apache.pekko.actor.Actor
import org.apache.pekko.testkit.{TestActorRef => PekkoTestActorRef}

package object testkit {
  type TestActorRef[T <: Actor] = PekkoTestActorRef[T]
  val TestActorRef = PekkoTestActorRef
}
