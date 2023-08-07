package reactivemongo.actors

package object actor {
  type ActorSystem = org.apache.pekko.actor.ActorSystem
  val ActorSystem = org.apache.pekko.actor.ActorSystem

  type ActorRef = org.apache.pekko.actor.ActorRef
  type Actor = org.apache.pekko.actor.Actor
  val Actor = org.apache.pekko.actor.Actor

  type Props = org.apache.pekko.actor.Props
  val Props = org.apache.pekko.actor.Props
  type Terminated = org.apache.pekko.actor.Terminated
  type Cancellable = org.apache.pekko.actor.Cancellable
}
