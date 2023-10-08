package reactivemongo.actors

package object actor {
  type ActorSystem = akka.actor.ActorSystem
  val ActorSystem = akka.actor.ActorSystem

  type ActorRef = akka.actor.ActorRef
  type Actor = akka.actor.Actor
  val Actor = akka.actor.Actor

  type Props = akka.actor.Props
  val Props = akka.actor.Props
  type Terminated = akka.actor.Terminated
  type Cancellable = akka.actor.Cancellable
}
