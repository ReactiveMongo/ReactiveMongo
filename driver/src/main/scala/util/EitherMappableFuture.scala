/*
 * Copyright 2012-2013 Stephane Godbillon (@sgodbillon) and Zenexity
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivemongo.util

//import scala.language.implicitConversions

import scala.concurrent.{ duration, Future, Promise }

import duration.Duration

private[reactivemongo] object ExtendedFutures {
  import reactivemongo.actors.actor.ActorSystem

  // better way to this?
  def delayedFuture(millis: Long, system: ActorSystem): Future[Unit] = {
    implicit val ec = system.dispatcher
    val promise = Promise[Unit]()

    system.scheduler.scheduleOnce(Duration.apply(millis, "millis")) {
      promise.success({}); ()
    }

    promise.future
  }
}
