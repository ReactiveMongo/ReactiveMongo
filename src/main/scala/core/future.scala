package reactivemongo.core.actors

import scala.concurrent.Promise
import reactivemongo.core.protocol.Response


class MongoFuture {

}

object MongoFuture {
  def promise :Promise[Response] = Promise()
}