package reactivemongo.core.actors

import reactivemongo.bson.lowlevel.{ DoubleField, LowLevelBsonDocReader }
import reactivemongo.core.commands.{ FailedAuthentication, X509Authenticate }
import reactivemongo.core.netty.ChannelBufferReadableBuffer
import reactivemongo.core.nodeset.{ Authenticate, Connection, X509Authenticating }
import reactivemongo.core.protocol.Response

private[reactivemongo] trait MongoX509Authentication { system: MongoDBSystem =>
  protected final def sendAuthenticate(
    connection: Connection,
    nextAuth: Authenticate): Connection = {

    connection.send(X509Authenticate(Option(nextAuth.user))(
      f"$$external").maker(RequestId.authenticate.next))

    connection.copy(authenticating = Some(
      X509Authenticating(nextAuth.db, nextAuth.user)))
  }

  protected val authReceive: Receive = {
    case resp: Response if RequestId.authenticate accepts resp => {
      val chanId = resp.info._channelId

      debug(s"AUTH: got authenticated response #${chanId}!")

      updateNodeSet(s"X509Authentication($chanId)") { ns =>
        if (failedToAuthenticate(resp)) {
          val err = s"Failed to authenticate on #${chanId} with X509 authentication. Either does not match certificate or one of the two does not exist"

          handleAuthResponse(ns, resp)(Left(FailedAuthentication(err)))
        } else {
          handleAuthResponse(ns, resp)(X509Authenticate.parseResponse(resp))
        }
      }

      ()
    }
  }

  private def failedToAuthenticate(response: Response): Boolean = {
    val reader = new LowLevelBsonDocReader(new ChannelBufferReadableBuffer(response.documents))
    //reader.lookup("ok") == Some(DoubleField("ok", 0.0))
    reader.lookup("ok").exists(_ == DoubleField("ok", 0.0))
  }
}
