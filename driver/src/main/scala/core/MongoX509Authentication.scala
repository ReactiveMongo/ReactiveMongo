package reactivemongo.core.actors

import reactivemongo.bson.lowlevel.{ DoubleField, LowLevelBsonDocReader }
import reactivemongo.core.commands.{ FailedAuthentication, X509Authenticate }
import reactivemongo.core.netty.ChannelBufferReadableBuffer
import reactivemongo.core.nodeset.{ Authenticate, Connection, X509Authenticating }
import reactivemongo.core.protocol.Response

private[reactivemongo] trait MongoX509Authentication { system: MongoDBSystem =>
  protected final def sendAuthenticate(connection: Connection, nextAuth: Authenticate): Connection = {
    connection.send(X509Authenticate(nextAuth.user)("$external").maker(RequestId.authenticate.next))

    connection.copy(authenticating = Some(
      X509Authenticating(nextAuth.db, nextAuth.user, nextAuth.password)))
  }

  protected val authReceive: Receive = {
    case response: Response if RequestId.authenticate accepts response =>
      logger.debug(s"AUTH: got authenticated response! ${response.info.channelId}")
      if (failedToAuthenticate(response)) {
        whenAuthenticating(response.info.channelId) {
          case (con, _) =>
            val failedMsg =
              "Failed to authenticate with X509 authentication. " +
                "Either does not match certificate or one of the two does not exist"
            authenticationResponse(response)(_ => Left(FailedAuthentication(failedMsg)))
            con.copy(authenticating = None)
        }
      } else {
        authenticationResponse(response)(X509Authenticate.parseResponse)
      }
      ()
  }

  private def failedToAuthenticate(response: Response): Boolean = {
    val reader = new LowLevelBsonDocReader(new ChannelBufferReadableBuffer(response.documents))
    reader.lookup("ok") == Some(DoubleField("ok", 0.0))
  }
}
