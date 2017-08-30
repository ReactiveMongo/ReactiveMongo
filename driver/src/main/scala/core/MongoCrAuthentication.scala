package reactivemongo.core.actors

import reactivemongo.core.commands.FailedAuthentication

import reactivemongo.core.protocol.Response
import reactivemongo.core.nodeset.{
  Authenticate,
  CrAuthenticating,
  Connection
}

private[reactivemongo] trait MongoCrAuthentication { system: MongoDBSystem =>
  import reactivemongo.core.commands.{ CrAuthenticate, GetCrNonce }
  import MongoDBSystem.logger

  protected final def sendAuthenticate(connection: Connection, nextAuth: Authenticate): Connection = {
    connection.send(GetCrNonce(nextAuth.db).maker(RequestId.getNonce.next))
    connection.copy(authenticating = Some(
      CrAuthenticating(nextAuth.db, nextAuth.user, nextAuth.password, None)))
  }

  protected val authReceive: Receive = {
    case response: Response if RequestId.getNonce accepts response => {
      GetCrNonce.ResultMaker(response).fold(
        { e =>
          logger.warn(s"[$lnm] An error has occured while processing getNonce response #${response.header.responseTo}", e)
        },
        { nonce =>
          logger.debug(s"[$lnm] Got authentication nonce for channel ${response.info.channelId}: $nonce")

          whenAuthenticating(response.info.channelId) {
            case (connection, a @ CrAuthenticating(db, user, pass, _)) =>
              connection.send(CrAuthenticate(user, pass, nonce)(db).
                maker(RequestId.authenticate.next))

              connection.copy(authenticating = Some(a.copy(
                nonce = Some(nonce))))

            case (connection, auth) => {
              val msg = s"Unexpected authentication: $auth"

              logger.warn(s"[$lnm] $msg")

              authenticationResponse(response)(
                _ => Left(FailedAuthentication(msg)))

              connection
            }
          }
        })

      ()
    }

    case response: Response if RequestId.authenticate accepts response => {
      logger.debug(s"[$lnm] Got authenticated response! ${response.info.channelId}")
      authenticationResponse(response)(CrAuthenticate.parseResponse(_))
      ()
    }
  }
}
