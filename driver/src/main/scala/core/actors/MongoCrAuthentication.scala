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

  protected final def sendAuthenticate(connection: Connection, nextAuth: Authenticate): Connection = {
    connection.send(GetCrNonce(nextAuth.db).
      maker(RequestIdGenerator.getNonce.next))

    nextAuth.password match {
      case Some(password) => connection.copy(authenticating = Some(
        CrAuthenticating(nextAuth.db, nextAuth.user, password, None)))

      case _ => {
        warn(s"Unexpected missing password: ${nextAuth.user}@${nextAuth.db}")
        connection
      }
    }
  }

  protected val authReceive: Receive = {
    case response: Response if RequestIdGenerator.getNonce accepts response => {
      GetCrNonce.ResultMaker(response).fold(
        { e =>
          warn(s"An error has occured while processing getNonce response #${response.header.responseTo}", e)
        },
        { nonce =>
          debug(s"Got authentication nonce for channel #${response.info._channelId}: $nonce")

          val chanId = response.info._channelId

          updateNodeSet(s"CrNonce($chanId)") { ns =>
            ns.pickByChannelId(chanId).fold(ns) { byChan =>
              val con = byChan._2

              con.authenticating match {
                case Some(a @ CrAuthenticating(db, user, pass, _)) => {
                  con.send(CrAuthenticate(user, pass, nonce)(db).
                    maker(RequestIdGenerator.authenticate.next)).
                    addListener(new OperationHandler(
                      { cause =>
                        error(s"Fails to send request after CR nonce #${chanId}", cause)
                      },
                      { _ => () }))

                  ns.updateConnectionByChannelId(chanId) { _ =>
                    con.copy(authenticating = Some(a.copy(nonce = Some(nonce))))
                  }
                }

                case authing => {
                  val msg = s"Unexpected authentication: $authing"

                  warn(msg)

                  handleAuthResponse(ns, response)(
                    Left(FailedAuthentication(msg)))

                }
              }
            }
          }

          ()
        })

      ()
    }

    case resp: Response if RequestIdGenerator.authenticate accepts resp => {
      val chanId = resp.info._channelId

      debug(s"Got authenticated response #${chanId}! ${resp.getClass}")

      updateNodeSet(s"CrAuthentication($chanId)") {
        handleAuthResponse(_, resp)(CrAuthenticate.parseResponse(resp))
      }

      ()
    }
  }
}
