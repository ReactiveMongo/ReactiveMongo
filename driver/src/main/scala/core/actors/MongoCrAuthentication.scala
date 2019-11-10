package reactivemongo.core.actors

import scala.util.control.NonFatal

import reactivemongo.core.commands.{
  AuthenticationResult,
  FailedAuthentication
}

import reactivemongo.core.protocol.Response
import reactivemongo.core.nodeset.{
  Authenticate,
  CrAuthenticating,
  Connection
}

import reactivemongo.api.ReadPreference
import reactivemongo.api.commands.{ CrAuthenticate, Command, GetCrNonce }

private[reactivemongo] trait MongoCrAuthentication { system: MongoDBSystem =>
  private lazy val getCrNonceWriter = GetCrNonce.writer(pack)

  protected final def sendAuthenticate(connection: Connection, nextAuth: Authenticate): Connection = {
    val (getCrNonce, _) = Command.buildRequestMaker(pack)(
      GetCrNonce, getCrNonceWriter, ReadPreference.primary, nextAuth.db)

    connection.send(getCrNonce(RequestIdGenerator.getNonce.next))

    nextAuth.password match {
      case Some(password) => connection.copy(authenticating = Some(
        CrAuthenticating(nextAuth.db, nextAuth.user, password, None)))

      case _ => {
        warn(s"Unexpected missing password: ${nextAuth.user}@${nextAuth.db}")
        connection
      }
    }
  }

  private lazy val getCrNonceReader = GetCrNonce.reader(pack)
  private lazy val crAuthWriter = CrAuthenticate.writer(pack)
  private lazy val crAuthReader = CrAuthenticate.reader(pack)

  protected val authReceive: Receive = {
    case response: Response if RequestIdGenerator.getNonce accepts response => {
      try {
        val nonce = pack.readAndDeserialize(response, getCrNonceReader).value

        debug(s"Got authentication nonce for channel #${response.info._channelId}: $nonce")

        val chanId = response.info._channelId

        updateNodeSet(s"CrNonce($chanId)") { ns =>
          ns.pickByChannelId(chanId).fold(ns) { byChan =>
            val con = byChan._2

            con.authenticating match {
              case Some(a @ CrAuthenticating(db, user, pass, _)) => {
                val (maker, _) = Command.buildRequestMaker(pack)(
                  CrAuthenticate(user, pass, nonce),
                  crAuthWriter, ReadPreference.primary, db)

                con.send(maker(RequestIdGenerator.authenticate.next)).
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
                  Left(FailedAuthentication(pack)(msg, None, None)))

              }
            }
          }
        }

        ()
      } catch {
        case NonFatal(e) => warn(s"An error has occured while processing getNonce response #${response.header.responseTo}", e)
      }
    }

    case resp: Response if RequestIdGenerator.authenticate accepts resp => {
      val chanId = resp.info._channelId

      debug(s"Got authenticated response #${chanId}! ${resp.getClass}")

      updateNodeSet(s"CrAuthentication($chanId)") {
        handleAuthResponse(_, resp)(
          AuthenticationResult.parse(pack, resp)(crAuthReader))
      }

      ()
    }
  }
}
