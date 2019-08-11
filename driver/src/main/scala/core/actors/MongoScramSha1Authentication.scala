package reactivemongo.core.actors

import reactivemongo.api.{ AuthenticationMode, ScramSha1Authentication }

import reactivemongo.core.commands.{
  CommandError,
  FailedAuthentication,
  ScramChallenge,
  ScramFinalNegociation,
  ScramInitiate,
  ScramNegociation,
  ScramStartNegociation,
  SuccessfulAuthentication
}

import reactivemongo.core.protocol.Response
import reactivemongo.core.nodeset.{
  Authenticate,
  Connection,
  ScramAuthenticating
}

private[reactivemongo] sealed trait MongoScramAuthentication[M <: AuthenticationMode.Scram] { system: MongoDBSystem =>
  import org.apache.commons.codec.binary.Base64

  protected val mechanism: M

  /** @param user The user name */
  protected def initiate(user: String): ScramInitiate[M]

  protected def parseChallenge(resp: Response): Either[CommandError, ScramChallenge[M]]

  protected def startNegociation(
    user: String,
    password: String,
    conversationId: Int,
    payload: Array[Byte],
    randomPrefix: String,
    message: String): ScramStartNegociation[M]

  protected def parseNegociation(resp: Response): ScramStartNegociation.ResType

  // ---

  protected final def sendAuthenticate(connection: Connection, nextAuth: Authenticate): Connection = {
    val start = initiate(nextAuth.user)

    connection.send(start(nextAuth.db).maker(RequestIdGenerator.getNonce.next))

    nextAuth.password match {
      case Some(password) => connection.copy(authenticating = Some(
        ScramAuthenticating(nextAuth.db, nextAuth.user, password,
          start.randomPrefix, start.message)))

      case _ => {
        warn(s"Unexpected missing password: ${nextAuth.user}@${nextAuth.db}")
        connection
      }
    }
  }

  protected val authReceive: Receive = {
    case resp: Response if RequestIdGenerator.getNonce accepts resp => {
      parseChallenge(resp).fold(
        { err =>
          val respTo = resp.header.responseTo
          val msg = s"Fails to process ${mechanism} nonce for $respTo"

          warn(msg, err)

          updateNodeSet(s"ScramNonceFailure($mechanism, $respTo)") { ns =>
            handleAuthResponse(ns, resp)(Left(FailedAuthentication(msg)))
          }

          ()
        }, { challenge =>
          val chanId = resp.info._channelId

          debug(s"Got $mechanism nonce on channel #${chanId}: $challenge")

          updateNodeSet(s"ScramNonce($mechanism, $chanId)") { ns =>
            ns.pickByChannelId(chanId).fold(ns) { byChan =>
              val con = byChan._2

              con.authenticating match {
                case Some(a @ ScramAuthenticating(
                  db, user, pwd, rand, msg, _, _, step)) => {
                  val negociation = startNegociation(user, pwd,
                    challenge.conversationId, challenge.payload, rand, msg)

                  negociation.serverSignature.fold(
                    { err => handleAuthResponse(ns, resp)(Left(err)) },
                    { sig =>
                      ns.updateConnectionByChannelId(chanId) { con =>
                        con.send(negociation(db).
                          maker(RequestIdGenerator.authenticate.next)).
                          addListener(new OperationHandler(
                            { cause =>
                              error(s"Fails to send request after ${mechanism} nonce #${chanId}", cause)
                            },
                            { _ => () }))

                        con.copy(authenticating = Some(a.copy(
                          conversationId = Some(challenge.conversationId),
                          serverSignature = Some(sig),
                          step = step + 1)))
                      }
                    })
                }

                case authing => {
                  val msg = s"Unexpected authentication: $authing"

                  warn(msg)

                  handleAuthResponse(ns, resp)(
                    Left(FailedAuthentication(msg)))
                }
              }
            }
          }

          ()
        })
    }

    case response: Response if RequestIdGenerator.authenticate accepts response => {
      val chanId = response.info._channelId

      debug(s"Got authenticated response #${chanId}!")

      @inline def resp: Either[Either[CommandError, SuccessfulAuthentication], Array[Byte]] = parseNegociation(response) match {
        case Left(err)             => Left(Left(err))
        case Right(Left(authed))   => Left(Right(authed))
        case Right(Right(payload)) => Right(payload)
      }

      updateNodeSet(s"ScramNegociation($mechanism, $chanId)") { ns =>
        resp.fold(
          { r => handleAuthResponse(ns, response)(r) },
          { payload: Array[Byte] =>
            debug(s"2-phase $mechanism negotiation")

            ns.pickByChannelId(chanId).fold(ns) { byChan =>
              val con = byChan._2

              con.authenticating match {
                case Some(a @ ScramAuthenticating(
                  db, _, _, _, _, Some(cid), Some(sig),
                  1 /* step; TODO: more retry? */ )) => {

                  val serverSig: Option[String] =
                    ScramNegociation.parsePayload(payload).get("v")

                  if (!serverSig.exists(_ == Base64.encodeBase64String(sig))) {
                    val msg = s"${mechanism} server signature is invalid"

                    warn(msg)

                    handleAuthResponse(ns, response)(
                      Left(FailedAuthentication(msg)))

                  } else {
                    val negociation = ScramFinalNegociation(cid, payload)

                    con.send(negociation(db).
                      maker(RequestIdGenerator.authenticate.next)).
                      addListener(new OperationHandler(
                        { e =>
                          error(s"Fails to negociate $mechanism #${chanId}", e)
                        },
                        { _ => () }))

                    ns.updateConnectionByChannelId(chanId) { _ =>
                      con.copy(authenticating = Some(a.copy(step = 2)))
                    }
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
          })
      }

      ()
    }
  }
}

private[reactivemongo] trait MongoScramSha1Authentication
  extends MongoScramAuthentication[ScramSha1Authentication.type] {
  system: MongoDBSystem =>

  val mechanism = ScramSha1Authentication

  import reactivemongo.core.commands.{
    CommandError,
    ScramSha1Initiate,
    ScramSha1Challenge,
    ScramSha1Negociation,
    ScramSha1FinalNegociation,
    ScramSha1StartNegociation
  }

  protected def initiate(user: String) = ScramSha1Initiate(user)

  protected def parseChallenge(resp: Response) =
    ScramSha1Initiate.parseResponse(resp)

  protected def startNegociation(
    user: String,
    password: String,
    conversationId: Int,
    payload: Array[Byte],
    randomPrefix: String,
    message: String) = ScramSha1StartNegociation(user, password,
    conversationId, payload, randomPrefix, message)

  protected def parseNegociation(resp: Response): ScramStartNegociation.ResType = ScramSha1StartNegociation.parseResponse(resp)

}
