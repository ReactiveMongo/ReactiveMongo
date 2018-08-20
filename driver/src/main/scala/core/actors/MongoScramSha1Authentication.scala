package reactivemongo.core.actors

import reactivemongo.core.commands.{
  FailedAuthentication,
  SuccessfulAuthentication
}

import reactivemongo.core.protocol.Response
import reactivemongo.core.nodeset.{
  Authenticate,
  Connection,
  ScramSha1Authenticating
}

private[reactivemongo] trait MongoScramSha1Authentication {
  system: MongoDBSystem =>

  import org.apache.commons.codec.binary.Base64
  import reactivemongo.core.commands.{
    CommandError,
    ScramSha1Initiate,
    ScramSha1Negociation,
    ScramSha1FinalNegociation,
    ScramSha1StartNegociation
  }

  protected final def sendAuthenticate(connection: Connection, nextAuth: Authenticate): Connection = {
    val start = ScramSha1Initiate(nextAuth.user)

    connection.send(start(nextAuth.db).maker(RequestId.getNonce.next))

    nextAuth.password match {
      case Some(password) => connection.copy(authenticating = Some(
        ScramSha1Authenticating(nextAuth.db, nextAuth.user, password,
          start.randomPrefix, start.message)))

      case _ => {
        warn(s"Unexpected missing password: ${nextAuth.user}@${nextAuth.db}")
        connection
      }
    }
  }

  protected val authReceive: Receive = {
    case resp: Response if RequestId.getNonce accepts resp => {
      ScramSha1Initiate.parseResponse(resp).fold(
        { err =>
          val respTo = resp.header.responseTo
          val msg = s"Fails to process SCRAM-SHA1 nonce for $respTo"

          warn(msg, err)

          updateNodeSet(s"ScramSha1NonceFailure($respTo)") { ns =>
            handleAuthResponse(ns, resp)(Left(FailedAuthentication(msg)))
          }

          ()
        }, { challenge =>
          val chanId = resp.info._channelId

          debug(s"Got SCRAM-SHA1 nonce on channel #${chanId}: $challenge")

          updateNodeSet(s"ScramSha1Nonce($chanId)") { ns =>
            ns.pickByChannelId(chanId).fold(ns) { byChan =>
              val con = byChan._2

              con.authenticating match {
                case Some(a @ ScramSha1Authenticating(
                  db, user, pwd, rand, msg, _, _, step)) => {
                  val negociation = ScramSha1StartNegociation(user, pwd,
                    challenge.conversationId, challenge.payload, rand, msg)

                  negociation.serverSignature.fold(
                    { err => handleAuthResponse(ns, resp)(Left(err)) },
                    { sig =>
                      ns.updateConnectionByChannelId(chanId) { con =>
                        con.send(negociation(db).
                          maker(RequestId.authenticate.next)).
                          addListener(new OperationHandler(
                            { cause =>
                              error(s"Fails to send request after SCRAM-SHA1 nonce #${chanId}", cause)
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

    case response: Response if RequestId.authenticate accepts response => {
      val chanId = response.info._channelId

      debug(s"Got authenticated response #${chanId}!")

      @inline def resp: Either[Either[CommandError, SuccessfulAuthentication], Array[Byte]] = ScramSha1StartNegociation.parseResponse(response) match {
        case Left(err)             => Left(Left(err))
        case Right(Left(authed))   => Left(Right(authed))
        case Right(Right(payload)) => Right(payload)
      }

      updateNodeSet(s"ScramSha1Negociation($chanId)") { ns =>
        resp.fold(
          { r => handleAuthResponse(ns, response)(r) },
          { payload: Array[Byte] =>
            debug("2-phase SCRAM-SHA1 negotiation")

            ns.pickByChannelId(chanId).fold(ns) { byChan =>
              val con = byChan._2

              con.authenticating match {
                case Some(a @ ScramSha1Authenticating(
                  db, _, _, _, _, Some(cid), Some(sig),
                  1 /* step; TODO: more retry? */ )) => {

                  val serverSig: Option[String] =
                    ScramSha1Negociation.parsePayload(payload).get("v")

                  if (!serverSig.exists(_ == Base64.encodeBase64String(sig))) {
                    val msg = "SCRAM-SHA1 server signature is invalid"

                    warn(msg)

                    handleAuthResponse(ns, response)(
                      Left(FailedAuthentication(msg)))

                  } else {
                    val negociation = ScramSha1FinalNegociation(cid, payload)

                    con.send(negociation(db).
                      maker(RequestId.authenticate.next)).
                      addListener(new OperationHandler(
                        { e =>
                          error(s"Fails to negociate SCRAM-SHA1 #${chanId}", e)
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
