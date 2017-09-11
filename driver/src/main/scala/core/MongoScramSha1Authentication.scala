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
  import MongoDBSystem.logger
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

    connection.copy(authenticating = Some(
      ScramSha1Authenticating(nextAuth.db, nextAuth.user, nextAuth.password,
        start.randomPrefix, start.message)))
  }

  protected val authReceive: Receive = {
    case response: Response if RequestId.getNonce accepts response => {
      ScramSha1Initiate.parseResponse(response).fold(
        { e =>
          val msg = s"Error while processing getNonce response #${response.header.responseTo}"

          logger.warn(s"[$lnm] $msg")
          logger.debug(s"[$lnm] SCRAM-SHA1 getNonce failure", e)

          authenticationResponse(response)(_ => Left(FailedAuthentication(msg)))
        }, { challenge =>
          logger.debug(s"[$lnm] Got challenge for channel ${response.info.channelId}: $challenge")

          whenAuthenticating(response.info.channelId) {
            case (con, a @ ScramSha1Authenticating(
              db, user, pwd, rand, msg, _, _, step)) => {
              val negociation = ScramSha1StartNegociation(user, pwd,
                challenge.conversationId, challenge.payload, rand, msg)

              negociation.serverSignature.fold[Connection](
                { e => authenticationResponse(response)(_ => Left(e)); con },
                { sig =>
                  con.send(negociation(db).maker(RequestId.authenticate.next))

                  con.copy(authenticating = Some(a.copy(
                    conversationId = Some(challenge.conversationId),
                    serverSignature = Some(sig),
                    step = step + 1)))
                })
            }

            case (con, auth) => {
              val msg = s"Unexpected authentication: $auth"

              logger.warn(s"[$lnm] $msg")

              authenticationResponse(response)(
                _ => Left(FailedAuthentication(msg)))

              con
            }
          }
        })

      ()
    }

    case response: Response if RequestId.authenticate accepts response => {
      logger.debug(s"[$lnm] Got authenticated response! ${response.info.channelId}")

      @inline def resp: Either[Either[CommandError, SuccessfulAuthentication], Array[Byte]] = ScramSha1StartNegociation.parseResponse(response) match {
        case Left(err)             => Left(Left(err))
        case Right(Left(authed))   => Left(Right(authed))
        case Right(Right(payload)) => Right(payload)
      }

      resp.fold(
        { r => authenticationResponse(response)(_ => r) },
        { payload: Array[Byte] =>
          logger.debug("2-phase SCRAM-SHA1 negotiation")

          whenAuthenticating(response.info.channelId) {
            case (con, a @ ScramSha1Authenticating(
              db, _, _, _, _, Some(cid), Some(sig),
              1 /* step; TODO: more retry? */ )) => {

              val serverSig: Option[String] =
                ScramSha1Negociation.parsePayload(payload).get("v")

              if (!serverSig.exists(_ == Base64.encodeBase64String(sig))) {
                val msg = "SCRAM-SHA1 server signature is invalid"

                logger.warn(s"[$lnm] $msg")

                authenticationResponse(response)(
                  _ => Left(FailedAuthentication(msg)))

                con
              } else {
                val negociation = ScramSha1FinalNegociation(cid, payload)

                con.send(negociation(db).maker(RequestId.authenticate.next))
                con.copy(authenticating = Some(a.copy(step = 2)))
              }
            }

            case (con, auth) => {
              val msg = s"Unexpected authentication: $auth"

              logger.warn(s"[$lnm] msg")

              authenticationResponse(response)(
                _ => Left(FailedAuthentication(msg)))

              con
            }
          }
        })

      ()
    }
  }
}
