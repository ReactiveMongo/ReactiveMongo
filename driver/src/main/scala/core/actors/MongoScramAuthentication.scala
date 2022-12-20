package reactivemongo.core.actors

import scala.util.control.NonFatal

import scala.collection.immutable.ListSet

import reactivemongo.core.errors.CommandException
import reactivemongo.core.nodeset.{
  Authenticate,
  Connection,
  ScramAuthenticating
}
import reactivemongo.core.protocol.Response

import reactivemongo.api.{
  AuthenticationMode,
  ReadPreference,
  ScramSha1Authentication,
  ScramSha256Authentication
}
import reactivemongo.api.commands.{
  Command,
  CommandKind,
  FailedAuthentication,
  ScramFinalNegociation,
  ScramInitiate,
  ScramNegociation,
  ScramStartNegociation,
  SuccessfulAuthentication
}

private[reactivemongo] trait MongoScramSha1Authentication
    extends MongoScramAuthentication[ScramSha1Authentication.type] {
  system: MongoDBSystem =>

  val mechanism = ScramSha1Authentication

  import reactivemongo.api.commands.{
    ScramSha1Initiate,
    ScramSha1StartNegociation
  }

  protected def initiate(user: String) = ScramSha1Initiate(user)

  protected lazy val challengeReader = ScramSha1Initiate.reader[pack.type](pack)

  protected def startNegociation(
      user: String,
      password: String,
      conversationId: Int,
      payload: Array[Byte],
      randomPrefix: String,
      message: String
    ) = ScramSha1StartNegociation(
    user,
    password,
    conversationId,
    payload,
    randomPrefix,
    message
  )

}

private[reactivemongo] trait MongoScramSha256Authentication
    extends MongoScramAuthentication[ScramSha256Authentication.type] {
  system: MongoDBSystem =>

  val mechanism = ScramSha256Authentication

  import reactivemongo.api.commands.{
    ScramSha256Initiate,
    ScramSha256StartNegociation
  }

  protected def initiate(user: String) = ScramSha256Initiate(user)

  protected lazy val challengeReader =
    ScramSha256Initiate.reader[pack.type](pack)

  protected def startNegociation(
      user: String,
      password: String,
      conversationId: Int,
      payload: Array[Byte],
      randomPrefix: String,
      message: String
    ) = ScramSha256StartNegociation(
    user,
    password,
    conversationId,
    payload,
    randomPrefix,
    message
  )

}

// ---

private[reactivemongo] sealed trait MongoScramAuthentication[
    M <: AuthenticationMode.Scram] { system: MongoDBSystem =>
  import org.apache.commons.codec.binary.Base64

  protected val mechanism: M

  /** @param user The user name */
  protected def initiate(user: String): ScramInitiate[M]

  protected def challengeReader: pack.Reader[ScramInitiate.Result[M]]

  protected def startNegociation(
      user: String,
      password: String,
      conversationId: Int,
      payload: Array[Byte],
      randomPrefix: String,
      message: String
    ): ScramStartNegociation[M]

  // ---

  private lazy val initiateWriter =
    ScramInitiate.writer[pack.type, M](pack, mechanism)

  protected final def sendAuthenticate(
      connection: Connection,
      nextAuth: Authenticate
    ): Connection = {
    val start = initiate(nextAuth.user)
    val maker = Command.buildRequestMaker(pack)(
      CommandKind.Authenticate,
      start,
      initiateWriter,
      ReadPreference.primary,
      nextAuth.db
    )

    connection.send(
      maker(RequestIdGenerator.getNonce.next),
      compression = ListSet.empty
    )

    nextAuth.password match {
      case Some(password) =>
        connection.copy(authenticating =
          Some(
            ScramAuthenticating(
              nextAuth.db,
              nextAuth.user,
              password,
              start.randomPrefix,
              start.message
            )
          )
        )

      case _ => {
        warn(s"Unexpected missing password: ${nextAuth.user}@${nextAuth.db}")
        connection
      }
    }
  }

  private lazy val negociationWriter =
    ScramStartNegociation.writer[pack.type, M](pack)

  private lazy val negociationReader =
    ScramStartNegociation.reader(pack, mechanism)

  private lazy val finalWriter = ScramFinalNegociation.writer(pack)

  protected val authReceive: Receive = {
    case resp: Response if RequestIdGenerator.getNonce accepts resp => {
      val chaRes: ScramInitiate.Result[M] =
        try {
          pack.readAndDeserialize(resp, challengeReader)
        } catch {
          case NonFatal(error) =>
            Left(FailedAuthentication(pack)(error.getMessage, None, None))
        }

      chaRes.fold(
        { err =>
          val respTo = resp.header.responseTo
          val msg = s"Fails to process ${mechanism} nonce for $respTo"

          warn(msg, err)

          updateNodeSet(s"ScramNonceFailure($mechanism, $respTo)") { ns =>
            handleAuthResponse(ns, resp)(
              Left(FailedAuthentication(pack)(msg, None, None))
            )
          }

          ()
        },
        { challenge =>
          val chanId = resp.info.channelId

          debug(s"Got $mechanism nonce on channel #${chanId}: $challenge")

          updateNodeSet(s"ScramNonce($mechanism, $chanId)") { ns =>
            ns.pickByChannelId(chanId).fold(ns) { byChan =>
              val con = byChan._2

              con.authenticating match {
                case Some(
                      a @ ScramAuthenticating(
                        db,
                        user,
                        pwd,
                        rand,
                        msg,
                        _,
                        _,
                        step
                      )
                    ) => {
                  val negociation = startNegociation(
                    user,
                    pwd,
                    challenge.conversationId,
                    challenge.payload,
                    rand,
                    msg
                  )

                  negociation.serverSignature.fold(
                    { err => handleAuthResponse(ns, resp)(Left(err)) },
                    { sig =>
                      ns.updateConnectionByChannelId(chanId) { con =>
                        val maker = Command.buildRequestMaker(pack)(
                          CommandKind.GetNonce,
                          negociation,
                          negociationWriter,
                          ReadPreference.primary,
                          db
                        )

                        con
                          .send(
                            maker(RequestIdGenerator.authenticate.next),
                            compression = ListSet.empty
                          )
                          .addListener(
                            new OperationHandler(
                              { cause =>
                                error(
                                  s"Fails to send request after ${mechanism} nonce #${chanId}",
                                  cause
                                )
                              },
                              { _ => () }
                            )
                          )

                        con.copy(authenticating =
                          Some(
                            a.copy(
                              conversationId = Some(challenge.conversationId),
                              serverSignature = Some(sig),
                              step = step + 1
                            )
                          )
                        )
                      }
                    }
                  )
                }

                case authing => {
                  val msg = s"Unexpected authentication: $authing"

                  warn(msg)

                  handleAuthResponse(ns, resp)(
                    Left(FailedAuthentication(pack)(msg, None, None))
                  )
                }
              }
            }
          }

          ()
        }
      )
    }

    case response: Response
        if RequestIdGenerator.authenticate accepts response => {
      val chanId = response.info.channelId

      debug(s"Got authenticated response #${chanId}!")

      @inline def resp: Either[Either[CommandException, SuccessfulAuthentication], Array[Byte]] =
        try {
          pack.readAndDeserialize(response, negociationReader) match {
            case Left(err)             => Left(Left(err))
            case Right(Left(authed))   => Left(Right(authed))
            case Right(Right(payload)) => Right(payload)
          }
        } catch {
          case NonFatal(error) =>
            Left(Left(FailedAuthentication(pack)(error.getMessage, None, None)))
        }

      updateNodeSet(s"ScramNegociation($mechanism, $chanId)") { ns =>
        resp.fold(
          { r => handleAuthResponse(ns, response)(r) },
          { (payload: Array[Byte]) =>
            debug(s"2-phase $mechanism negotiation")

            ns.pickByChannelId(chanId).fold(ns) { byChan =>
              val con = byChan._2

              con.authenticating match {
                case Some(
                      a @ ScramAuthenticating(
                        db,
                        _,
                        _,
                        _,
                        _,
                        Some(cid),
                        Some(sig),
                        1 /* step */
                      )
                    ) => {

                  val serverSig: Option[String] =
                    ScramNegociation.parsePayload(payload).get("v")

                  if (!serverSig.contains(Base64.encodeBase64String(sig))) {
                    val msg = s"${mechanism} server signature is invalid"

                    warn(msg)

                    handleAuthResponse(ns, response)(
                      Left(FailedAuthentication(pack)(msg, None, None))
                    )

                  } else {
                    val maker = Command.buildRequestMaker(pack)(
                      CommandKind.Authenticate,
                      ScramFinalNegociation(cid, payload),
                      finalWriter,
                      ReadPreference.primary,
                      db
                    )

                    con
                      .send(
                        maker(RequestIdGenerator.authenticate.next),
                        compression = ListSet.empty
                      )
                      .addListener(
                        new OperationHandler(
                          { e =>
                            error(
                              s"Fails to negociate $mechanism #${chanId}",
                              e
                            )
                          },
                          { _ => () }
                        )
                      )

                    ns.updateConnectionByChannelId(chanId) { _ =>
                      con.copy(authenticating = Some(a.copy(step = 2)))
                    }
                  }
                }

                case authing => {
                  val msg = s"Unexpected authentication: $authing"

                  warn(msg)

                  handleAuthResponse(ns, response)(
                    Left(FailedAuthentication(pack)(msg, None, None))
                  )

                }
              }
            }
          }
        )
      }

      ()
    }
  }
}
