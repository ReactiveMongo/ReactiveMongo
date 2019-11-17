package reactivemongo.core.actors

import reactivemongo.api.ReadPreference

import reactivemongo.api.commands.{ Command, X509Authenticate }

import reactivemongo.core.commands.AuthenticationResult

import reactivemongo.core.nodeset.{
  Authenticate,
  Connection,
  X509Authenticating
}

import reactivemongo.core.protocol.Response

private[reactivemongo] trait MongoX509Authentication { system: MongoDBSystem =>
  private lazy val writer = X509Authenticate.writer(pack)

  protected final def sendAuthenticate(
    connection: Connection,
    nextAuth: Authenticate): Connection = {

    val (maker, _) = Command.buildRequestMaker(pack)(
      X509Authenticate(Option(nextAuth.user)),
      writer, ReadPreference.primary, db = f"$$external")

    connection.send(maker(RequestIdGenerator.authenticate.next))

    connection.copy(authenticating = Some(
      X509Authenticating(nextAuth.db, nextAuth.user)))
  }

  private lazy val reader = X509Authenticate.reader(pack)

  protected val authReceive: Receive = {
    case resp: Response if RequestIdGenerator.authenticate accepts resp => {
      val chanId = resp.info._channelId

      debug(s"AUTH: got authenticated response #${chanId}!")

      updateNodeSet(s"X509Authentication($chanId)") { ns =>
        handleAuthResponse(ns, resp)(
          AuthenticationResult.parse(pack, resp)(reader))
      }

      ()
    }
  }
}
