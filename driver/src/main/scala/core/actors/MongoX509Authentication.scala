package reactivemongo.core.actors

import scala.collection.immutable.ListSet

import reactivemongo.core.nodeset.{
  Authenticate,
  Connection,
  X509Authenticating
}
import reactivemongo.core.protocol.Response

import reactivemongo.api.ReadPreference
import reactivemongo.api.commands.{
  AuthenticationResult,
  Command,
  CommandKind,
  X509Authenticate
}

private[reactivemongo] trait MongoX509Authentication { system: MongoDBSystem =>
  private lazy val writer = X509Authenticate.writer(pack)

  protected final def sendAuthenticate(
      connection: Connection,
      nextAuth: Authenticate
    ): Connection = {

    val maker = Command.buildRequestMaker(pack)(
      CommandKind.Authenticate,
      X509Authenticate(Option(nextAuth.user)),
      writer,
      ReadPreference.primary,
      db = f"$$external"
    )

    connection.send(
      maker(RequestIdGenerator.authenticate.next),
      compression = ListSet.empty
    )

    connection.copy(authenticating =
      Some(X509Authenticating(nextAuth.db, nextAuth.user))
    )
  }

  private lazy val reader = X509Authenticate.reader(pack)

  protected val authReceive: Receive = {
    case resp: Response if RequestIdGenerator.authenticate accepts resp => {
      val chanId = resp.info.channelId

      debug(s"AUTH: got authenticated response #${chanId}!")

      updateNodeSet(s"X509Authentication($chanId)") { ns =>
        handleAuthResponse(ns, resp)(
          AuthenticationResult.parse(pack, resp)(reader)
        )
      }

      ()
    }
  }
}
